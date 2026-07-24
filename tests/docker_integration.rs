use mysql_async::{Conn, Opts, OptsBuilder, params, prelude::Queryable};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Duration, sleep},
};
use tokio_postgres::NoTls;

#[tokio::test]
#[ignore = "requires `docker compose up --build --detach --wait`"]
async fn binary_prepared_statements_route_through_proxy() {
    let opts = Opts::from(
        OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(6033)
            .user(Some("app"))
            .pass(Some("app-secret"))
            .db_name(Some("app")),
    );
    let mut connection = Conn::new(opts)
        .await
        .expect("connect to the Docker DBProxy listener");

    let marker: Option<String> = connection
        .exec_first(
            "SELECT marker FROM backend_identity WHERE id = :id",
            params! {"id" => 1_u8},
        )
        .await
        .expect("execute a binary prepared read");
    assert_eq!(marker.as_deref(), Some("replica"));

    connection
        .exec_drop(
            "INSERT INTO proxy_write_test(id, message) VALUES (:id, :message) \
             ON DUPLICATE KEY UPDATE message = VALUES(message)",
            params! {
                "id" => 9002_u64,
                "message" => "binary prepared write",
            },
        )
        .await
        .expect("execute a binary prepared write");

    let primary_visible: Option<u64> = connection
        .exec_first(
            "SELECT COUNT(*) FROM proxy_write_test WHERE id = :id",
            params! {"id" => 9002_u64},
        )
        .await
        .expect("read the prepared write from the primary consistency window");
    assert_eq!(primary_visible, Some(1));

    connection.disconnect().await.expect("disconnect cleanly");
}

#[tokio::test]
#[ignore = "requires `docker compose up --build --detach --wait`"]
async fn postgres_extended_protocol_and_timescale_route_through_proxy() {
    let (read_write, read_write_connection) = tokio_postgres::connect(
        "host=127.0.0.1 port=6432 user=proxy password=proxy-secret dbname=app",
        NoTls,
    )
    .await
    .expect("connect to the PostgreSQL read-write listener");
    tokio::spawn(async move {
        read_write_connection
            .await
            .expect("read-write PostgreSQL connection");
    });

    let statement = read_write
        .prepare("SELECT marker FROM backend_identity WHERE id = $1")
        .await
        .expect("prepare a PostgreSQL extended-protocol statement");
    let row = read_write
        .query_one(&statement, &[&1_i16])
        .await
        .expect("query the PostgreSQL primary");
    assert_eq!(row.get::<_, String>(0), "primary");

    let timescale: i64 = read_write
        .query_one(
            "SELECT COUNT(*) FROM pg_extension WHERE extname = 'timescaledb'",
            &[],
        )
        .await
        .expect("query the TimescaleDB extension")
        .get(0);
    assert_eq!(timescale, 1);

    let (read_only, read_only_connection) = tokio_postgres::connect(
        "host=127.0.0.1 port=6433 user=proxy password=proxy-secret dbname=app",
        NoTls,
    )
    .await
    .expect("connect to the PostgreSQL read-only listener");
    tokio::spawn(async move {
        read_only_connection
            .await
            .expect("read-only PostgreSQL connection");
    });
    let row = read_only
        .query_one(
            "SELECT marker FROM backend_identity WHERE id = $1",
            &[&1_i16],
        )
        .await
        .expect("query the PostgreSQL replica");
    assert_eq!(row.get::<_, String>(0), "replica");
}

#[tokio::test]
#[ignore = "requires `docker compose up --build --detach --wait`"]
async fn redis_resp_replication_and_large_values_route_through_proxy() {
    let key = b"dbproxy:rust-integration";
    let value = vec![b'x'; 1024 * 1024];
    assert_eq!(
        redis_command(6380, &[b"SET", key, &value]).await,
        RedisReply::Simple(b"OK".to_vec())
    );

    let mut replica_value = RedisReply::Bulk(None);
    for _ in 0..50 {
        replica_value = redis_command(6381, &[b"GET", key]).await;
        if replica_value == RedisReply::Bulk(Some(value.clone())) {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(replica_value, RedisReply::Bulk(Some(value)));

    let write_attempt = redis_command(6381, &[b"SET", b"dbproxy:must-fail", b"value"]).await;
    assert!(
        matches!(write_attempt, RedisReply::Error(message) if message.starts_with(b"READONLY"))
    );
}

#[derive(Debug, Eq, PartialEq)]
enum RedisReply {
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Bulk(Option<Vec<u8>>),
    Integer(i64),
}

async fn redis_command(port: u16, arguments: &[&[u8]]) -> RedisReply {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect to Redis proxy listener");
    write_resp_command(&mut stream, &[b"AUTH", b"redis-secret"]).await;
    assert_eq!(
        read_resp_reply(&mut stream).await,
        RedisReply::Simple(b"OK".to_vec())
    );
    write_resp_command(&mut stream, arguments).await;
    read_resp_reply(&mut stream).await
}

async fn write_resp_command(stream: &mut TcpStream, arguments: &[&[u8]]) {
    let mut request = format!("*{}\r\n", arguments.len()).into_bytes();
    for argument in arguments {
        request.extend_from_slice(format!("${}\r\n", argument.len()).as_bytes());
        request.extend_from_slice(argument);
        request.extend_from_slice(b"\r\n");
    }
    stream
        .write_all(&request)
        .await
        .expect("write Redis command");
}

async fn read_resp_reply(stream: &mut TcpStream) -> RedisReply {
    let prefix = stream.read_u8().await.expect("read Redis reply prefix");
    let line = read_crlf_line(stream).await;
    match prefix {
        b'+' => RedisReply::Simple(line),
        b'-' => RedisReply::Error(line),
        b':' => RedisReply::Integer(
            std::str::from_utf8(&line)
                .expect("integer reply is UTF-8")
                .parse()
                .expect("valid Redis integer reply"),
        ),
        b'$' => {
            let length: i64 = std::str::from_utf8(&line)
                .expect("bulk length is UTF-8")
                .parse()
                .expect("valid Redis bulk length");
            if length < 0 {
                return RedisReply::Bulk(None);
            }
            let mut value = vec![0_u8; length as usize];
            stream
                .read_exact(&mut value)
                .await
                .expect("read Redis bulk payload");
            let mut terminator = [0_u8; 2];
            stream
                .read_exact(&mut terminator)
                .await
                .expect("read Redis bulk terminator");
            assert_eq!(&terminator, b"\r\n");
            RedisReply::Bulk(Some(value))
        }
        other => panic!("unsupported Redis reply prefix: {other:#x}"),
    }
}

async fn read_crlf_line(stream: &mut TcpStream) -> Vec<u8> {
    let mut line = Vec::with_capacity(32);
    loop {
        let byte = stream.read_u8().await.expect("read Redis reply line");
        if byte == b'\r' {
            assert_eq!(stream.read_u8().await.expect("read Redis line feed"), b'\n');
            return line;
        }
        line.push(byte);
        assert!(line.len() <= 64 * 1024, "Redis reply line is too long");
    }
}
