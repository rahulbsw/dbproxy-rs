FROM rust:1.89-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
RUN cargo build --locked --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home dbproxy
COPY --from=builder /build/target/release/dbproxy /usr/local/bin/dbproxy
USER 10001
EXPOSE 6033 6071 6380 6381 6432 6433
ENTRYPOINT ["/usr/local/bin/dbproxy"]
CMD ["--config", "/etc/dbproxy/dbproxy.toml"]
