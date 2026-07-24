CREATE DATABASE IF NOT EXISTS app;
CREATE USER IF NOT EXISTS 'proxy'@'%' IDENTIFIED WITH mysql_native_password BY 'proxy-secret';
GRANT ALL PRIVILEGES ON app.* TO 'proxy'@'%';

USE app;
CREATE TABLE IF NOT EXISTS proxy_smoke_test (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    message VARCHAR(255) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);
INSERT INTO proxy_smoke_test(message) VALUES ('seed row');

CREATE TABLE IF NOT EXISTS backend_identity (
    id TINYINT PRIMARY KEY,
    marker VARCHAR(32) NOT NULL
);

CREATE TABLE IF NOT EXISTS proxy_write_test (
    id BIGINT PRIMARY KEY,
    message VARCHAR(255) NOT NULL
);
