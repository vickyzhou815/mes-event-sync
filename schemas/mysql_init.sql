CREATE TABLE IF NOT EXISTS lot_status (
    lot_id      VARCHAR(32) PRIMARY KEY,
    state       VARCHAR(16) NOT NULL,
    tool_id     VARCHAR(32),
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
    ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;

-- Downstream "RTD" table — written to by the RTD consumer, independent
-- from the source lot_status table that Debezium watches. In the real
-- architecture this would live in RTD's own database; here it's the same
-- MySQL instance for simplicity.
CREATE TABLE IF NOT EXISTS lot_status_rtd (
    lot_id      VARCHAR(32) PRIMARY KEY,
    state       VARCHAR(16) NOT NULL,
    tool_id     VARCHAR(32),
    synced_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

