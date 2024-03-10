CREATE TABLE logs (
   timestamp DateTime,
   level String,
   message String,
   attrs String
) ENGINE = MergeTree()
ORDER BY timestamp
PARTITION BY toYYYYMMDD(timestamp)