CREATE TABLE logs (
   timestamp DateTime,
   hostname String,
   level String,
   message String,
   attrs String
) ENGINE = MergeTree()
ORDER BY (timestamp,hostname)
PARTITION BY toYYYYMMDD(timestamp)
SETTINGS index_granularity = 8192;