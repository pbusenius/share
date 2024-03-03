CREATE TABLE port.port_location_h3 (
    'name' String,
    'longitude' Float64,
    'latitude' Float64,
    'h3_cell' UInt64 MATERIALIZED geoToH3("longitude", "latitude", 10)
) 
ENGINE = MergeTree
ORDER BY 'name';

INSERT INTO port.port_location FROM INFILE 'input.csv' FORMAT CSV;


CREATE TABLE github_queue
(

)
ENGINE = Kafka('kafka_host:9092', 'github', 'clickhouse', 'JSONEachRow') settings kafka_thread_per_consumer = 0, kafka_num_consumers = 1;