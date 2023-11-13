CREATE TABLE IF NOT EXISTS musicschema.etl_watermark
(
  ID SERIAL PRIMARY KEY NOT NULL,
  etl_last_execution_time TIMESTAMP
);
