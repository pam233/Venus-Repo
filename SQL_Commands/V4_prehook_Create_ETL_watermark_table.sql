CREATE TABLE IF NOT EXISTS musicschema.etl_watermark
(
  ID SERIAL PRIMARY KEY NOT NULL,
  table_name VARCHAR(50) PRIMARY KEY,
  last_update_timestamp TIMESTAMP
);
