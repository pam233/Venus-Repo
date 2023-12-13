
CREATE TABLE IF NOT EXISTS musicschema.etl_watermark (
    id SERIAL PRIMARY KEY,
    table_name TEXT,
    etl_last_id INT
);
 