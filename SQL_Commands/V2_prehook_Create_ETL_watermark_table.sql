create table if not EXISTS etl_watermark
(
  ID SERIAL PRIMARY KEY Not NULL,
  etl_last_execution_timr TIMESTAMP
;)