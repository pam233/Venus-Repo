CREATE TABLE IF NOT EXISTS {target.schema}.dim_user
(
    id PRIMARY KEY NOT NULL,
    user_id INTEGER,
    age TEXT
);

CREATE INDEX IF NOT EXISTS idx_user_id {target_schema}.dim_user ON (user_id);

INSERT INTO {target_schema}.dim_user
(user_id, age)
SELECT 
    src_user.user_id,
    src_user.age
FROM {target_schema}.stg_users
ON CONFLICT(user_id)
DO UPDATE SET 
    user_id = excluded.user_id
    age = excluded.age

