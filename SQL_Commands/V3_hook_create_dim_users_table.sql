CREATE TABLE IF NOT EXISTS musicschema.dim_user
(
    id PRIMARY KEY NOT NULL,
    user_id INTEGER,
    age TEXT
);

CREATE INDEX IF NOT EXISTS id.dim_user ON (id);

INSERT INTO musicschema.dim_user
(id, age)
SELECT 
    src_user.user_id,
    src_user.age
FROM musicschema.stg_users
ON CONFLICT(id)
DO UPDATE SET 
    id = excluded.id
    age = excluded.age

