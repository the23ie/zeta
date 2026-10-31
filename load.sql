-- Usage: psql "$DATABASE_URL" -v dir=data_10m -f load.sql
\set ON_ERROR_STOP on
\i schema.sql
\set t :dir '/tenants.csv'
\set c :dir '/customers.csv'
\set p :dir '/products.csv'
\set o :dir '/orders.csv'
\set i :dir '/order_items.csv'
\copy tenants     FROM :'t' CSV HEADER
\copy customers   FROM :'c' CSV HEADER
\copy products    FROM :'p' CSV HEADER
\copy orders      FROM :'o' CSV HEADER
\copy order_items FROM :'i' CSV HEADER
ANALYZE;
