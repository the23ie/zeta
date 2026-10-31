-- ZETA hackathon dataset schema (PostgreSQL 15+)
-- Shopify-like multi-tenant store: each tenant = one store owner.

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id   INT PRIMARY KEY,
    name        TEXT NOT NULL,
    tier        TEXT NOT NULL            -- 'enterprise' | 'growth' | 'starter'
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id BIGINT PRIMARY KEY,
    tenant_id   INT NOT NULL REFERENCES tenants(tenant_id),
    name        TEXT NOT NULL,
    city        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    product_id  BIGINT PRIMARY KEY,
    tenant_id   INT NOT NULL REFERENCES tenants(tenant_id),
    name        TEXT NOT NULL,
    category    TEXT NOT NULL,
    price_cents BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id    BIGINT PRIMARY KEY,
    tenant_id   INT NOT NULL REFERENCES tenants(tenant_id),
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id),
    status      TEXT NOT NULL CHECK (status IN ('placed','shipped','delivered','cancelled','returned')),
    total_cents BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id    BIGINT NOT NULL REFERENCES orders(order_id),
    line_no     SMALLINT NOT NULL,
    tenant_id   INT NOT NULL,
    product_id  BIGINT NOT NULL REFERENCES products(product_id),
    qty         INT NOT NULL,
    price_cents BIGINT NOT NULL,
    PRIMARY KEY (order_id, line_no)
);

-- Indexes the *app* would have anyway (also used by the baseline "direct query" for correctness checks)
CREATE INDEX IF NOT EXISTS orders_tenant_created ON orders (tenant_id, created_at);
CREATE INDEX IF NOT EXISTS items_tenant_product  ON order_items (tenant_id, product_id);

-- ZETA needs UPDATEs to carry the OLD row so it can emit (-old, +new) deltas
ALTER TABLE orders REPLICA IDENTITY FULL;
