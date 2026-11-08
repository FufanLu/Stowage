CREATE TABLE fulfillment_center (
    fc_id        text PRIMARY KEY,
    region       text NOT NULL,
    -- Order in which FCs outside the customer's region are tried: nearest first, roughly.
    fallback_rank int NOT NULL
);

INSERT INTO fulfillment_center (fc_id, region, fallback_rank) VALUES
    ('east-1',    'us-east',    1),
    ('east-2',    'us-east',    2),
    ('central-1', 'us-central', 3),
    ('south-1',   'us-south',   4),
    ('west-1',    'us-west',    5),
    ('west-2',    'us-west',    6);

-- Physical stock. Changes only on restock and shipment (M4), so it is never a hot row.
CREATE TABLE stock (
    sku     varchar(10) NOT NULL CHECK (sku ~ '^B0[0-9A-F]{8}$'),
    fc_id   text        NOT NULL REFERENCES fulfillment_center,
    on_hand int         NOT NULL CHECK (on_hand >= 0),
    PRIMARY KEY (sku, fc_id)
);

-- Every hold is a row; available = on_hand - sum(held and committed lines). Writing a hold is
-- an INSERT, so thousands of buyers of one SKU never queue on one row lock.
CREATE TABLE reservation (
    reservation_id  uuid        PRIMARY KEY,
    idempotency_key text        NOT NULL UNIQUE,
    order_id        text        NOT NULL,
    status          text        NOT NULL CHECK (status IN ('HELD', 'COMMITTED', 'RELEASED', 'EXPIRED', 'REJECTED')),
    expires_at      timestamptz NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX reservation_held_expiry_idx ON reservation (expires_at) WHERE status = 'HELD';
CREATE INDEX reservation_updated_idx ON reservation (updated_at);

CREATE TABLE reservation_line (
    reservation_id uuid        NOT NULL REFERENCES reservation ON DELETE CASCADE,
    sku            varchar(10) NOT NULL,
    fc_id          text        NOT NULL REFERENCES fulfillment_center,
    quantity       int         NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (reservation_id, sku, fc_id)
);
CREATE INDEX reservation_line_sku_fc_idx ON reservation_line (sku, fc_id);
