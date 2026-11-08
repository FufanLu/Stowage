CREATE TABLE product (
    sku         varchar(10)  PRIMARY KEY CHECK (sku ~ '^B0[0-9A-F]{8}$'),
    title       text         NOT NULL,
    brand       text         NOT NULL,
    category    text         NOT NULL,
    price_cents bigint       NOT NULL CHECK (price_cents >= 0),
    currency    char(3)      NOT NULL DEFAULT 'USD',
    rating      numeric(2,1) NOT NULL DEFAULT 0 CHECK (rating BETWEEN 0 AND 5),
    version     bigint       NOT NULL DEFAULT 1 CHECK (version >= 1),
    updated_at  timestamptz  NOT NULL DEFAULT now()
);

CREATE INDEX product_category_idx ON product (category);
