-- Milkrun sync schema (ADR 0002, ADR 0003).
--
-- Every write to a route happens inside a transaction that first locks the route row
-- (SELECT ... FOR UPDATE). That makes route.version a gap-free, totally ordered change counter, so
-- "every stop with version > cursor" is a correct pull: no change can commit with a version lower
-- than one a client has already seen.

CREATE TABLE route (
    id          text        PRIMARY KEY,
    version     bigint      NOT NULL CHECK (version >= 1),
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE stop (
    route_id    text        NOT NULL REFERENCES route (id),
    id          text        NOT NULL,
    version     bigint      NOT NULL,          -- route.version at the stop's last change
    sequence    integer     NOT NULL,
    status      text        NOT NULL,          -- denormalised from snapshot for ops queries
    snapshot    jsonb       NOT NULL,          -- the full Stop, exactly what pull returns
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (route_id, id)
);

-- Serves the pull query: WHERE route_id = ? AND version > ? ORDER BY sequence, id
CREATE INDEX stop_changes ON stop (route_id, version);

-- Per device, per route: the highest seq the server has ruled on (Replicache's lastMutationID).
CREATE TABLE client_cursor (
    route_id    text        NOT NULL REFERENCES route (id),
    client_id   text        NOT NULL,
    last_seq    bigint      NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (route_id, client_id)
);

-- Every ruling, kept so a retried push gets the original answer back, and as the audit trail
-- ("who said this parcel was delivered, and when").
CREATE TABLE mutation_log (
    route_id    text        NOT NULL REFERENCES route (id),
    client_id   text        NOT NULL,
    seq         bigint      NOT NULL,
    stop_id     text        NOT NULL,
    hlc         text        NOT NULL,
    type        text        NOT NULL,
    payload     jsonb       NOT NULL,
    outcome     text        NOT NULL CHECK (outcome IN ('accepted', 'rejected')),
    reason      text,
    ruled_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (route_id, client_id, seq)
);
