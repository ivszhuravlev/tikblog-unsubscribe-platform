-- Operational state of one user's subscription to one writer.
--
-- The row is keyed by (user_id, writer_id) — this pair is where business
-- idempotency lives. Unsubscribes from UI, Customer Success and Legal for the
-- same pair are three independent commands for one state change, and they must
-- converge to one row. Nothing here dedupes by event_id: the events are
-- genuinely different, the intent is not.

CREATE TABLE IF NOT EXISTS subscription (
    user_id              text        NOT NULL,
    writer_id            text        NOT NULL,
    status               text        NOT NULL,
    -- When the user asked, not when we processed it: the earliest requested_at
    -- across duplicates wins, so a Legal batch replayed two weeks later cannot
    -- move a consent timestamp forward.
    unsubscribed_at      timestamptz,
    -- Provenance of the request that set unsubscribed_at.
    unsubscribe_source   text,
    unsubscribe_event_id text,
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT subscription_pkey PRIMARY KEY (user_id, writer_id),
    CONSTRAINT subscription_status_known
        CHECK (status IN ('SUBSCRIBED', 'UNSUBSCRIBED')),
    CONSTRAINT subscription_source_known
        CHECK (unsubscribe_source IS NULL
               OR unsubscribe_source IN ('UI', 'CUSTOMER_SUCCESS', 'LEGAL')),
    -- An unsubscribed row without a timestamp is a consent record we cannot
    -- defend, so the database refuses to hold one.
    CONSTRAINT subscription_unsubscribed_has_timestamp
        CHECK (status <> 'UNSUBSCRIBED' OR unsubscribed_at IS NOT NULL)
);
