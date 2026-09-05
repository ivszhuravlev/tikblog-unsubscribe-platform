-- One row stores one user's subscription to one writer.

CREATE TABLE IF NOT EXISTS subscription (
    user_id              text        NOT NULL,
    writer_id            text        NOT NULL,
    status               text        NOT NULL,
    -- When the user requested the unsubscribe.
    unsubscribed_at      timestamptz,
    -- Which source supplied the earliest request.
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
    -- Every unsubscribe must have a timestamp.
    CONSTRAINT subscription_unsubscribed_has_timestamp
        CHECK (status <> 'UNSUBSCRIBED' OR unsubscribed_at IS NOT NULL)
);
