create schema if not exists gacha_runtime;

create table if not exists gacha_runtime.pity_snapshots (
  user_id uuid not null,
  banner_id text not null,
  since_five integer not null default 0,
  since_four integer not null default 0,
  guaranteed_featured_five boolean not null default false,
  version bigint not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (user_id, banner_id),
  constraint pity_snapshots_banner_id_length
    check (length(banner_id) > 0 and length(banner_id) <= 100),
  constraint pity_snapshots_counters_non_negative
    check (since_five >= 0 and since_four >= 0),
  constraint pity_snapshots_version_non_negative check (version >= 0)
);

create table if not exists gacha_runtime.pull_operations (
  id uuid primary key,
  user_id uuid not null,
  idempotency_key_hash text not null,
  request_hash text not null,
  status text not null,
  response jsonb,
  event jsonb,
  error_code text,
  error_message text,
  recovery_locked_until timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint pull_operations_user_idempotency_unique
    unique (user_id, idempotency_key_hash),
  constraint pull_operations_idempotency_hash_format
    check (idempotency_key_hash ~ '^[0-9a-f]{64}$'),
  constraint pull_operations_request_hash_format
    check (request_hash ~ '^[0-9a-f]{64}$'),
  constraint pull_operations_status_check
    check (status in ('processing', 'event_pending', 'succeeded', 'refund_pending', 'failed')),
  constraint pull_operations_response_object
    check (response is null or jsonb_typeof(response) = 'object'),
  constraint pull_operations_event_object
    check (event is null or jsonb_typeof(event) = 'object'),
  constraint pull_operations_error_code_length
    check (error_code is null or length(error_code) <= 100),
  constraint pull_operations_error_message_length
    check (error_message is null or length(error_message) <= 1000)
);

create index if not exists pull_operations_event_pending_idx
  on gacha_runtime.pull_operations (updated_at, id)
  where status = 'event_pending';
