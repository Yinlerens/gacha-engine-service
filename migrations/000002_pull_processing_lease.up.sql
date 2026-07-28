alter table gacha_runtime.pull_operations
  add column if not exists processing_token uuid,
  add column if not exists processing_lease_until timestamptz;

create index if not exists pull_operations_processing_lease_idx
  on gacha_runtime.pull_operations (processing_lease_until, id)
  where status = 'processing';
