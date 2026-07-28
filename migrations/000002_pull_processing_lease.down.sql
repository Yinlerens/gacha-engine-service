drop index if exists gacha_runtime.pull_operations_processing_lease_idx;

alter table gacha_runtime.pull_operations
  drop column if exists processing_lease_until,
  drop column if exists processing_token;
