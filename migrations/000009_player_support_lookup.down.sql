drop index concurrently if exists gacha_runtime.pull_operations_player_support_idx;

alter table gacha_runtime.pull_operations
  drop column if exists request_id;
