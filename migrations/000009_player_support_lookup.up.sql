alter table gacha_runtime.pull_operations
  add column if not exists request_id uuid;

create index concurrently if not exists pull_operations_player_support_idx
  on gacha_runtime.pull_operations (user_id, created_at desc, id desc);
