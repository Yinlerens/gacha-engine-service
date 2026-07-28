alter table gacha_runtime.pity_snapshots
  validate constraint pity_snapshots_pity_group_id_length;

create unique index concurrently if not exists pity_snapshots_user_pity_group_uidx
  on gacha_runtime.pity_snapshots (user_id, pity_group_id);
