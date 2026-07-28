drop trigger if exists pity_snapshots_fill_pity_group_id
  on gacha_runtime.pity_snapshots;
drop function if exists gacha_runtime.fill_pity_group_id();

alter table gacha_runtime.pity_snapshots
  drop constraint if exists pity_snapshots_pity_group_id_length,
  drop column if exists pity_group_id;
