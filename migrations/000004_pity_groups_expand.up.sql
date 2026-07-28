alter table gacha_runtime.pity_snapshots
  add column if not exists pity_group_id text;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'pity_snapshots_pity_group_id_length'
      and conrelid = 'gacha_runtime.pity_snapshots'::regclass
  ) then
    alter table gacha_runtime.pity_snapshots
      add constraint pity_snapshots_pity_group_id_length
      check (
        pity_group_id is not null
        and length(pity_group_id) > 0
        and length(pity_group_id) <= 100
      ) not valid;
  end if;
end
$$;

create or replace function gacha_runtime.fill_pity_group_id()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  if new.pity_group_id is null then
    new.pity_group_id := new.banner_id;
  end if;
  return new;
end;
$$;

drop trigger if exists pity_snapshots_fill_pity_group_id
  on gacha_runtime.pity_snapshots;
create trigger pity_snapshots_fill_pity_group_id
  before insert or update of banner_id, pity_group_id
  on gacha_runtime.pity_snapshots
  for each row execute function gacha_runtime.fill_pity_group_id();
