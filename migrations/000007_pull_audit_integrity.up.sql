create unique index concurrently if not exists pull_operations_event_id_uidx
  on gacha_runtime.pull_operations ((response ->> 'event_id'))
  where response ? 'event_id';

create or replace function gacha_runtime.protect_pull_audit_evidence()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  if tg_op = 'DELETE' then
    if old.status in ('event_pending', 'succeeded') then
      raise exception 'pull audit evidence is immutable';
    end if;
    return old;
  end if;

  if old.status = 'succeeded' then
    raise exception 'pull audit evidence is immutable';
  end if;

  if old.status = 'event_pending' and (
    new.status not in ('event_pending', 'succeeded')
    or new.request_hash is distinct from old.request_hash
    or new.response is distinct from old.response
    or new.event is distinct from old.event
  ) then
    raise exception 'pull audit evidence is immutable';
  end if;

  return new;
end
$$;

drop trigger if exists pull_operations_protect_audit_evidence
  on gacha_runtime.pull_operations;
create trigger pull_operations_protect_audit_evidence
  before update or delete on gacha_runtime.pull_operations
  for each row execute function gacha_runtime.protect_pull_audit_evidence();
