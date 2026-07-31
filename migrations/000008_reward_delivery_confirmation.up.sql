alter table gacha_runtime.pull_operations
  drop constraint pull_operations_status_check;
alter table gacha_runtime.pull_operations
  add constraint pull_operations_status_check
  check (
    status in (
      'processing',
      'event_pending',
      'event_published',
      'succeeded',
      'refund_pending',
      'failed'
    )
  ) not valid;
alter table gacha_runtime.pull_operations
  validate constraint pull_operations_status_check;

create index concurrently if not exists pull_operations_delivery_pending_idx
  on gacha_runtime.pull_operations (updated_at, id)
  where status in ('event_pending', 'event_published');

create or replace function gacha_runtime.protect_pull_audit_evidence()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  if tg_op = 'DELETE' then
    if old.status in ('event_pending', 'event_published', 'succeeded') then
      raise exception 'pull audit evidence is immutable';
    end if;
    return old;
  end if;

  if old.status = 'succeeded' then
    raise exception 'pull audit evidence is immutable';
  end if;

  if old.status in ('event_pending', 'event_published') and (
    new.status not in (old.status, 'event_published', 'succeeded')
    or new.request_hash is distinct from old.request_hash
    or new.response is distinct from old.response
    or new.event is distinct from old.event
  ) then
    raise exception 'pull audit evidence is immutable';
  end if;

  return new;
end
$$;
