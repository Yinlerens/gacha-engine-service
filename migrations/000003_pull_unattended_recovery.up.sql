alter table gacha_runtime.pull_operations
  add column if not exists recovery_context jsonb;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'pull_operations_recovery_context_object'
      and conrelid = 'gacha_runtime.pull_operations'::regclass
  ) then
    alter table gacha_runtime.pull_operations
      add constraint pull_operations_recovery_context_object
      check (
        recovery_context is null
        or jsonb_typeof(recovery_context) = 'object'
      ) not valid;
  end if;
end
$$;

alter table gacha_runtime.pull_operations
  validate constraint pull_operations_recovery_context_object;

create index concurrently if not exists pull_operations_processing_recovery_idx
  on gacha_runtime.pull_operations (processing_lease_until, id)
  where status = 'processing' and recovery_context is not null;

create index concurrently if not exists pull_operations_refund_pending_recovery_idx
  on gacha_runtime.pull_operations (updated_at, id)
  where status = 'refund_pending' and recovery_context is not null;
