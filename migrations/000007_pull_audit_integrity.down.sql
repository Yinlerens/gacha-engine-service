drop trigger if exists pull_operations_protect_audit_evidence
  on gacha_runtime.pull_operations;
drop function if exists gacha_runtime.protect_pull_audit_evidence();
drop index concurrently if exists gacha_runtime.pull_operations_event_id_uidx;
