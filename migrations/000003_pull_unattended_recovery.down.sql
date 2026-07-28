drop index concurrently if exists gacha_runtime.pull_operations_processing_recovery_idx;

alter table gacha_runtime.pull_operations
  drop constraint if exists pull_operations_recovery_context_object,
  drop column if exists recovery_context;
