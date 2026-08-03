update gacha_runtime.pull_operations
set request_id = (recovery_context ->> 'request_id')::uuid
where request_id is null
  and recovery_context ->> 'request_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
