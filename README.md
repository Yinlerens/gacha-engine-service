# Gacha Engine Service

这是一个 FastAPI 抽卡引擎。

它只做四件事：

1. 从 Supabase/Postgres 或静态兜底配置读取当前卡池
2. 通过 Asset Service 幂等扣款或退款
3. 在 Postgres 事务中持久化幂等操作、保底和待发送事件
4. 把已经提交的完成事件发到 Kafka

它不负责：

- 资产账户和账务流水持久化
- 背包落库
- 用户历史持久化

## API

```http
GET /health
GET /ready
GET /v1/banners
GET /v1/items
GET /v1/me/pity?banner_id=limited-character-001
GET /v1/me/pulls/operation
GET /v1/me/pulls/{event_id}/audit
POST /v1/me/pulls
```

`POST /v1/me/pulls` 示例：

```json
{
  "banner_id": "limited-character-001",
  "count": 10,
  "seed": "optional-seed"
}
```

请求头必须由网关注入：

```http
X-Internal-Token: <internal-token>
X-User-Id: <uuid>
Idempotency-Key: <pull-operation-id>
X-Request-Accepted-At: <gateway-rfc3339-timestamp>
```

客户端在 `POST /v1/me/pulls` 超时或结果不确定时，应保留原请求参数和
`Idempotency-Key`，并用 `GET /v1/me/pulls/operation` 查询进度。服务端恢复不依赖
客户端再次请求：后台 Worker 会接管租约过期的操作并继续执行。客户端在操作进入
终态前不能用新键重复发起同一笔业务操作。

## 配置

```text
PORT=8080
INTERNAL_TOKEN=...
KAFKA_BOOTSTRAP_SERVERS=localhost:19092
KAFKA_TOPIC=gacha.pull_completed.v1
KAFKA_CLIENT_ID=gacha-engine-service
GACHA_CONFIG_DATABASE_URL=
GACHA_STATE_DATABASE_URL=
GACHA_PROJECT_ID=
GACHA_ENVIRONMENT_ID=
GACHA_CONFIG_CACHE_TTL_SECONDS=30
GACHA_CONFIG_QUERY_TIMEOUT_SECONDS=5
GACHA_CONFIG_POOL_SIZE=2
GACHA_STATE_QUERY_TIMEOUT_SECONDS=5
GACHA_STATE_POOL_SIZE=8
GACHA_ENGINE_BUILD_SHA=<git-commit-sha>
```

`GACHA_CONFIG_DATABASE_URL` 为空时使用代码内置静态配置。配置后必须同时提供 `GACHA_PROJECT_ID` 和 `GACHA_ENVIRONMENT_ID`，服务只读取该环境当前发布指针指向的不可变快照，并在快照内选择时间有效的卡池版本。

`GACHA_STATE_DATABASE_URL` 是抽卡运行状态的权威 Postgres。如果未单独配置，会复用 `GACHA_CONFIG_DATABASE_URL`。启动服务前必须执行：

```bash
psql "$GACHA_STATE_DATABASE_URL" -f migrations/000001_gacha_runtime_state.up.sql
psql "$GACHA_STATE_DATABASE_URL" -f migrations/000002_pull_processing_lease.up.sql
psql "$GACHA_STATE_DATABASE_URL" -f migrations/000003_pull_unattended_recovery.up.sql
psql "$GACHA_STATE_DATABASE_URL" -f migrations/000004_pity_groups_expand.up.sql
psql "$GACHA_STATE_DATABASE_URL" -f migrations/000005_backfill_pity_groups.up.sql
psql "$GACHA_STATE_DATABASE_URL" -f migrations/000006_pity_groups_enforce.up.sql
psql "$GACHA_STATE_DATABASE_URL" -f migrations/000007_pull_audit_integrity.up.sql
```

`000007` 必须先于包含审计功能的 Engine 版本部署。它为 `event_id` 建立在线索引，并保护已经生成的抽卡结果、Kafka 事件和成功记录不被修改或删除。

生产环境不能清理 `gacha_runtime.pull_operations` 中的幂等键；如需归档响应正文，也必须永久保留 `(user_id, idempotency_key_hash)` 墓碑。

### 从旧 Redis 状态切换

首次上线必须在维护窗口完成，避免扫描期间继续产生新状态：

1. 暂停抽卡流量。
2. 执行 Postgres migration。
3. 保留旧 `REDIS_URL`，运行一次迁移工具：

```bash
uv run --extra legacy-migration python -m scripts.migrate_redis_state
```

4. 确认输出的 `skipped` 只包含旧 recovery lock 等非业务 key，再部署新服务。

工具可以安全重跑：保底只接受更高版本，幂等操作使用永久唯一约束且不会覆盖已经写入 Postgres 的记录。

Supabase 推荐长期运行的后端服务使用 direct connection；如果运行环境只有 IPv4，可以使用 Supavisor session pooler。连接串建议带 `sslmode=require`。

## 本地开发

安装依赖：

```bash
uv sync --extra test
```

运行测试：

```bash
uv run --extra test pytest
```

启动服务：

```bash
uv run gacha-engine-service --host 127.0.0.1 --port 8080
```

或者直接用 Docker Compose：

```bash
docker compose up --build
```

## 说明

Postgres 是抽卡操作、原始响应和保底快照的唯一事实源。幂等记录没有 TTL；缓存故障、切主或淘汰不会把已执行操作变成“未找到”。扣款前会持久化完成本次抽卡所需的冻结配置、种子和事件 ID，但不保存明文 `Idempotency-Key`。处理中请求使用短租约和 fencing token：进程中断后，后台 Worker 会用同一个资产幂等键自动接管，旧进程不能再提交或退款。数据库提交结果不明确时不会猜测退款，而是读取持久化状态继续恢复。

恢复 Worker 同时处理三类非终态：`processing` 自动续跑抽卡，`event_pending` 自动补发 Kafka，`refund_pending` 自动重试退款。扣款、退款和下游事件都使用稳定幂等标识；即使外部调用成功后进程再次中断，下一轮恢复也不会重复扣款或重复入账。操作进入 `succeeded` 或 `failed` 后会清除冻结恢复上下文，只永久保留幂等墓碑和必要的审计结果。

同一用户在同一卡池并发抽取时，请求可以先读取到相同的保底版本，但只有一个能提交。其他请求会自动读取已提交的新版本、重新计算候选结果并继续提交；持续竞争超过当前处理轮次时保留为 `processing` 交给恢复 Worker，已扣款请求不会因为保底版本竞争而退款。

保底状态按发布配置中的 `pity_group_id` 隔离，而不是按展示用的 `banner_id` 隐式隔离。同一保底组可以跨卡池版本继承，不同组严格分开；旧快照和旧恢复记录缺少该字段时会回退到原 `banner_id`，因此升级不会重置已有保底。修改已投入使用的保底组属于数据迁移，不能只改配置。

`event_pending` 记录同时承担 Outbox 职责，Kafka 恢复任务会按数据库租约重复投递，背包服务再按 `event_id` 去重。

抽卡响应和 Kafka 事件会带上 `banner_version_id` 与 `pity_group_id`，用于追溯当次抽卡使用的卡池配置版本和保底作用域。审计元数据还会永久绑定 `release_id`、发布快照 SHA-256、规范化卡池配置 SHA-256、RNG 算法版本、Engine 版本和构建 commit。`GET /v1/me/pulls/{event_id}/audit` 会读取指定不可变发布快照，校验全部哈希，并使用原始种子和抽取前保底状态逐抽重放。

网关接入时，把公开路径 `/api/v1/gacha` 转发到服务内的 `/v1`：

```text
gacha=/api/v1/gacha|http://gacha-engine-service.gacha-engine-service.svc.cluster.local/v1
```
