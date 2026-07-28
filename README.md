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
```

客户端在 `POST /v1/me/pulls` 超时或结果不确定时，必须保留原请求参数和
`Idempotency-Key`，先调用 `GET /v1/me/pulls/operation` 查询当前用户的操作状态。
只有明确返回 `pull_operation_not_found` 或 `failed` 后，才能丢弃旧键并允许新操作。

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
```

`GACHA_CONFIG_DATABASE_URL` 为空时使用代码内置静态配置。配置后必须同时提供 `GACHA_PROJECT_ID` 和 `GACHA_ENVIRONMENT_ID`，服务只读取该环境当前发布指针指向的不可变快照，并在快照内选择时间有效的卡池版本。

`GACHA_STATE_DATABASE_URL` 是抽卡运行状态的权威 Postgres。如果未单独配置，会复用 `GACHA_CONFIG_DATABASE_URL`。启动服务前必须执行：

```bash
psql "$GACHA_STATE_DATABASE_URL" -f migrations/000001_gacha_runtime_state.up.sql
psql "$GACHA_STATE_DATABASE_URL" -f migrations/000002_pull_processing_lease.up.sql
```

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

Postgres 是抽卡操作、原始响应和保底快照的唯一事实源。幂等记录没有 TTL；缓存故障、切主或淘汰不会把已执行操作变成“未找到”。处理中请求使用短租约和 fencing token：进程中断后，同一请求可接管并用同一个资产幂等键继续，旧进程不能再提交或退款。数据库提交结果不明确时不会猜测退款，而是读取持久化操作状态后继续恢复。

`event_pending` 记录同时承担 Outbox 职责，Kafka 恢复任务会按数据库租约重复投递，背包服务再按 `event_id` 去重。

抽卡响应和 Kafka 事件会带上 `banner_version_id`，用于追溯当次抽卡使用的卡池配置版本。

网关接入时，把公开路径 `/api/v1/gacha` 转发到服务内的 `/v1`：

```text
gacha=/api/v1/gacha|http://gacha-engine-service.gacha-engine-service.svc.cluster.local/v1
```
