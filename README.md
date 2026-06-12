# Gacha Engine Service

这是一个 FastAPI 抽卡引擎。

它只做三件事：

1. 从 Supabase/Postgres 或静态兜底配置读取当前卡池
2. 读取 Redis 里的保底快照并执行抽卡
3. 把完成事件发到 Kafka

它不负责：

- 资产扣减
- 背包落库
- 用户历史持久化

## API

```http
GET /health
GET /ready
GET /v1/banners
GET /v1/items
GET /v1/me/pity?banner_id=limited-character-001
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
```

## 配置

```text
PORT=8080
INTERNAL_TOKEN=...
REDIS_URL=redis://localhost:6379/0
KAFKA_BOOTSTRAP_SERVERS=localhost:19092
KAFKA_TOPIC=gacha.pull_completed.v1
KAFKA_CLIENT_ID=gacha-engine-service
REDIS_KEY_PREFIX=gacha:pity
GACHA_CONFIG_DATABASE_URL=
GACHA_CONFIG_CACHE_TTL_SECONDS=30
GACHA_CONFIG_QUERY_TIMEOUT_SECONDS=5
GACHA_CONFIG_POOL_SIZE=2
```

`GACHA_CONFIG_DATABASE_URL` 为空时使用代码内置静态配置。配置后服务会读取 Supabase/Postgres 的 `gacha.*` 表中当前 `published` 且时间有效的卡池版本。

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

Redis 保存的是运行态保底快照，不是最终事实源。后续背包服务接入后，可以把这份快照当作恢复输入。

抽卡响应和 Kafka 事件会带上 `banner_version_id`，用于追溯当次抽卡使用的卡池配置版本。

网关接入时，把公开路径 `/api/v1/gacha` 转发到服务内的 `/v1`：

```text
gacha=/api/v1/gacha|http://gacha-engine-service.gacha-engine-service.svc.cluster.local/v1
```
