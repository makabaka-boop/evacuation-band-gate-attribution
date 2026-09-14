# 火警疏散腕带首次通过 API

纯后端服务：火警疏散时，同一腕带可能被相邻闸机几乎同时扫到。本服务保证
**同一腕带全局恰有一次 `first_seen`**，其余并发上报均为携带同一归属事实的
`already_seen`，使分区清点不会虚增人数。

- Python 3.12 · FastAPI · Pydantic v2 · SQLAlchemy 2 · PostgreSQL 16 · pytest
- 并发正确性由 **PostgreSQL 事务 + 唯一约束**裁决，不依赖应用进程内锁，
  因此跨请求、跨 uvicorn worker、跨容器进程均成立。

## 快速开始

```bash
# 启动 API 与 PostgreSQL（宿主端口可用 API_PORT 覆盖）
API_PORT=9000 docker compose up --build

# 一次性验收服务：对真实 PostgreSQL 跑并发事务验收，退出码即结论
docker compose --profile verify run --rm verify
```

验收通过的关键断言（见 `tests/acceptance/test_evacuation.py`）：

1. **清点只 +1**：4 个独立进程在数据库屏障集合后同时提交同一腕带，
   `first_seen` 恰 1 个、`already_seen` 3 个，清点表 `zone_tally` 最终恰 1 行。
2. 落败响应携带的 `first_gate_id` / `first_seen_at` 与胜者完全一致。
3. `GET /bands/{band_id}` 返回同一条唯一事实。
4. 幂等：完全相同载荷的重放逐字返回原响应且不新增任何记录。
5. 同一 `event_id` 不同载荷返回 `409`，且不改变归属、不留记录；仅时区
   写法不同（`+00:00` vs `Z`）也按不同载荷拒绝。
6. 归属按**事务提交先后**裁决，不按客户端 `scanned_at` 倒排。
7. 同一 `event_id` 并发提交也只落一条记录，其余逐字重放（不出现 409）。
8. HTTP 端到端：经双 worker 的真实 API 竞争 + 查询 + 重放 + 409。

## API

### `POST /scans`

请求体（`scanned_at` 必须带时区偏移）：

```json
{
  "event_id": "evt-001",
  "band_id": "band-77",
  "gate_id": "GATE-A",
  "scanned_at": "2026-09-14T10:00:00+08:00"
}
```

响应（首次与非首次结构相同）：

```json
{
  "event_id": "evt-001",
  "band_id": "band-77",
  "gate_id": "GATE-A",
  "scanned_at": "2026-09-14T02:00:00Z",
  "first_gate_id": "GATE-A",
  "first_seen_at": "2026-09-14T02:00:01.123456Z",
  "result": "first_seen"
}
```

- 首个**成功提交事务**的闸机永久成为 `first_gate_id`；后到者 `result` 为
  `already_seen`，但 `first_gate_id` / `first_seen_at` 指向同一事实。
- 幂等键为 `event_id`：**完整载荷逐字符相同**的重放才返回原响应（因此
  `scanned_at` 的时区写法也必须一致，例如 `+08:00` 改写成等价的 `Z` 时刻
  视为不同载荷，返回 `409` 且原闸机归属不变）。

### `GET /bands/{band_id}`

返回该腕带唯一的首次通过事实（含胜出 `event_id`、闸机、扫描时刻、落库时刻）；
无记录返回 `404`。

### `GET /health`

存活探针。

## 并发与一致性设计

`band_first_seen` 以 `band_id` 为**主键**（另有 `event_id` 唯一约束），
`idempotent_requests` 以 `event_id` 为**主键**。

每次 `POST /scans` 是一个事务（`app/service.py`）：

1. 对 `event_id` 的哈希取 `pg_advisory_xact_lock`（事务级、跨进程、随事务
   自动释放），把同键重放/冲突的判定串行化。
2. 命中既有幂等记录：载荷逐字符相同则逐字返回原响应；任一字段不同（含
   `scanned_at` 的时区写法不同）则抛 409、整事务回滚，归属不变。
3. 否则执行 `INSERT ... ON CONFLICT (band_id) DO NOTHING RETURNING`：
   - 插入成功的事务即**唯一胜者**（`first_seen`）；
   - 被主键阻塞的并发事务在胜者提交后唤醒，取不到 RETURNING 行，
     转而读取同一事实（`already_seen`）。
4. 幂等记录与归属事实**同一事务**写入并提交，杜绝“响应可重放但事实缺失”。

因此无需 `SELECT ... FOR UPDATE`/唯一索引重试循环，主键竞争与事务阻塞本身
即裁决机制；唯一事实同时持久化在数据库中，服务重启后重放响应与查询结果不变。

## 目录

```
app/
  config.py     # 环境变量配置（DATABASE_URL / POSTGRES_*）
  database.py   # 引擎与会话工厂
  models.py     # band_first_seen / idempotent_requests
  schemas.py    # Pydantic 模型（强制带时区、规范化载荷）
  service.py    # 事务核心：咨询锁 + ON CONFLICT 裁决
  main.py       # FastAPI 路由
tests/
  test_api.py                   # API 功能测试
  acceptance/test_evacuation.py # 真实 PostgreSQL 并发事务验收
  acceptance/conftest.py        # spawn 独立进程/独立连接的并发工具
Dockerfile
docker-compose.yml   # db / api（2 worker）/ verify（一次性，profile=verify）
```

## 本地（非 Docker）运行测试

需要一个可连通的 PostgreSQL：

```bash
pip install -r requirements.txt
export DATABASE_URL=postgresql+psycopg://evac:evac@localhost:5432/evac
python -m pytest tests -v
```
