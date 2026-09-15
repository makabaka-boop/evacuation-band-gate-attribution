# 火警疏散腕带首次通过 API

纯后端服务：火警疏散时，同一腕带可能被相邻闸机几乎同时扫到。本服务保证
**同一腕带全局恰有一次 `first_seen`**，其余并发上报均为携带同一归属事实的
`already_seen`，使分区清点不会虚增人数。

疏散负责人还可按本次演练的应到名单创建**疏散名册**，随时核对尚未过闸人员：
名册复用腕带编号与既有首次通过事实做集合差分，不改变归属裁决与扫描响应。

演练开始前，值守人员可为每台闸机提交**巡检记录**（结论：可用/故障），负责人
按闸机号查询最近一次记录与闸机状态：记录只增不改，“最近一次”由数据库生成的
递增序号（提交顺序）裁决，与客户端检查时间无关；故障结论不阻断既有扫描。

演练进行中，调度员可把增援人员**派驻**到指定闸机，增援人员到岗后按派驻标识
确认：阶段只沿 待到岗 → 已到岗 单向推进，并发确认由数据库条件更新裁决 ——
恰有一个请求写入到岗时间，其余返回 409 且不覆盖原值；派驻复用闸机号命名
空间，但不读取也不改变巡检结论。

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

容器化运行的数据库配置均可用环境变量覆盖（api 与 verify 容器一致）：

```bash
# 指向独立数据库地址（优先于 POSTGRES_* 组装）
DATABASE_URL=postgresql+psycopg://user:pass@pg.example.com:5432/evac \
  docker compose up --build

# 或调整内置库的凭据与连接池容量
POSTGRES_USER=ops POSTGRES_PASSWORD='p@ss/w#rd' \
DB_POOL_SIZE=20 DB_MAX_OVERFLOW=10 \
  docker compose up --build
```

`POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` 含 `@` `:` `/` `%` `#`
等特殊字符时无需手工转义：服务组装连接地址时会自动做 URL 编码，凭据完整保留。

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
9. **名册差分**（见 `tests/acceptance/test_roster.py`）：部分成员过闸时未通过
   名单精确等于差集；全部过闸后未通过数归零；核对期间并发过闸的每次查询
   都是内部一致的快照（汇总 == 明细），只影响后续请求。
10. 非法创建（空名单/空或空白条目/重复腕带/纯空白标识或名称）返回 422 且
    不留残行；并发创建同一 `roster_id` 恰有一个 201、其余 409 且原名册保留；
    含斜杠的 `roster_id` 创建后可按原标识核对；未知名册查询 404。
11. **闸机巡检**（见 `tests/acceptance/test_inspection.py`）：检查时间乱序
    提交时，“最近一次”仍按提交顺序（数据库递增序号 `seq`）返回；并发提交
    同一 `inspection_id` 恰有一个 201、其余 409 且原记录保留；同一闸机的
    并发不同巡检全部追加落库。
12. 非法巡检提交（空白标识/闸机号、无时区检查时间、超长备注）返回 422 且
    不留残行；无记录闸机查询 404；故障结论不阻断既有扫描的并发归属与名册
    核对。
13. **增援派驻**（见 `tests/acceptance/test_deployment.py`）：创建后处于
    待到岗，到岗确认推进为已到岗，响应携带完整派驻事实与当前阶段；并发
    确认同一 `deployment_id` 恰有一个 200、其余 409 且先到岗时间不被覆盖；
    并发创建同一 `deployment_id` 恰有一个 201、其余 409 且原派驻保留。
14. 非法派驻/确认（空白标识/人员号/闸机号、无时区时间）返回 422 且不留
    残行；未知派驻确认/查询 404；派驻不读取也不改变巡检结论与扫描归属。

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

### `POST /rosters`

创建疏散名册（负责人提交已去重的应到腕带列表）：

```json
{
  "roster_id": "roster-3f-east",
  "name": "3F 东侧车间",
  "band_ids": ["band-01", "band-02", "band-03"]
}
```

- 成功返回 `201`：`{"roster_id", "name", "expected_count"}`。
- 空名单、空/空白条目或重复腕带返回 `422`，且不留任何残行；`roster_id`
  与 `name` 为纯空白（仅空格/制表/换行）同样在持久化前以 `422` 拒绝。
- `roster_id` 已存在返回 `409`，原名册及其成员分毫不动。

### `GET /rosters/{roster_id:path}`

按名册核对尚未过闸人员，响应按腕带编号稳定排序。路径使用 `:path` 转换器，
因此含斜杠的层级式 `roster_id`（如 `team-a/3f-east`）创建后仍可按原标识
逐字寻址（原始 `/` 与 URL 编码的 `%2F` 都会解码为同一标识）。

```json
{
  "roster_id": "roster-3f-east",
  "name": "3F 东侧车间",
  "expected_count": 3,
  "passed_count": 1,
  "missing_count": 2,
  "missing_band_ids": ["band-02", "band-03"]
}
```

名册不存在返回 `404`。核对是即时的：查询期间提交的扫描只影响后续请求。

### `POST /inspections`

提交一条闸机巡检记录（`checked_at` 必须带时区偏移，`notes` 可选）：

```json
{
  "inspection_id": "insp-001",
  "gate_id": "GATE-A",
  "checked_at": "2026-09-14T08:30:00+08:00",
  "conclusion": "available",
  "notes": "例行巡检"
}
```

- `conclusion` 取值：`available`（可用）/ `faulty`（故障）。
- 成功返回 `201`，响应为落库记录本体（含数据库生成的递增 `seq` 与
  `recorded_at`）。
- `inspection_id` 已存在返回 `409`，原记录分毫不动、不新增行。
- 空白 `inspection_id`/`gate_id`、无时区 `checked_at`、超长 `notes`
  （> 500 字符）在入库前返回 `422`，不留残行。

### `GET /gates/{gate_id}/inspections/latest`

按闸机号（与扫描载荷同一 `gate_id` 命名空间）查询最近一次巡检记录及闸机
可用/故障状态。“最近一次”按数据库生成的递增 `seq`（提交顺序）裁决，
不按客户端 `checked_at` 倒排：

```json
{
  "gate_id": "GATE-A",
  "status": "faulty",
  "latest_inspection": {
    "inspection_id": "insp-002",
    "gate_id": "GATE-A",
    "checked_at": "2026-09-14T00:40:00Z",
    "conclusion": "faulty",
    "notes": "门体异响",
    "seq": 2,
    "recorded_at": "2026-09-14T00:41:03.123456Z"
  }
}
```

该闸机无任何巡检记录返回 `404`。故障结论只反映在此状态查询中，不影响
扫描归属与名册核对。

### `POST /deployments`

调度员提交一次增援派驻（`deployed_at` 必须带时区偏移），成功即形成
“待到岗”记录：

```json
{
  "deployment_id": "dep-001",
  "responder_id": "resp-77",
  "gate_id": "GATE-A",
  "deployed_at": "2026-09-15T09:00:00+08:00"
}
```

- 成功返回 `201`，响应为完整派驻事实与当前阶段：`{"deployment_id",
  "responder_id", "gate_id", "deployed_at", "arrived_at": null,
  "phase": "pending", "recorded_at"}`。
- `deployment_id` 已存在返回 `409`，原派驻分毫不动、不新增行。
- 空白 `deployment_id`/`responder_id`/`gate_id`、无时区 `deployed_at`
  在入库前返回 `422`，不留残行。

### `POST /deployments/{deployment_id:path}/arrival`

增援人员按派驻标识确认到岗（`arrived_at` 必须带时区偏移）：

```json
{
  "arrived_at": "2026-09-15T09:07:00+08:00"
}
```

- 首个成功确认返回 `200`，响应为完整派驻事实，`phase` 推进为
  `"arrived"`，`arrived_at` 落定。
- 阶段只允许 待到岗 → 已到岗：并发/重复确认由数据库条件更新
  （`WHERE arrived_at IS NULL`）裁决，恰有一个请求写入到岗时间，其余
  返回 `409` 并回带先到岗时间，原值不被覆盖。
- 派驻不存在返回 `404`；无时区 `arrived_at` 返回 `422`。

### `GET /deployments/{deployment_id:path}`

按派驻标识查询完整派驻事实与当前阶段（`pending` / `arrived`），供调度员
核实增援是否真正到岗；不存在返回 `404`。

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

## 名册核对设计

`evacuation_rosters` 以 `roster_id` 为**主键**，`roster_members` 以
`(roster_id, band_id)` 为**联合主键**（外键级联），名册与成员关系持久化在
PostgreSQL。

- **创建**（`POST /rosters`，单事务）：`INSERT ... ON CONFLICT (roster_id)
  DO NOTHING RETURNING` —— 主键竞争即唯一性裁决，拿不到 RETURNING 行即
  抛 409 并整体回滚，原名册不动；名册行与成员行同一事务落库，非法请求
  （空名单、空/空白腕带、重复腕带、纯空白 `roster_id` 或 `name`）在
  Pydantic 层以 422 拒绝，根本不到数据库。
- **核对**（`GET /rosters/{roster_id:path}`）：`roster_members LEFT JOIN
  band_first_seen` 的**单条 SELECT** —— 成员集与首次通过事实取自同一事务
  快照，差分（未命中者即未过闸）与应到/已通过/未通过汇总由同一批行推导，
  数字与明细必然一致；语句执行期间提交的扫描对该快照不可见，只影响后续
  请求。名册只增不改、创建后必有至少一名成员，因此零行结果即名册不存在
  （404）。路径形参使用 `:path` 转换器，含 `/` 的标识（原始或 `%2F`
  编码）都按创建时的原标识逐字还原，保证凡已创建的名册均可寻址核对。

名册只读复用 `band_first_seen` 的既有事实，归属裁决与扫描响应完全不受影响。

## 闸机巡检设计

`gate_inspections` 是只增不改的追加式事实表：数据库生成的递增 `seq`
（IDENTITY）为主键，`inspection_id` 上有唯一约束。

- **提交**（`POST /inspections`，单事务）：`INSERT ... ON CONFLICT
  (inspection_id) DO NOTHING RETURNING seq` —— 唯一约束即重复裁决，拿不到
  RETURNING 行即抛 409 并整体回滚，原记录不动；`seq` 由数据库生成，递增
  顺序即提交顺序。空白标识/闸机号、无时区检查时间、超长备注在 Pydantic
  层以 422 拒绝，根本不到数据库。
- **查询**（`GET /gates/{gate_id}/inspections/latest`）：按 `gate_id` 取
  `seq` 最大的一行 —— “最近一次”由数据库序号（提交顺序）裁决，客户端
  `checked_at` 乱序无法反客为主；`status` 派生自该记录的结论。无记录
  返回 404。

巡检与扫描、名册完全解耦：故障结论只影响状态查询，不阻断既有扫描的并发
归属与名册核对。

## 增援派驻设计

`gate_deployments` 以 `deployment_id` 为**主键**，`arrived_at` 只能从 NULL
被写入一次 —— 阶段（待到岗 `pending` / 已到岗 `arrived`）由它派生，只沿
待到岗 → 已到岗 单向推进。

- **派驻**（`POST /deployments`，单事务）：`INSERT ... ON CONFLICT
  (deployment_id) DO NOTHING RETURNING` —— 主键竞争即唯一性裁决，拿不到
  RETURNING 行即抛 409 并整体回滚，原派驻不动；新记录 `arrived_at` 为
  NULL，即待到岗。空白标识/人员号/闸机号、无时区派驻时间在 Pydantic 层
  以 422 拒绝，根本不到数据库。
- **到岗确认**（`POST /deployments/{deployment_id:path}/arrival`，单事务）：
  `UPDATE ... WHERE deployment_id = :id AND arrived_at IS NULL ...
  RETURNING` —— 并发确认在数据库行锁上串行，获胜事务提交后被阻塞的事务
  按 READ COMMITTED 重估条件，发现 `arrived_at` 已非 NULL，拿不到
  RETURNING 行：恰有一个请求写入到岗时间（200），其余读取同一行后返回
  409（回带先到岗时间），原值分毫不动；条件更新未命中且该行不存在时
  返回 404。
- **查询**（`GET /deployments/{deployment_id:path}`）：按主键返回完整派驻
  事实与当前阶段；不存在返回 404。

派驻复用扫描/巡检载荷的 `gate_id` 命名空间，但与巡检完全解耦：既不读取
也不改变巡检结论，故障闸机照常接受派驻与到岗确认。

## 目录

```
app/
  config.py     # 环境变量配置（DATABASE_URL / POSTGRES_*）
  database.py   # 引擎与会话工厂
  models.py     # band_first_seen / idempotent_requests / evacuation_rosters / roster_members / gate_inspections / gate_deployments
  schemas.py    # Pydantic 模型（强制带时区、规范化载荷、名册去重与巡检/派驻校验）
  service.py    # 事务核心：咨询锁 + ON CONFLICT 裁决；名册快照差分；巡检追加与最新查询；派驻与条件更新到岗确认
  main.py       # FastAPI 路由
tests/
  test_api.py                    # API 功能测试（名册/巡检/派驻的 422/409/404 与残行检查）
  acceptance/test_evacuation.py  # 真实 PostgreSQL 并发事务验收
  acceptance/test_roster.py      # 名册差分/归零/快照一致性/并发创建验收
  acceptance/test_inspection.py  # 巡检乱序时钟/并发同号/追加落库/故障不阻断扫描验收
  acceptance/test_deployment.py  # 派驻到岗/并发确认唯一/并发创建唯一/解耦验收
  acceptance/conftest.py         # spawn 独立进程/独立连接的并发工具
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
