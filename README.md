# 有状态 API 场景模拟器（Stateful API Scenario Simulator）

用于联调的有状态 API Mock 服务：提交一份包含**路由、请求匹配条件、响应模板、状态迁移**的
YAML 场景，服务按**会话令牌**隔离每个调用方的当前状态，并在**单个 SQLite 写事务**中
完成「选择最高优先级匹配 → 渲染响应 → 推进状态」，保证同一会话的并发请求不会重复触发同一条迁移。

场景按**不可变版本（revision）**发布：每次 `PUT` 校验通过后产生递增 revision 并置为当前版本，
旧版本定义保留不覆盖；会话在创建时绑定当时的当前 revision，之后无论场景再发布多少新版本，
该会话的真实请求与预演都始终按绑定版本执行。

会话可携带**随机种子（seed）**：创建或重置会话时传入后，模板中的
`uuid4()` / `random_str()` / `randint()` / `choice()` 按会话级确定性序列取值——
同一 seed、同一初始状态与同一请求链必然产生相同结果；随机状态随会话上下文在
同一个 SQLite 写事务中推进，会话查询中可看到累计抽样步数。未传 seed 时保持
原有的进程级随机行为与接口兼容。

会话支持**快照分支（fork）**：`POST /__sessions/{token}/fork` 在单个 SQLite 写事务中
读取源会话并创建新 token，完整复制绑定的 scenario revision、state、业务 context、
`seq()` 计数器、seed 随机序列位置与**虚拟时钟**（`clock_ms`/超时到期时刻）——分支后
首个相同请求（含时钟推进）得到与源会话同快照一致的结果；事件历史不复制，之后两条
会话独立推进。

会话拥有独立的**虚拟时钟**：场景可为状态声明 `after_ms`/`next_state`/可选 `set` 的
**超时自动迁移**；`POST /__sessions/{token}/advance` 以 `delta_ms` 推进该会话时钟，
并在**单个 SQLite 写事务**中按到期时刻连续执行迁移，返回每步 `from`/`to`/`at` 与
最终快照。请求迁移先离开原状态时其旧超时立即失效；同一 advance 内新状态产生的超时
继续结算，零延迟超时环会被检测并**原子回滚**。创建/重置会话时钟归零。

场景还支持**版本回归对比**：`POST /__scenarios/{name}/compare` 输入两个 revision、
可选 seed 与最多 20 步操作链（业务请求或虚拟时钟推进），两侧分别从各自版本的
`initial_state` 建立**独立的纯内存快照**，以相同 seed 同步执行并逐步比较响应、
状态、context、`clock_ms` 与超时迁移；**首次差异后两侧仍各自继续**，返回每步双侧
结果与 `first_difference`，任一侧单步失败只记录该侧、不影响另一侧。整个过程
**不写 sessions/events、不持久化随机计数**，可反复调用。

服务提供**会话事件订阅（SSE）**：`GET /__events/stream` 按 `events` 表的全局
递增 `id` 推送全部已提交事件（业务命中与超时迁移均在其中），支持按 `token`
过滤；客户端用 `Last-Event-ID` 请求头或 `after_id` 查询参数指定补发游标
（同传时以请求头为准），未传游标则从**连接时**最新事件之后开始只收实时事件。
补发按 id 分批进行并与实时推送无缝衔接，**不漏发、不重复**；每个事件的 SSE
`id` 即行 id，浏览器 `EventSource` 断连重连自动续传；空闲时发送心跳，非法游标
返回 `422`。

- 技术栈：Python 3.12 · FastAPI · SQLite（WAL）· Jinja2 沙箱模板 · PyYAML
- 容器内启动即自动完成依赖安装与数据库初始化（含旧库就地升级），并自动加载 `scenarios/` 下的示例场景

---

## 一条命令启动

```bash
docker compose up --build -d
```

服务地址：<http://localhost:8080>

| 用途 | 地址 |
| --- | --- |
| 健康检查 | <http://localhost:8080/__health> |
| Swagger 文档 | <http://localhost:8080/docs> |
| 场景管理 | `http://localhost:8080/__scenarios` |
| 版本回归对比 | `POST http://localhost:8080/__scenarios/{name}/compare` |
| 会话状态 | `http://localhost:8080/__sessions/{token}` |
| 虚拟时钟推进 | `POST http://localhost:8080/__sessions/{token}/advance` |
| 会话快照分支 | `POST http://localhost:8080/__sessions/{token}/fork` |
| 请求预演 | `POST http://localhost:8080/__sessions/{token}/preview` |
| 连续请求链预演 | `POST http://localhost:8080/__sessions/{token}/preview/chain` |
| 事件订阅（SSE） | `GET http://localhost:8080/__events/stream` |
| 模拟业务流量 | 除上述管理前缀外的任意路径，如 `POST http://localhost:8080/orders` |

停止：`docker compose down`（数据保留在命名卷 `simulator-data` 中；加 `-v` 一并清除）。

不使用容器时：

```bash
pip install -r requirements.txt
SIM_DB_PATH=./data/simulator.db SIM_SCENARIOS_DIR=./scenarios \
  uvicorn app.main:app --host 0.0.0.0 --port 8080
```

---

## 快速体验

镜像自带 `scenarios/order-flow.yaml`（下单 → 支付 → 发货 → 收货，含取消分支；
`created` 状态声明了 30 分钟未支付自动取消的虚拟时钟超时）。

```bash
BASE=http://localhost:8080

# 1) 用令牌 t1 发起第一次业务请求（会话自动创建，初始状态 init）
curl -s -X POST $BASE/orders \
  -H 'sim-session-token: t1' -H 'Content-Type: application/json' \
  -d '{"sku":"A-1","quantity":2}'
# -> 201, 状态 init -> created

# 2) 支付（created -> paid）
curl -s -X POST $BASE/orders/42/pay \
  -H 'sim-session-token: t1' -H 'Content-Type: application/json' \
  -d '{"channel":"alipay"}'

# 3) 查询该会话当前状态与上下文
curl -s $BASE/__sessions/t1

# 4) 查询请求/迁移事件历史
curl -s "$BASE/__sessions/t1/events"

# 5) 虚拟时钟：另一下单会话不支付，推进 30 分钟虚拟时间 -> 自动取消
curl -s -X POST $BASE/orders -H 'sim-session-token: t2' \
  -H 'Content-Type: application/json' -d '{"sku":"B-2","quantity":1}'
curl -s -X POST $BASE/__sessions/t2/advance \
  -H 'Content-Type: application/json' -d '{"delta_ms":1800000}'
# -> transitions: [{from: created, to: cancelled, at: 1800000, ...}]
```

每个模拟响应还带有调试响应头：

- `X-Sim-Matched: order-flow:pay-order:pay-created`（场景:路由:规则）
- `X-Sim-Revision: 1`（实际执行的场景 revision）
- `X-Sim-State: created->paid`（迁移前后状态）

---

## 场景 YAML 写法

```yaml
name: order-flow                 # 必填，场景名（字母数字 . _ -）
description: 可选描述
initial_state: init              # 新会话的初始状态
# states 元素可直接写字符串（无超时），也可写对象声明虚拟时钟超时：
states:
  - init
  - name: created
    after_ms: 1800000            # 进入 created 后虚拟时间经过 30 分钟
    next_state: cancelled        # 到期自动迁移到 cancelled（须在 states 内）
    set:                        # 可选：超时迁移时写入会话上下文（支持模板）
      cancel_reason: payment_timeout
  - paid
  - shipped
  - completed
  - cancelled

routes:
  - id: pay-order                # 路由 id，仅用于标识
    path: /orders/{order_id}/pay # 支持 {path_param} 路径参数
    methods: [POST]              # 默认 [GET, POST]
    rules:                       # 同一路由下按 priority 从高到低匹配
      - name: pay-created
        priority: 100
        when:                    # 所有条件为 AND；全部满足即命中
          - source: state        # state: 当前会话状态
            op: eq
            value: created
          - source: body         # body / query / header / path / state
            path: channel        # 点路径，支持 a.b.0 与 a[0].b
            op: in
            value: [alipay, wechat, card]
        response:
          status: 200            # 也支持 Jinja 模板，如 "{{ 200 if body.ok else 400 }}"
          headers:
            Content-Type: application/json
          body:                  # dict/list 自动按 JSON 返回；字符串按文本返回
            order_id: "{{ path.order_id }}"
            trace: "{{ uuid4() }}"
            paid_at: "{{ now('%Y-%m-%dT%H:%M:%SZ') }}"
        set:                     # 命中后写入会话上下文（持久化）
          pay_channel: "{{ body.channel }}"
        next_state: paid         # 命中后迁移；省略则状态不变
        delay_ms: 0              # 可选：响应前延迟（模拟慢接口）

      - name: double-pay-rejected # 低优先级兜底规则
        priority: 50
        when:
          - {source: state, op: in, value: [paid, shipped, completed]}
        response:
          status: 409
          body: {error: already_paid}
```

### 匹配条件（when）

| 字段 | 说明 |
| --- | --- |
| `source` | `body`（默认，JSON 解析）、`query`、`header`（键名小写）、`path`（路径参数）、`state` |
| `path` | 点路径：`a.b.0` / `a[0].b`；`source=state` 时忽略 |
| `op` | `eq` `ne` `gt` `gte` `lt` `lte` `in` `contains` `starts_with` `ends_with` `regex` `exists` `truthy` |
| `value` | 比较右值；`eq/ne/gt...` 会尽量按数字比较（如 `"2"` 与 `2` 相等） |

匹配语义：规则按 `priority` 降序（相同按定义顺序），**取第一条全部条件满足的规则**；
没有规则命中时返回 `409 no_matching_rule`（不会推进状态）；路径无匹配返回 `404`，
方法不支持返回 `405`。

### 状态超时（虚拟时钟自动迁移）

`states` 中的元素可写成对象，为该状态声明一条**超时自动迁移**：

```yaml
states:
  - init
  - name: awaiting_payment
    after_ms: 30000            # 必填：进入状态后经过多少虚拟毫秒到期（非负整数，允许 0）
    next_state: expired        # 必填：到期迁移目标（必须在 states 中声明）
    set:                       # 可选：迁移时写入会话上下文（与规则的 set 同套 Jinja 渲染）
      reason: payment_timeout
      trace: "{{ uuid4() }}"
  - expired
```

- 发布时校验：`after_ms` 为**非负整数**（不接受布尔/负数）、`next_state` 在
  `states` 内、`set` 为对象、状态名不重复（**同一状态至多一条超时**）、
  `set` 模板语法正确；不声明超时的状态仍可直接写字符串；
- 会话**进入**带超时的状态（创建/重置、业务请求迁入、上一次超时迁入）时，
  按「当前虚拟时钟 + `after_ms`」调度到期时刻；
- 业务请求迁移**先离开原状态**（如用户提前支付）时，其旧超时**立即失效**，
  不会在之后补触发；规则命中但状态不变时不重新调度；
- 超时迁移的 `set` 与真实请求同一套渲染器：可调用 `uuid4()`/`seq()` 等，
  seed 会话的随机序列在超时迁移中同样按确定性序列推进；
- 不声明任何超时的场景行为与旧版完全一致（`advance` 只推进时钟、无迁移）。

### 响应模板（Jinja2 沙箱）

- 整个值写成单个 `"{{ expr }}"` 时保留原生类型（数字、布尔、列表、对象）；
  嵌在字符串中则渲染为文本。
- 可用变量：`state`、`body`、`query`、`header`、`path`、`context`（已持久化的上下文）、
  `request`（`method`/`path`/`route`）。
- 内置函数：`uuid4()`、`now(fmt)`、`random_str(n, prefix='')`、`randint(a,b)`、
  `choice(list)`、`seq(name, start=1)`（会话内自增计数器，跨请求持久化）。
  会话传入 `seed` 后，`uuid4()`/`random_str()`/`randint()`/`choice()` 按会话级
  确定性序列取值（见下文「可复现随机序列（seed）」）。
- 过滤器：Jinja2 标准过滤器（如 `body.x | default('v')`）。
- 响应体 `body` 为 dict/list 时按 JSON 返回；为字符串时配合 `Content-Type` 头
  可返回 HTML/纯文本等。

### 校验规则

载入时会校验：场景名合法、`initial_state` 在 `states` 内、状态名不重复、
状态超时的 `after_ms` 为非负整数且 `next_state` 已声明（同状态超时唯一）、
路径/方法合法、条件 `source/op` 合法、`next_state` 已声明、状态码范围、
模板语法正确（规则与超时 `set`）、路由 id 与「路径+方法」组合不重复、
业务路径不得占用 `/__*`、`/docs` 等管理前缀。

完整示例见 [`scenarios/order-flow.yaml`](scenarios/order-flow.yaml)。

---

## 管理接口

### 场景

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/__scenarios/validate` | 只校验不入库。body 为 YAML 文本（`Content-Type: application/yaml`）或 JSON |
| GET | `/__scenarios` | 列出全部场景及解析后的结构（含 `current_revision` 与版本数 `revisions`） |
| GET | `/__scenarios/{name}` | 查看单个场景摘要（当前版本） |
| GET | `/__scenarios/{name}/yaml` | 取回原始 YAML；`?revision=N` 可取指定历史版本 |
| PUT | `/__scenarios/{name}` | **校验并发布**：产生递增 revision 并置为当前版本；内容与当前版本完全相同时幂等返回原 revision |
| GET | `/__scenarios/{name}/revisions` | **版本列表**：全部不可变 revision，标注当前版本 |
| GET | `/__scenarios/{name}/revisions/{rev}` | 查看指定 revision 的解析结构 |
| POST | `/__scenarios/{name}/compare` | **版本回归对比**：两个 revision 从各自 `initial_state` 建独立纯内存快照，同 seed 执行最多 20 步操作链（业务请求/时钟推进），逐步比较响应、状态、context、`clock_ms`、超时迁移；revision 不存在返回 404 |
| POST | `/__scenarios/{name}/revisions/{rev}/activate` | **激活**指定 revision 为当前版本（只影响之后新建的会话）；目标不存在返回 404 |
| DELETE | `/__scenarios/{name}` | 删除场景及其全部版本；仍有会话引用时返回 409，加 `?force=true` 级联删除 |

```bash
# 校验
curl -s -X POST $BASE/__scenarios/validate \
  -H 'Content-Type: application/yaml' --data-binary @my-scenario.yaml

# 发布（文件名/场景名任意，以 URL 中的 name 为准）
curl -s -X PUT $BASE/__scenarios/my-scenario \
  -H 'Content-Type: application/yaml' --data-binary @my-scenario.yaml
# -> {"loaded":true,"action":"created","revision":1,"current_revision":1,...}
#    再次 PUT 修改后的定义 -> {"action":"published","revision":2,...}
#    重复 PUT 与当前版本相同的内容 -> {"action":"unchanged","revision":2,...}（幂等，不产生新版本）
```

#### 场景版本（revision）与会话绑定

- 每次 `PUT` 校验通过后，定义作为**新的不可变 revision** 落库并置为当前版本，
  旧版本保留在 `scenario_revisions` 表中，永不覆盖；
- **新会话**（显式 `POST /__sessions` 或首个业务请求懒创建）绑定**当时的当前
  revision**，之后该会话的真实请求、单次预演与请求链预演都按该版本执行——
  场景再发布新版本或激活旧版本都**不影响既有会话**；
- **激活**只切换「新会话的默认版本」，用于回滚：把旧 revision 置为当前版本后，
  只有之后新建的会话使用它；
- 场景查询、会话查询、事件历史与模拟响应（`X-Sim-Revision` 头）都会暴露实际
  revision；再次发布时 revision 始终取 `max+1`（激活过旧版本也不会回绕或覆盖）。

```bash
# 查看版本列表（current 标注新会话默认版本）
curl -s $BASE/__scenarios/order-flow/revisions
# -> {"name":"order-flow","current_revision":2,
#     "revisions":[{"revision":1,"created_at":"...","current":false},
#                  {"revision":2,"created_at":"...","current":true}]}

# 回滚：激活 revision 1（只影响之后新建的会话）
curl -s -X POST $BASE/__scenarios/order-flow/revisions/1/activate

# 取回历史版本的原始 YAML
curl -s "$BASE/__scenarios/order-flow/yaml?revision=1"
```

#### 版本回归对比（compare）

发布新版本前，用 `POST /__scenarios/{name}/compare` 把**两个不可变 revision** 放在
同一条操作链上逐步对照：两侧分别从**各自版本的 `initial_state`** 建立**独立的纯
内存快照**（时钟归零、context 为空），以**相同 seed** 从头初始化确定性随机序列，
然后按操作链**同步**执行并逐步比较——整个过程不创建任何会话：

- 请求体：`revision_a` / `revision_b`（两个必填整数）、可选 `seed`
  （字符串/数字/布尔，两侧共用）、`steps`（**最多 20 步**，超出返回 422）；
  任一侧 revision 不存在返回 `404 revision_not_found`；`steps` 可省略（空链）；
- 每一步是以下两种之一（按 `type` 区分，**省略 `type` 即业务请求**）：
  - `{"type":"request", "path":"/orders/42/pay", "method":"POST", "query":{},
    "headers":{}, "body":{...}}` —— 字段语义与请求预演完全相同（`method` 默认
    `GET`，`body` 默认 `{}`）；
  - `{"type":"advance", "delta_ms":1800000}` —— 虚拟时钟推进（非负整数，
    拒绝负数/布尔/浮点），窗口内到期的状态超时按到期时刻连续结算，
    零延迟超时环按真实 advance 的语义让**该侧本步回滚**（快照保持本步之前）；
- 逐步比较的字段：业务请求的**拟响应**（状态码/头/体）、**state**、业务
  **context**、**`clock_ms`**、**`timeout_at_ms`/`pending_timeout`** 与
  **超时迁移列表**（`from`/`to`/`at`/`after_ms`/`set`）；差异以点路径递归给出
  （`a`/`b` 双侧取值），严格区分类型（`"200"` ≠ `200`）；
- **首次差异后链路不中断**：两侧继续各自执行后续步骤（状态分歧后同一请求可能在
  两侧命中不同规则），响应顶层给出 `first_difference`（首个差异的步骤下标、类型
  与字段清单）；`identical` 表示所有步骤是否完全一致；
- **任一侧步骤失败只记录该侧结果**：路由 404/405、无规则 409、渲染 500 或
  advance 的零延迟环 409/链过长 422 时，该步该侧标记 `ok=false` 并给出
  `failure`（`kind` 为 `route`/`rule`/`render`/`timeout`、状态码、错误体与判定
  轨迹），**该侧快照保持本步之前的值**（与真实引擎“失败不消耗 seq/随机序列”一致），
  另一侧照常执行，后续步骤两侧也都继续；
- **零持久化副作用**：不写 `sessions`、不写 `events`，`seq()` 计数与 seed 随机
  序列只存在于两侧各自的内存渲染器中；重复调用结果可重复（模板中的 `now()` 与
  未 seed 的 `uuid4()` 等运行时值除外）；
- 响应还给出链路前的 `initial_snapshots`（两侧初始 state 与初始超时调度，
  可直接看出 `after_ms` 差异）与执行后的 `final_snapshots`。

```bash
# 先准备两个 revision：revision 1 的 created 超时为 30 分钟，
# 修改 after_ms 为 20 分钟并调整取消原因后再发布一次 -> revision 2
curl -s -X PUT $BASE/__scenarios/order-flow \
  -H 'Content-Type: application/yaml' --data-binary @scenarios/order-flow.yaml
# （编辑一份 after_ms: 1200000 的 order-flow-v2.yaml 后）
curl -s -X PUT $BASE/__scenarios/order-flow \
  -H 'Content-Type: application/yaml' --data-binary @order-flow-v2.yaml

# 对比：下单 -> 推进 25 分钟虚拟时间 -> 再次支付
curl -s -X POST $BASE/__scenarios/order-flow/compare \
  -H 'Content-Type: application/json' -d '{
    "revision_a": 1,
    "revision_b": 2,
    "seed": "regression-1",
    "steps": [
      {"path": "/orders", "method": "POST", "body": {"sku": "A-1", "quantity": 2}},
      {"type": "advance", "delta_ms": 1500000},
      {"path": "/orders/1000/pay", "method": "POST", "body": {"channel": "alipay"}}
    ]
  }'
```

响应（节选；注释为说明，实际无注释）：

```json
{
  "compare": {"persisted": false},
  "scenario": "order-flow",
  "seed": "regression-1",
  "revisions": {"a": 1, "b": 2},
  "steps_total": 3,
  "steps_executed": 3,
  "identical": false,
  "first_difference": {
    "index": 0,
    "type": "request",
    "fields": ["timeout_at_ms", "pending_timeout.after_ms",
               "pending_timeout.at_ms", "pending_timeout.remaining_ms"]
  },
  "initial_snapshots": {
    "a": {"revision": 1, "state": "init", "clock_ms": 0, "timeout_at_ms": null, "...": "..."},
    "b": {"revision": 2, "state": "init", "clock_ms": 0, "timeout_at_ms": null, "...": "..."}
  },
  "steps": [
    {"index": 0, "type": "request", "identical": false,
     "differences": [
       {"field": "timeout_at_ms", "a": 1800000, "b": 1200000},
       {"field": "pending_timeout.at_ms", "a": 1800000, "b": 1200000}
     ],
     "a": {"ok": true, "response": {"status_code": 201, "body": {"order_id": 1000,
           "trace": "d9acb9b4-…"}}, "state": "created",
           "clock_ms": {"before": 0, "after": 0},
           "timeout_at_ms": {"before": null, "after": 1800000},
           "timeout_transitions": [], "...": "..."},
     "b": {"ok": true, "response": {"status_code": 201, "body": {"order_id": 1000,
           "trace": "d9acb9b4-…"}}, "state": "created",
           "timeout_at_ms": {"before": null, "after": 1200000}, "...": "..."}
    },
    {"index": 1, "type": "advance", "delta_ms": 1500000, "identical": false,
     "differences": [
       {"field": "state", "a": "created", "b": "cancelled"},
       {"field": "context.cancel_reason", "a": null, "b": "payment_timeout"},
       {"field": "pending_timeout", "a": {"state": "created", "after_ms": 1800000,
        "at_ms": 1800000, "remaining_ms": 300000}, "b": null}
     ],
     "a": {"ok": true, "state": "created", "clock_ms": {"before": 0, "after": 1500000},
           "timeout_transitions": []},
     "b": {"ok": true, "state": "cancelled", "clock_ms": {"before": 0, "after": 1500000},
           "timeout_transitions": [
             {"index": 0, "from": "created", "to": "cancelled",
              "at": 1200000, "after_ms": 1200000,
              "set": {"cancel_reason": "payment_timeout"}}]}
    },
    {"index": 2, "type": "request", "identical": false,
     "a": {"ok": true, "response": {"status_code": 200, "body": {"paid": true}},
           "state": "paid", "...": "..."},
     "b": {"ok": true, "response": {"status_code": 400,
           "body": {"error": "invalid_channel"}}, "state": "cancelled", "...": "..."}
    }
  ],
  "final_snapshots": {
    "a": {"revision": 1, "state": "paid", "clock_ms": 1500000,
          "context": {"sku": "A-1", "quantity": 2, "pay_channel": "alipay"},
          "seq": {"order": 1001}, "random": {"seed": "regression-1", "draws": 2}},
    "b": {"revision": 2, "state": "cancelled", "clock_ms": 1500000,
          "context": {"sku": "A-1", "quantity": 2, "cancel_reason": "payment_timeout"},
          "seq": {"order": 1001}, "random": {"seed": "regression-1", "draws": 1}}
  }
}
```

要点回顾：第 0 步两侧下单响应一致（同 seed 时 `trace` 与 `order_id` 完全相同，
差异仅可能体现在进入 `created` 后的超时调度时刻）；第 1 步推进 25 分钟后
revision 1 尚未到期（30 分钟）而 revision 2 已自动取消（20 分钟），成为首个差异；
**链路并未停止**——第 2 步同一支付请求在 a 侧正常支付（200），在 b 侧落到兜底
规则（400）。若某一步某侧请求了不存在的路径，该侧该步为
`{"ok": false, "failure": {"kind": "route", "status_code": 404, ...}}` 且快照
不变，另一侧与后续步骤照常执行；对比结束后数据库中没有新增任何会话或事件。

### 会话

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/__sessions` | 显式创建会话：`{"token":"t1","scenario":"order-flow","state":"created","context":{}}`，字段均可省略；可传 `"seed"` 启用可复现随机序列；虚拟时钟归零并绑定**当前 revision**（响应中返回） |
| GET | `/__sessions/{token}` | **会话状态查询**：当前状态、绑定的 `revision`、`clock_ms`（虚拟时钟）、`timeout_at_ms`（当前状态超时到期时刻，无则 `null`）、`pending_timeout`（待触发超时与 `remaining_ms`）、上下文、随机序列摘要 `random`、可用状态、路由列表（按绑定版本给出） |
| POST | `/__sessions/{token}/advance` | **虚拟时钟推进**：body `{"delta_ms": 1800000}`（非负整数），在单个写事务内按到期时刻连续结算状态超时，返回每步 `from`/`to`/`at` 与最终快照；零延迟超时环返回 409 并原子回滚 |
| GET | `/__sessions/{token}/events?limit=50` | 最近的匹配与状态迁移历史（新→旧，含每次命中时的 `revision`；超时迁移记 `method=ADVANCE`、`route=@timeout`、`rule=timeout:<from>`） |
| POST | `/__sessions/{token}/fork` | **会话快照分支**：单写事务内复制源会话绑定的 revision、state、context、seq、随机序列位置与虚拟时钟（clock_ms/timeout_at_ms）到新 token（可指定目标 token，冲突 409；源不存在 404；绑定版本已卸载 410）；事件历史不复制，分支后两条会话独立推进 |
| POST | `/__sessions/{token}/preview` | **请求预演**：不落库地试运行一次业务请求，返回拟响应与 state/context 差异（按会话绑定的 revision 执行） |
| POST | `/__sessions/{token}/preview/chain` | **连续请求链预演**：有序请求数组基于同一会话快照依次试跑（最多 50 步），逐步返回命中/拟响应/差异与最终快照，不落库 |
| POST | `/__sessions/{token}/reset` | 重置到初始（或指定）状态、虚拟时钟归零并清空事件；不改变会话绑定的 revision；可传 `"seed"` 让随机序列从头初始化 |
| DELETE | `/__sessions/{token}` | 删除会话及其事件 |

#### 可复现随机序列（seed）

创建会话或重置会话时传入 `seed`（字符串/数字/布尔），该会话即获得一条
**确定性随机序列**：模板中的 `uuid4()`、`random_str()`、`randint()`、
`choice()` 不再使用进程级随机源，而是从会话自带的随机状态依次取值。
**同一 seed、同一初始状态与同一请求链，必然产生完全相同的随机结果**——
联调用例与排障回放因此可以逐字节复现。

- 随机状态（seed + 序列位置）作为会话上下文的一部分持久化，并在**同一个
  SQLite 写事务**中随状态迁移一起推进；请求处理失败（无路由、无命中规则、
  渲染错误等）时随机序列**不会被消耗**，修正后重试仍得到同一个值；
- 每调用一次上述任一函数计 **1 步抽样**；`GET /__sessions/{token}` 响应的
  `random` 字段暴露 seed 与累计抽样步数 `draws`（未传 seed 的会话为 `null`）；
- 单次预演与请求链预演都**克隆**当前随机状态：链内后一步能看到前一步的
  消耗（每步结果附 `rng` 字段，链预演 `final_snapshot.random` 给出拟最终
  步数），但预演结束后真实序列保持不变；
- `reset` 时再次传入 `seed` 可让随机序列**从头初始化**；不传则重置后的
  会话回到进程级随机行为；
- 未传 `seed` 的会话（含懒创建会话）保持原有行为：四个函数使用进程级
  随机源，结果不可复现，接口字段完全兼容。

```bash
# 两个会话使用同一 seed + 同一初始状态
curl -s -X POST $BASE/__sessions -H 'Content-Type: application/json' \
  -d '{"token":"s1","state":"created","seed":"demo-1"}'
curl -s -X POST $BASE/__sessions -H 'Content-Type: application/json' \
  -d '{"token":"s2","state":"created","seed":"demo-1"}'

# 发出相同请求，响应中的 uuid4() 完全一致
curl -s -X POST $BASE/orders/7/pay -H 'sim-session-token: s1' \
  -H 'Content-Type: application/json' -d '{"channel":"alipay"}'
curl -s -X POST $BASE/orders/7/pay -H 'sim-session-token: s2' \
  -H 'Content-Type: application/json' -d '{"channel":"alipay"}'
# -> 两个响应体的 trace 字段相同

# 会话查询暴露累计抽样步数
curl -s $BASE/__sessions/s1
# -> {"token":"s1", ..., "random":{"seed":"demo-1","draws":1}}

# 重置并重新播种：随机序列从头开始
curl -s -X POST $BASE/__sessions/s1/reset -H 'Content-Type: application/json' \
  -d '{"state":"created","seed":"demo-1"}'
```

#### 会话快照分支（fork）

`POST /__sessions/{token}/fork` 把源会话的**当前快照**完整复制到一个新 token：
在**单个 SQLite 写事务**中读取源会话并插入新会话，复制内容包括绑定的
scenario revision、当前 `state`、业务 `context`、`_seq` 计数器（`seq()` 进度）、
seed 随机序列位置（`_rng`）以及**虚拟时钟**（`clock_ms` 与当前状态的
`timeout_at_ms`）。新会话与源会话在此刻完全同快照——
**对两条会话先后发出同一个请求或同样的 advance，得到的第一份结果一致**
（seed 会话的 `uuid4()` 等随机值、`seq()` 计数与时钟到期时刻都从同一位置继续）。

- 请求体可选：`{"token": "t2"}` 指定目标 token；不传或传空 body 时自动生成
  `sess-...` 令牌；目标 token 已存在返回 `409 session_exists`；
- 源会话不存在返回 `404 session_not_found`；源会话绑定的场景版本已卸载
  返回 `410 scenario_unloaded`；
- **事件历史不复制**：新会话的 `events` 从空开始；分支后两条会话各自独立
  推进，互不影响（各自的状态迁移、`seq()` 与随机序列都只写自己的行）；
- 响应为 `201`，返回源/目标标识（`source_token` / `token`）与快照摘要
  （`scenario` / `revision` / `state` / `context` / `random`），
  `events_copied` 恒为 `false`。

```bash
# 1) 源会话推进到 paid：下单（消耗 seq）+ 支付（消耗 uuid4 随机序列）
curl -s -X POST $BASE/__sessions -H 'Content-Type: application/json' \
  -d '{"token":"t1","seed":"demo-1"}'
curl -s -X POST $BASE/orders -H 'sim-session-token: t1' \
  -H 'Content-Type: application/json' -d '{"sku":"A-1","quantity":2}'
curl -s -X POST $BASE/orders/1000/pay -H 'sim-session-token: t1' \
  -H 'Content-Type: application/json' -d '{"channel":"alipay"}'

# 2) 在 paid 快照上分支出 t2（省略 body 则自动生成目标 token）
curl -s -X POST $BASE/__sessions/t1/fork -H 'Content-Type: application/json' \
  -d '{"token":"t2"}'
# -> 201 {"forked":true,"source_token":"t1","token":"t2","scenario":"order-flow",
#         "revision":1,"state":"paid","context":{"_seq":{"order":1001},"sku":"A-1",...},
#         "random":{"seed":"demo-1","draws":1},"events_copied":false,...}

# 3) 分支后首个相同请求：t1 与 t2 得到一致结果（同快照、同随机序列位置）
curl -s -X POST $BASE/orders/1000/ship -H 'sim-session-token: t1' \
  -H 'Content-Type: application/json' -d '{"carrier":"sf"}'
curl -s -X POST $BASE/orders/1000/ship -H 'sim-session-token: t2' \
  -H 'Content-Type: application/json' -d '{"carrier":"sf"}'

# 4) 此后两条会话独立推进；t2 的事件历史从空开始，只记录分支后的请求
curl -s "$BASE/__sessions/t2/events"
```

#### 虚拟时钟推进（advance）

每个会话拥有独立的**虚拟时钟** `clock_ms`（创建/重置时为 `0`，业务请求不推进
时钟）。`POST /__sessions/{token}/advance` 以 `delta_ms`（非负整数）推进时钟，
并在**单个 SQLite 写事务**中把推进窗口内到期的状态超时**按到期时刻依次结算**：
同一次 advance 内迁移到新状态后产生的超时，只要到期时刻仍在窗口内就会继续触发，
因此一次调用可能产生多步迁移链。

- 请求体仅 `{"delta_ms": <非负整数>}`；负数/布尔/浮点返回 422；会话不存在 404；
  绑定版本已卸载 410；
- 业务请求若**先把会话迁出原状态**（例如用户提前支付），其旧超时**立即失效**，
  之后再 advance 也不会补触发；规则命中但状态不变时保留原到期时刻；
- 每一步超时迁移都在同一写事务内：渲染可选 `set`（`seq()`/seed 随机序列随之
  推进）、更新会话、写入一条 `method=ADVANCE`/`route=@timeout`/
  `rule=timeout:<from状态>` 的事件（`status_code` 为 `null`）；
- 响应给出 `clock_ms.before/after`、逐步 `transitions`（`from`/`to`/`at`/
  `after_ms`/`set`）与 `final_snapshot`（含 `state`/`context`/`seq`/`random`/
  `clock_ms`/`timeout_at_ms`/`pending_timeout`）；
- **零延迟超时环**（`after_ms: 0` 的迁移在同一虚拟时刻重复进入同一状态，
  包括自环）会返回 `409 timeout_cycle`，整个 advance **原子回滚**——状态、
  时钟、上下文、seq、随机序列与事件都保持调用前不变，响应体中
  `attempted_transitions` 给出回滚前已尝试的各步；正延迟环在一次调用内步数
  超过 10000 时以 `422 timeout_chain_too_long` 同样原子回滚；
- 超时 `set` 渲染失败返回 `500 render_error` 并回滚；
- 状态没有声明超时时，advance 只是把时钟前移（无 `transitions`）。

```bash
# 下单后会话进入 created（30 分钟未支付自动取消）
curl -s -X POST $BASE/orders -H 'sim-session-token: t1' \
  -H 'Content-Type: application/json' -d '{"sku":"A-1","quantity":2}'

# 查询：时钟为 0，超时到期时刻为 1800000ms
curl -s $BASE/__sessions/t1
# -> {"state":"created","clock_ms":0,"timeout_at_ms":1800000,
#     "pending_timeout":{"state":"created","after_ms":1800000,
#                        "at_ms":1800000,"remaining_ms":1800000}, ...}

# 推进虚拟时钟 30 分钟：到期迁移 created -> cancelled
curl -s -X POST $BASE/__sessions/t1/advance \
  -H 'Content-Type: application/json' -d '{"delta_ms":1800000}'
```

响应（节选）：

```json
{
  "advanced": true,
  "token": "t1",
  "scenario": "order-flow",
  "revision": 1,
  "clock_ms": {"before": 0, "after": 1800000},
  "delta_ms": 1800000,
  "transitions": [
    {"index": 0, "from": "created", "to": "cancelled", "at": 1800000,
     "after_ms": 1800000, "set": {"cancel_reason": "payment_timeout"}}
  ],
  "final_snapshot": {
    "state": "cancelled",
    "context": {"sku": "A-1", "quantity": 2, "cancel_reason": "payment_timeout"},
    "seq": {"order": 1000},
    "random": null,
    "clock_ms": 1800000,
    "timeout_at_ms": null,
    "pending_timeout": null
  }
}
```

超时链（多个状态的到期时刻都落在窗口内）会在一次 advance 中连续结算，例如
`b --100ms--> c --150ms--> d`，推进 `delta_ms=200` 得到两步：

```json
"transitions": [
  {"index": 0, "from": "b", "to": "c", "at": 100, "after_ms": 100, "set": {}},
  {"index": 1, "from": "c", "to": "d", "at": 150, "after_ms": 50,  "set": {}}
]
```

零延迟环回滚示例（`a --0ms--> b --0ms--> a`）：

```bash
curl -s -X POST $BASE/__sessions/lc/advance -H 'Content-Type: application/json' \
  -d '{"delta_ms":10}'
# -> 409
```

```json
{
  "error": "timeout_cycle",
  "detail": "检测到零延迟超时环：状态 a 在同一虚拟时刻 0ms 被重复进入，本次 advance 已原子回滚",
  "at_ms": 0,
  "cycle": {"from": "b", "to": "a"},
  "attempted_transitions": [
    {"index": 0, "from": "a", "to": "b", "at": 0, "after_ms": 0, "set": {}}
  ],
  "rolled_back": true
}
```

#### 请求预演（dry-run）

在**不改动会话**的前提下，基于该会话的当前快照走一遍与真实请求完全相同的
路由匹配、`when` 条件判定与 Jinja 模板渲染，用于联调前确认"如果我发这个请求会发生什么"。

- 全程在只读事务中执行并最终回滚：**不写 sessions、不写 events**；
- 即使模板调用了 `seq()`，计数也只在预演渲染器内存中自增——随后的真实请求
  仍从**预演前的计数值**开始（可多次预演，结果幂等）；
- seed 会话的随机序列同样只在克隆的状态上消耗：预演结果的 `rng` 字段给出
  本步抽样前后步数，真实序列保持不变；
- 命中时 HTTP 状态码为 `200`，拟返回的状态码在 `response.status_code` 中；
  未命中时直接返回**与真实请求一致的状态码与错误详情**（404 / 405 / 409 /
  500 等），并附带逐条件判定轨迹 `trace`；
- 预演要求会话**已存在**（不会懒创建）；会话不存在返回 404。

请求体字段：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `path` | 是 | 业务路径（不含 query），如 `/orders/42/pay` |
| `method` | 否 | 默认 `GET`，自动转大写；支持 GET/POST/PUT/PATCH/DELETE/HEAD/OPTIONS |
| `query` | 否 | 查询参数对象，值会按字符串处理（与真实 query 一致） |
| `headers` | 否 | 请求头对象，键名自动归一化为小写（与真实请求一致） |
| `body` | 否 | 请求体，按已解析的 JSON 传入（与真实 `application/json` 请求一致），缺省为 `{}` |

```bash
# 先把会话置到 created
curl -s -X POST $BASE/__sessions -H 'Content-Type: application/json' \
  -d '{"token":"t1","state":"created"}'

# 预演一次支付：返回命中规则、拟响应、状态/上下文差异与判定轨迹
curl -s -X POST $BASE/__sessions/t1/preview \
  -H 'Content-Type: application/json' \
  -d '{"method":"POST","path":"/orders/42/pay","body":{"channel":"alipay"}}'
```

命中时响应（节选）：

```json
{
  "preview": {"persisted": false},
  "revision": 1,
  "matched": {"scenario": "order-flow", "revision": 1, "route": "pay-order",
              "rule": "pay-created", "path_params": {"order_id": "42"}},
  "response": {
    "status_code": 200,
    "headers": {},
    "body": {"order_id": "42", "paid": true, "channel": "alipay",
             "paid_at": "2026-09-19T10:00:00Z", "trace": "..."},
    "delay_ms": 0
  },
  "state_diff": {"before": "created", "after": "paid", "changed": true},
  "context_diff": {"added": {"pay_channel": "alipay"}, "changed": {}, "removed": []},
  "seq_preview": [
    {"name": "order", "start": 1000, "value": 1000, "existed": false,
     "before": null,
     "note": "仅预演内自增，不会持久化；随后真实请求仍从预演前的计数开始"}
  ],
  "rng": null,
  "trace": [
    {"stage": "route", "route": "pay-order", "path": "/orders/{order_id}/pay",
     "path_match": true, "method_match": true, "path_params": {"order_id": "42"}},
    {"stage": "route_selected", "route": "pay-order"},
    {"stage": "rule", "rule": "pay-created", "priority": 100, "matched": true,
     "conditions": [
       {"source": "state", "op": "eq", "expected": "created",
        "present": true, "actual": "created", "passed": true},
       {"source": "body", "path": "channel", "op": "in",
        "expected": ["alipay", "wechat", "card"],
        "present": true, "actual": "alipay", "passed": true}
    ]},
    {"stage": "rule", "rule": "double-pay-rejected", "matched": false, "skipped": true}
  ]
}
```

随后发起真实请求，状态照常 `created -> paid`，且 `seq()` 的值与预演时一致
（预演从未消耗计数）。未命中时 `trace` 会逐条给出每个条件的左值来源、取值
（`actual`）、期望值与是否通过，并保留与真实引擎一致的短路顺序；例如在
`paid` 状态预演支付，预演接口信封为 `200` 但 `response.status_code` 为
`409`，错误体与真实请求完全相同。

#### 连续请求链预演（chain dry-run）

在**不改动会话**的前提下，把一组**有序请求**当作一条真实调用链依次试跑：
从该会话的当前快照出发，**后一步一定能看到前一步拟产生的 `state`、
`context`、`seq` 计数与随机序列消耗**，用于一次性确认"我按顺序发这一串
请求会发生什么"（如下单 → 支付 → 发货 → 收货）。

- 请求体只有一个字段 `requests`：元素结构与单次预演**完全相同**（`path`
  必填，`method` 默认 `GET`，`query`/`headers`/`body` 语义一致），**最多
  50 步**，超出返回 422；
- 每一步复用与单次预演/真实请求**同一份**路由匹配、规则判定与 Jinja 渲染
  逻辑；每步返回内容与单次预演一致：`matched` / `request` / `response` /
  `state_diff` / `context_diff` / `seq_preview` / `rng` / `trace`；
- 拟响应为错误状态码但**确实命中了规则**（如 400/409 兜底规则）属于正常
  模拟响应，链路继续；只有**路由失败**（404/405）、**无规则命中**（409
  `no_matching_rule`）或**渲染失败**（500 `render_error`）才会立即停止，
  返回 `failed_index`、失败类型 `failure.kind`（`route`/`rule`/`render`）、
  失败请求与错误体（含 `trace`），以及此前已成功的各步；
- 全程在只读事务中执行并最终回滚：**不写 sessions、不写 events、不推进序列
  计数与随机序列**；`seq()` 只在预演内存中沿链路自增，seed 会话的随机序列
  也只在克隆的状态上消耗（后一步看得到、库中不保留），会话快照不变时
  **多次调用结果可重复**（模板内的 `now()` 是运行时值；未 seed 会话的
  `uuid4()` 等同样是运行时随机值）；
- 同样要求会话**已存在**，不存在返回 404。

```bash
# 新建一个 init 状态的会话
curl -s -X POST $BASE/__sessions -H 'Content-Type: application/json' \
  -d '{"token":"c1","state":"init"}'

# 连续预演：下单 -> 支付 -> 发货 -> 收货
curl -s -X POST $BASE/__sessions/c1/preview/chain \
  -H 'Content-Type: application/json' -d '{
    "requests": [
      {"method":"POST","path":"/orders","body":{"sku":"A-1","quantity":2}},
      {"method":"POST","path":"/orders/1000/pay","body":{"channel":"alipay"}},
      {"method":"POST","path":"/orders/1000/ship","body":{"carrier":"sf"}},
      {"method":"POST","path":"/orders/1000/confirm"}
    ]
  }'
```

响应（节选）：

```json
{
  "preview": {"persisted": false, "chain": true},
  "token": "c1",
  "scenario": "order-flow",
  "revision": 1,
  "steps_completed": 4,
  "steps_total": 4,
  "stopped": false,
  "failed_index": null,
  "failure": null,
  "steps": [
    {"index": 0, "matched": {"scenario": "order-flow", "route": "create-order",
                             "rule": "create-success", "path_params": {}},
     "response": {"status_code": 201, "headers": {}, "body": {"...": "..."},
                  "delay_ms": 0},
     "state_diff": {"before": "init", "after": "created", "changed": true},
     "context_diff": {"added": {"sku": "A-1", "quantity": 2},
                      "changed": {}, "removed": []},
     "seq_preview": [{"name": "order", "start": 1000, "value": 1000,
                      "existed": false, "before": null}],
     "trace": [ {"...": "与单次预演相同的逐条件轨迹..."} ]},
    {"index": 1, "state_diff": {"before": "created", "after": "paid",
                                "changed": true}, "...": "..."},
    {"index": 2, "state_diff": {"before": "paid", "after": "shipped",
                                "changed": true}, "...": "..."},
    {"index": 3, "state_diff": {"before": "shipped", "after": "completed",
                                "changed": true}, "...": "..."}
  ],
  "final_snapshot": {
    "state": "completed",
    "context": {"sku": "A-1", "quantity": 2, "pay_channel": "alipay",
                "carrier": "sf"},
    "seq": {"order": 1001},
    "random": null,
    "note": "链路预演的拟最终快照，不会持久化；随后真实请求仍从预演前的快照开始"
  }
}
```

链中途失败时，HTTP 信封仍为 `200`，但 `stopped` 为 `true`、
`steps_completed` 只含失败前的成功步骤数，例如第 2 步打了一个不存在的路径：

```json
{
  "steps_completed": 1,
  "steps_total": 2,
  "stopped": true,
  "failed_index": 1,
  "failure": {
    "index": 1,
    "kind": "route",
    "status_code": 404,
    "request": {"method": "GET", "path": "/ghost/9", "query": {},
                "headers": {}, "body": {}},
    "error": {"error": "no_route", "path": "/ghost/9", "trace": [ "..."]}
  },
  "steps": [ {"index": 0, "...": "失败前的成功步骤..."} ],
  "final_snapshot": {"state": "created", "context": {"...": "..."}, "seq": {}}
}
```

会话也可以**懒创建**：业务请求只需携带令牌；首个匹配到路由的请求自动建会话。
当已载入多个场景时，懒创建需通过请求头 `sim-scenario: <name>` 指定场景
（或设置环境变量 `SIM_DEFAULT_SCENARIO`）。

### 业务请求约定

- 会话令牌：请求头 `sim-session-token: <token>`（也支持 `?sim_session_token=`）。
- 可选请求头 `sim-scenario: <name>`：仅新会话首次请求时用于绑定场景。
- 请求体：`application/json` 按 JSON 解析；`application/x-www-form-urlencoded`
  按表单解析；YAML 的 content-type 按 YAML 解析；其他文本保存为 `body._raw`。

### 会话事件订阅（SSE）

`GET /__events/stream` 是一条 [Server-Sent Events](https://developer.mozilla.org/docs/Web/API/Server-sent_events)
长连接，按 `events` 表的**全局递增 `id`** 把每次规则命中与每次超时迁移实时推给
所有订阅者：

- **游标**：`Last-Event-ID` 请求头或 `?after_id=N` 查询参数指定“只收 id 严格
  大于 N 的事件”；**两者同传时以请求头为准**；未传游标时以**连接建立时**的
  最大事件 id 为界，即只收连接之后提交的新事件（历史事件不补发）；
- **补发 → 实时无缝衔接**：带游标连接时，服务先按全局 id **分批**（每批 1000 条）
  补发历史事件，追平后转入持续推送；补发与实时共用同一条 `id > cursor` 的严格
  递增推进，且订阅登记先于首次查库——补发末尾与实时唤醒之间提交的事件由首轮
  实时查询兜底，**不漏发、不重复**；
- **按令牌过滤**：`?token=t1` 只订阅该会话；可重复传入同时订阅多个会话
  （`?token=t1&token=t2`）；不传则订阅全部会话；
- **事件内容**：每条消息的 SSE `id` 字段即事件行 id，`data` 为 JSON，含
  `id`、`token`、`scenario`、`revision`、`method`、`route`、`rule`、
  `path`、`from_state`、`to_state`、`status_code`、`created_at`；超时迁移
  事件同样推送（`method=ADVANCE`、`route=@timeout`、`rule=timeout:<from>`、
  `status_code=null`）；
- **心跳与断连**：无事件时空闲约 15 秒发送一条 SSE 注释心跳（`: heartbeat …`，
  EventSource 不向应用暴露，仅用于保活与防代理超时）；检测到客户端断连后
  立即停止读取数据库并注销订阅；
- **非法游标**：`after_id` / `Last-Event-ID` 不是非负整数（负数、小数、
  非数字文本）时返回 `422 invalid_after_id`。

```bash
BASE=http://localhost:8080

# 订阅全部会话的新事件（不带游标：只收连接之后的事件），-N 关闭 curl 缓冲
curl -N "$BASE/__events/stream"

# 只订阅 t1，从最早事件开始补发并保持实时
curl -N "$BASE/__events/stream?token=t1&after_id=0"

# 同时订阅 t1 与 t2，从 id=100 之后续传
curl -N "$BASE/__events/stream?token=t1&token=t2&after_id=100"

# 用请求头指定游标（与 after_id 同传时以请求头为准）
curl -N -H 'Last-Event-ID: 100' "$BASE/__events/stream"

# 非法游标 -> 422
curl -i "$BASE/__events/stream?after_id=abc"
```

典型输出（每条事件两行：`id:` + `data:`；以空行分隔）：

```
: connected after_id=0 scope=全部会话
: 补发历史事件后将持续推送新事件；空闲 15s 发送一次心跳

id: 1
data: {"id":1,"token":"t1","scenario":"order-flow","revision":1,"method":"POST","path":"/orders","route":"create-order","rule":"create-success","from_state":"init","to_state":"created","status_code":201,"created_at":"2026-09-26 12:00:00"}

id: 2
data: {"id":2,"token":"t1","scenario":"order-flow","revision":1,"method":"ADVANCE","path":"/__sessions/t1/advance","route":"@timeout","rule":"timeout:created","from_state":"created","to_state":"cancelled","status_code":null,"created_at":"2026-09-26 12:00:01"}
```

浏览器中使用 `EventSource` 时，断连重连会自动带上最后收到的 `Last-Event-ID`，
无需自行管理游标：

```js
const es = new EventSource("/__events/stream?token=t1&after_id=0");
es.onmessage = (ev) => {
  const event = JSON.parse(ev.data);
  console.log(ev.lastEventId, event.from_state, "->", event.to_state);
};
```

> 注意：SSE 为 GET 请求，不要把它与模拟业务流量的全路径转发混淆——
> `/__events/stream` 是在 catch-all 之前注册的管理路由，不会进入场景匹配。

---

## 并发安全：为什么迁移不会重复触发

所有会读写会话的业务请求都在同一个 `BEGIN IMMEDIATE` 事务中执行：

1. 立即获取 SQLite 写锁（同库同刻只有一个写事务）；
2. 读取会话行（`state`/`context`）；
3. 匹配路由与最高优先级规则；
4. 渲染响应、计算 `set` 与 `next_state`；
5. 更新会话并写入一条 `events` 记录，然后提交。

拿不到锁时按 `SIM_BUSY_TIMEOUT_MS` 等待并在应用层重试（`SIM_DB_RETRIES` 次）。
因此同一会话的并发请求会被**串行化**，每个请求看到的都是前一个请求提交后的新状态：
例如 20 个并发支付请求中恰好 1 个得到 200 并完成 `created -> paid`，
其余 19 个得到 409（已内置压测验证，见下方"压测"说明）。

> 部署限制：SQLite 的写锁是**进程内文件级**的，因此容器固定 `--workers 1`。
> 单 worker + 异步框架足以支撑联调量级的并发；会话状态使用线程局部连接，
> 同步事务段很短，不阻塞事件循环。SSE 事件总线同样是进程内的，单 worker
> 即可保证所有写提交都能实时唤醒同进程内的订阅连接。

---

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SIM_PORT`（仅 compose） | `8080` | 宿主机映射端口，改后访问 `http://localhost:<端口>` |
| `SIM_DB_PATH` | `/data/simulator.db` | SQLite 文件路径（compose 已挂卷持久化） |
| `SIM_SCENARIOS_DIR` | `/app/scenarios` | 启动时自动加载的场景目录（`*.yaml`/`*.yml`，文件名即场景名） |
| `SIM_DEFAULT_SCENARIO` | 空 | 多场景时未显式指定场景的默认场景名 |
| `SIM_BUSY_TIMEOUT_MS` | `5000` | 写锁等待毫秒 |
| `SIM_DB_RETRIES` | `20` | 锁冲突时的应用侧重试次数（每次 20ms 起退避） |
| `SIM_LOG_LEVEL` | `info` | 日志级别 |

自定义场景目录（改 YAML 无需重新构建镜像）：

```bash
# docker-compose.yml 中取消注释挂载行，或：
SIM_SCENARIOS_DIR=/app/scenarios docker compose run --rm \
  -v "$PWD/my-scenarios:/app/scenarios" -p 8080:8080 api-simulator
```

---

## 项目结构

```
.
├── app/
│   ├── main.py            # FastAPI 入口、生命周期初始化、全路径转发
│   ├── config.py          # 环境变量配置
│   ├── db.py              # 连接/WAL/建表/旧库迁移/IMMEDIATE 写事务（含只读回滚事务）与锁重试
│   ├── scenario.py        # YAML 解析与校验（含状态超时 after_ms/next_state/set）
│   ├── models.py          # 场景/路由/规则/状态超时数据模型
│   ├── matching.py        # 路径模式、点路径取值、条件运算符
│   ├── renderer.py        # Jinja2 沙箱渲染、模板函数与会话级确定性随机源（seed）
│   ├── engine.py          # 核心：单事务匹配→渲染→迁移；虚拟时钟 advance 超时连续结算；同快照只读预演（零副作用）；两 revision 纯内存版本回归对比
│   ├── event_bus.py       # 进程内事件总线：写事务提交后唤醒 SSE 订阅者（单 worker 权威）
│   ├── store.py           # 已编译场景缓存（按 name+revision）、发布/激活
│   └── routers/
│       ├── scenarios.py   # 场景校验/版本发布/版本列表/激活/查询/删除/版本回归对比
│       ├── sessions.py    # 会话创建/状态查询/advance 时钟推进/重置/fork/事件/预演
│       └── events.py      # GET /__events/stream：SSE 事件订阅（游标补发+实时推送/token 过滤/心跳/422）
├── scenarios/
│   └── order-flow.yaml    # 示例场景（随启动自动加载）
├── requirements.txt
├── Dockerfile             # 依赖安装 + DB 初始化随启动完成
├── docker-compose.yml
└── README.md
```

数据库表：`scenarios`（场景名 → 当前 revision）、`scenario_revisions`（不可变的
场景定义版本，`(scenario, revision)` 主键）、`sessions`（令牌 → 场景/绑定
revision/状态/上下文/虚拟时钟 `clock_ms`/当前状态超时到期时刻 `timeout_at_ms`；
上下文内含 `_seq` 计数器与 `_rng` 随机序列状态）、
`events`（每次命中的 revision、路由、规则、前后状态与状态码；超时迁移记为
`method=ADVANCE`、`route=@timeout`、`rule=timeout:<from>`、`status_code=NULL`）。
启动时自动检测并就地升级旧版表结构：旧定义固化为 revision 1，既有会话回填绑定
该版本、虚拟时钟归零，历史事件的 revision 置空。随机序列状态存于上下文 JSON
内部，无需表结构变更，既有会话不受影响。

---

## 并发自测（可选）

```bash
BASE=http://localhost:8080
curl -s -X POST $BASE/__sessions -H 'Content-Type: application/json' \
  -d '{"token":"ct","state":"created"}' >/dev/null
seq 1 20 | xargs -P 20 -I{} curl -s -o /dev/null -w '%{http_code}\n' \
  -X POST $BASE/orders/1/pay \
  -H 'sim-session-token: ct' -H 'Content-Type: application/json' \
  -d '{"channel":"card"}' | sort | uniq -c
# 期望: 1 个 200，19 个 409
curl -s "$BASE/__sessions/ct/events"   # 事件中恰好 1 次 created -> paid
```
