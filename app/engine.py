"""核心模拟引擎：在单个 IMMEDIATE 事务中完成匹配、渲染、状态推进。

同一会话快照还支持**预演**（``preview_session`` / ``preview_chain``）：在只读
事务中复用路由匹配、条件判定与模板渲染的同一份代码。单次预演产出拟响应与
state/context 差异；请求链预演（``preview_chain``）从同一快照出发依次执行
有序请求，后一步看到前一步拟产生的 state/context/seq/随机序列变化，任一匹配
或渲染失败即停止。两种预演全程回滚、不写任何表；即使模板调用 ``seq()`` 或
会话级随机函数，计数与随机状态也只在一次性的渲染器实例内推进，随后的真实
请求仍看到预演前的值。

此外提供**版本回归对比**（``compare_revisions``）：两个 scenario revision
各自从其 ``initial_state`` 建立完全独立的纯内存快照（``_CompareSide``，时钟
归零、context 为空、可选同 seed 的确定性随机序列），对同一条最多
``MAX_COMPARE_STEPS`` 步的操作链（业务请求 / 虚拟时钟推进）同步执行并逐步
比较响应、状态、context、clock_ms 与超时迁移。首次差异后两侧仍各自继续；
任一侧单步失败只记录该侧结果、该侧快照保持本步之前的值，不影响另一侧与
后续步骤。整个对比过程不触碰数据库：不写 sessions/events，也不持久化任何
seq 计数或随机状态。
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl

from .db import dumps, read_tx, write_tx
from .event_bus import event_bus
from .matching import evaluate_condition, get_condition_value, match_path
from .models import Route, Rule, Scenario
from .renderer import RenderError, Renderer, SessionRNG
from .store import store


class SimAbort(Exception):
    """以指定 HTTP 响应终止；commit_on_abort=True 时提交事务。"""

    def __init__(self, status_code: int, body: Any, *, commit_on_abort: bool = False,
                 trace: list[dict[str, Any]] | None = None):
        super().__init__(json.dumps(body, ensure_ascii=False))
        self.status_code = status_code
        self.body = body
        self.commit_on_abort = commit_on_abort
        self.trace = trace


@dataclass
class SimResult:
    status_code: int
    headers: dict[str, str]
    body: Any
    delay_ms: int
    is_json: bool
    matched: dict[str, Any]


# ---------------------------------------------------------------------------
# 请求解析
# ---------------------------------------------------------------------------

async def _read_body(request) -> Any:
    raw_bytes = await request.body()
    raw = raw_bytes.decode("utf-8") if raw_bytes else ""
    if not raw:
        return {}
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type or raw.lstrip().startswith(("{", "[")):
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise SimAbort(400, {"error": "invalid_json", "detail": str(exc)}) from exc
    if "application/x-www-form-urlencoded" in content_type:
        return dict(parse_qsl(raw, keep_blank_values=True))
    if "yaml" in content_type:
        import yaml
        try:
            return yaml.safe_load(raw) or {}
        except yaml.YAMLError as exc:
            raise SimAbort(400, {"error": "invalid_yaml", "detail": str(exc)}) from exc
    return {"_raw": raw}


def _select_scenario(explicit: str | None, default_scenario: str | None) -> tuple[Scenario, int]:
    """选出 (当前版本的场景, 当前 revision)，用于新会话绑定。"""
    if explicit:
        found = store.get_current(explicit)
        if found is None:
            raise SimAbort(404, {"error": "scenario_not_found", "scenario": explicit})
        return found
    if default_scenario:
        found = store.get_current(default_scenario)
        if found is None:
            raise SimAbort(400, {"error": "no_scenario_bound",
                                 "detail": "会话未指定场景且默认场景不存在"})
        return found
    names = store.names()
    if len(names) == 1:
        return store.get_current(names[0])  # type: ignore[return-value]
    if not names:
        raise SimAbort(400, {"error": "no_scenario_loaded",
                             "detail": "请先通过 /__scenarios 载入场景，或创建会话时指定 scenario"})
    raise SimAbort(409, {"error": "ambiguous_scenario",
                         "detail": "存在多个场景，请在创建会话时指定 scenario",
                         "scenarios": names})


# ---------------------------------------------------------------------------
# 路由匹配与规则判定（真实请求与预演共用；预演时额外产出判定轨迹）
# ---------------------------------------------------------------------------

def _match_routes(scenario: Scenario, method: str, path: str) -> tuple[
        Route, dict[str, str], list[dict[str, Any]]]:
    """返回 (命中路由, 路径参数, 路由判定轨迹)。"""
    trace: list[dict[str, Any]] = []
    candidates: list[tuple[Route, dict[str, str]]] = []
    for route in scenario.routes:
        params = match_path(route.path_re, path)
        if params is not None:
            candidates.append((route, params))
            trace.append({
                "stage": "route",
                "route": route.id,
                "path": route.path,
                "methods": route.methods,
                "path_match": True,
                "path_params": params,
                "method_match": method in route.methods,
            })

    if not candidates:
        raise SimAbort(404, {"error": "no_route", "path": path}, trace=trace)

    method_matches = [c for c in candidates if method in c[0].methods]
    if not method_matches:
        allowed = sorted({m for r, _ in candidates for m in r.methods})
        raise SimAbort(405, {"error": "method_not_allowed", "allowed": allowed},
                       trace=trace)

    trace.append({"stage": "route_selected", "route": method_matches[0][0].id})
    return method_matches[0][0], method_matches[0][1], trace


def _condition_trace(cond, bag: dict[str, Any]) -> dict[str, Any]:
    present, actual = get_condition_value(cond.source, cond.path, bag)
    try:
        passed = evaluate_condition(cond, actual, present)
        error = None
    except ValueError as exc:  # 未知运算符等，载入时已校验，这里仅兜底
        passed = False
        error = str(exc)
    return {
        "source": cond.source,
        "path": cond.path,
        "op": cond.op,
        "expected": cond.value,
        "present": present,
        "actual": actual,
        "passed": passed,
        **({"error": error} if error else {}),
    }


def evaluate_rules_traced(rule_list: list[Rule], bag: dict[str, Any]) -> tuple[
        Rule | None, list[dict[str, Any]]]:
    """按 priority 降序逐条判定规则，返回 (命中规则或 None, 逐条件轨迹)。

    与真实引擎的短路语义完全一致：某规则首个失败条件之后的条件不再判定。
    """
    trace: list[dict[str, Any]] = []
    matched = False
    for rule in rule_list:  # 已按 priority 降序排序
        entry: dict[str, Any] = {
            "stage": "rule",
            "rule": rule.name,
            "priority": rule.priority,
            "matched": False,
            "conditions": [],
        }
        if matched:
            # 已有更高优先级规则命中：真实引擎同样不会再看后续规则
            entry["skipped"] = True
            trace.append(entry)
            continue

        conditions_ok = True
        for cond in rule.conditions:
            cond_trace = _condition_trace(cond, bag)
            entry["conditions"].append(cond_trace)
            if not cond_trace["passed"]:
                conditions_ok = False
                break  # 与真实引擎一致：首个不满足条件即短路
        entry["matched"] = conditions_ok
        trace.append(entry)
        if conditions_ok:
            matched = True

    hit = next((r for r, e in zip(rule_list, trace)
                if not e.get("skipped") and e["matched"]), None)
    return hit, trace


# ---------------------------------------------------------------------------
# 响应渲染与状态推进规划（不触碰数据库；真实/预演共用）
# ---------------------------------------------------------------------------

def _render_response(rule: Rule, renderer: Renderer) -> tuple[int, dict[str, str], Any, bool]:
    spec = rule.response
    status_value = renderer.render_value(spec.get("status", 200))
    try:
        status = int(str(status_value).strip())
    except (TypeError, ValueError) as exc:
        raise SimAbort(500, {"error": "bad_rendered_status", "value": status_value}) from exc
    if not 100 <= status <= 599:
        raise SimAbort(500, {"error": "bad_rendered_status", "value": status})

    headers = {str(k): str(renderer.render_value(v))
               for k, v in (spec.get("headers") or {}).items()}

    if "body" not in spec:
        return status, headers, None, False

    body = renderer.render_value(spec["body"])
    explicit_json = any(k.lower() == "content-type" and "json" in v.lower()
                        for k, v in headers.items())
    is_json = explicit_json or isinstance(body, (dict, list))
    return status, headers, body, is_json


@dataclass
class _Planned:
    """命中规则后的渲染结果与拟推进状态——尚未持久化。"""
    scenario_name: str
    route: Route
    rule: Rule
    path_params: dict[str, str]
    status_code: int
    headers: dict[str, str]
    body: Any
    is_json: bool
    delay_ms: int
    state: str
    next_state: str
    context: dict[str, Any]
    new_context: dict[str, Any]
    counters: dict[str, int]
    seq_calls: list[dict[str, Any]]
    rng: SessionRNG | None
    rng_draws_before: int | None
    trace: list[dict[str, Any]]


def _plan_matched(scenario: Scenario, scenario_name: str, route: Route,
                  path_params: dict[str, str], rule: Rule, *, state: str,
                  context: dict[str, Any], method: str, path: str,
                  body: Any, query: dict[str, Any], headers: dict[str, Any],
                  trace: list[dict[str, Any]]) -> _Planned:
    render_ctx = dict(context)
    render_ctx.update({
        "state": state,
        "body": body,
        "query": query,
        "header": headers,
        "path": path_params,
        "request": {"method": method, "path": path, "route": route.id},
        "context": dict(context),
    })
    # 预演（preview=True）时记录 seq() 调用；计数器在任何情况下都只存于本实例
    # seed 会话从上下文克隆随机状态：渲染消耗的抽样只推进内存副本，
    # 由调用方决定（真实请求）是否随事务写回上下文
    rng = SessionRNG.from_context(context)
    rng_draws_before = rng.draws if rng is not None else None
    renderer = Renderer(render_ctx, trace_calls=True, rng=rng)

    try:
        status, resp_headers, resp_body, is_json = _render_response(rule, renderer)
        updates = renderer.render_value(rule.set) if rule.set else {}
        next_state = state
        if rule.next_state is not None:
            next_state = renderer.render_template(rule.next_state).strip()
            if next_state not in scenario.states:
                raise SimAbort(500, {"error": "render_error",
                                     "detail": f"规则 {rule.name} 渲染后的状态 {next_state!r} 未声明"})
    except RenderError as exc:
        raise SimAbort(500, {"error": "render_error",
                             "rule": rule.name, "detail": str(exc)},
                        trace=trace) from exc

    if not isinstance(updates, dict):
        raise SimAbort(500, {"error": "render_error",
                             "detail": f"规则 {rule.name} 的 set 渲染结果必须是对象"},
                       trace=trace)

    new_context = dict(context)
    new_context.update(updates)
    new_context["_seq"] = renderer.counters
    if rng is not None:
        # 随机状态随上下文一并推进；渲染失败时本函数已抛出，不会到达这里
        new_context[SessionRNG.STATE_KEY] = rng.snapshot()

    return _Planned(
        scenario_name=scenario_name,
        route=route,
        rule=rule,
        path_params=path_params,
        status_code=status,
        headers=resp_headers,
        body=resp_body,
        is_json=is_json,
        delay_ms=rule.delay_ms,
        state=state,
        next_state=next_state,
        context=context,
        new_context=new_context,
        counters=dict(renderer.counters),
        seq_calls=list(renderer.seq_calls),
        rng=rng,
        rng_draws_before=rng_draws_before,
        trace=trace,
    )


def _resolve_snapshot(conn, token: str, explicit_scenario: str | None,
                      default_scenario: str | None) -> tuple[
                          Scenario, str, int, str, dict[str, Any], int, int | None, bool]:
    """解析会话快照；不存在时按真实请求的规则懒创建（仅真实写路径会落库）。

    既有会话始终返回其创建时绑定的 revision；场景之后再次发布或激活
    其他版本都不影响该会话。新会话则绑定此刻的当前 revision。
    返回值额外携带虚拟时钟 clock_ms 与当前状态的超时到期时刻 timeout_at_ms。
    """
    row = conn.execute(
        "SELECT scenario, revision, state, context, clock_ms, timeout_at_ms"
        " FROM sessions WHERE token = ?",
        (token,),
    ).fetchone()

    if row is not None:
        scenario_name = row["scenario"]
        revision = row["revision"]
        if revision is None:  # 兜底：异常旧数据缺 revision 时按当前版本处理
            revision = store.current_revision(scenario_name)
        scenario = store.get(scenario_name, revision)
        if scenario is None:
            raise SimAbort(410, {"error": "scenario_unloaded", "scenario": scenario_name})
        return (scenario, scenario_name, revision, row["state"],
                json.loads(row["context"]), int(row["clock_ms"] or 0),
                row["timeout_at_ms"], False)

    scenario, revision = _select_scenario(explicit_scenario, default_scenario)
    return scenario, scenario.name, revision, scenario.initial_state, {}, 0, None, True


def _timeout_due_at(scenario: Scenario, state: str, entered_at_ms: int) -> int | None:
    """会话在 entered_at_ms 进入 state 时的超时到期时刻；该状态无超时则 None。"""
    timeout = scenario.timeouts.get(state)
    return None if timeout is None else entered_at_ms + timeout.after_ms


# ---------------------------------------------------------------------------
# 真实请求主入口
# ---------------------------------------------------------------------------

async def simulate(request, default_scenario: str | None = None) -> SimResult:
    token = (request.headers.get("sim-session-token")
             or request.query_params.get("sim_session_token"))
    if not token:
        raise SimAbort(400, {"error": "missing_session_token",
                             "detail": "请通过 sim-session-token 请求头（或 sim_session_token 查询参数）提供会话令牌"})

    method = request.method
    path = request.url.path
    query = {k: v for k, v in request.query_params.multi_items()
             if k != "sim_session_token"}
    headers = {k.lower(): v for k, v in request.headers.items()}
    body = await _read_body(request)
    # 新会话首次请求时可用 sim-scenario 头指定场景；已有会话忽略
    explicit_scenario = request.headers.get("sim-scenario")

    with write_tx() as conn:
        result = _run_locked(conn, token, method, path, query, headers, body,
                             default_scenario, explicit_scenario)
    # 事务已提交：唤醒事件流订阅者，让它们立刻读到新事件（回滚路径不发）
    event_bus.publish()
    return result


def _run_locked(conn, token, method, path, query, headers, body,
                default_scenario, explicit_scenario) -> SimResult:
    # 1) 查找会话与场景（不存在则按懒创建规则选场景，暂不落库）
    (scenario, scenario_name, revision, state, context, clock_ms, timeout_at_ms,
     is_new_session) = _resolve_snapshot(
        conn, token, explicit_scenario, default_scenario)

    # 2) 匹配路由（不存在则回滚，不留下空会话）
    route, path_params, _trace = _match_routes(scenario, method, path)

    # 3) 新会话落库：绑定此刻的当前 revision，之后不再随发布变化；
    #    虚拟时钟归零，超时到期时刻按初始状态调度
    if is_new_session:
        try:
            conn.execute(
                "INSERT INTO sessions(token, scenario, revision, state, context,"
                " clock_ms, timeout_at_ms) VALUES (?, ?, ?, ?, ?, 0, ?)",
                (token, scenario_name, revision, state, dumps(context),
                 _timeout_due_at(scenario, state, 0)),
            )
        except sqlite3.IntegrityError:
            # 并发首请求：另一事务已插入，转为锁冲突重试
            raise sqlite3.OperationalError("database is locked") from None

    # 4) 选择最高优先级匹配规则
    bag = {"state": state, "body": body, "query": query,
           "header": headers, "path": path_params}
    rule, rule_trace = evaluate_rules_traced(route.rules, bag)
    trace = _trace + rule_trace

    if rule is None:
        raise SimAbort(409, {
            "error": "no_matching_rule",
            "scenario": scenario_name,
            "revision": revision,
            "route": route.id,
            "state": state,
        }, commit_on_abort=True, trace=trace)

    # 5) 渲染响应 + 推进状态（同一事务内）
    planned = _plan_matched(scenario, scenario_name, route, path_params, rule,
                            state=state, context=context, method=method, path=path,
                            body=body, query=query, headers=headers, trace=trace)

    # 请求迁移离开原状态时其旧超时立即失效；进入新状态的时刻为当前虚拟
    # 时钟，按新状态的 after_ms 重新调度；状态不变时保留原到期时刻。
    # 新会话的首次请求视为“进入初始状态”（此前没有调度基准），同样重算。
    if is_new_session or planned.next_state != state:
        timeout_at_ms = _timeout_due_at(scenario, planned.next_state, clock_ms)

    conn.execute(
        "UPDATE sessions SET state = ?, context = ?, clock_ms = ?, timeout_at_ms = ?,"
        " updated_at = datetime('now') WHERE token = ?",
        (planned.next_state, dumps(planned.new_context), clock_ms, timeout_at_ms, token),
    )
    conn.execute(
        """
        INSERT INTO events(token, scenario, revision, method, path, route, rule,
                           from_state, to_state, status_code)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (token, scenario_name, revision, method, path, route.id, rule.name,
         state, planned.next_state, planned.status_code),
    )

    return SimResult(
        status_code=planned.status_code,
        headers=planned.headers,
        body=planned.body,
        delay_ms=planned.delay_ms,
        is_json=planned.is_json,
        matched={
            "scenario": scenario_name,
            "revision": revision,
            "route": route.id,
            "rule": rule.name,
            "from_state": state,
            "to_state": planned.next_state,
        },
    )


# ---------------------------------------------------------------------------
# 虚拟时钟推进：POST /__sessions/{token}/advance
# 在单个 IMMEDIATE 写事务内按到期时刻连续执行状态超时迁移
# ---------------------------------------------------------------------------

MAX_TIMEOUT_STEPS = 10_000


def _render_timeout_set(timeout, *, state: str,
                        context: dict[str, Any], at_ms: int,
                        request_path: str = "/__sessions/{token}/advance",
                        method: str = "ADVANCE") -> dict[str, Any]:
    """渲染超时迁移的 set 模板（无业务请求：body/query/header/path 均为空）。

    与真实请求同一套渲染器：``seq()`` 计数与 seed 随机序列随会话上下文推进。
    """
    render_ctx = dict(context)
    render_ctx.update({
        "state": state,
        "body": {},
        "query": {},
        "header": {},
        "path": {},
        "request": {"method": method, "path": request_path, "route": "@timeout"},
        "context": dict(context),
    })
    rng = SessionRNG.from_context(context)
    renderer = Renderer(render_ctx, trace_calls=False, rng=rng)
    try:
        updates = renderer.render_value(timeout.set) if timeout.set else {}
    except RenderError as exc:
        raise SimAbort(500, {"error": "render_error",
                             "detail": f"状态 {state} 超时 set 渲染失败: {exc}",
                             "at_ms": at_ms}) from exc
    if not isinstance(updates, dict):
        raise SimAbort(500, {"error": "render_error",
                             "detail": f"状态 {state} 超时的 set 渲染结果必须是对象",
                             "at_ms": at_ms})

    new_context = dict(context)
    new_context.update(updates)
    new_context["_seq"] = renderer.counters
    if rng is not None:
        new_context[SessionRNG.STATE_KEY] = rng.snapshot()
    return new_context


def advance_session(token: str, delta_ms: int) -> dict[str, Any]:
    """把会话虚拟时钟推进 delta_ms，并结算窗口内全部到期超时。

    在单个写事务中：读取会话快照 -> 每当 ``timeout_at_ms <= clock+delta``
    即执行一次超时迁移（渲染可选 set、推进 seq/随机序列、写事件、按新状态
    after_ms 重新调度）-> 时钟落到目标时刻并写回最终快照。零延迟超时环
    （同一到期时刻重复进入同一状态）与渲染失败都会让整个事务原子回滚。
    """
    with write_tx() as conn:
        row = conn.execute(
            "SELECT scenario, revision, state, context, clock_ms, timeout_at_ms"
            " FROM sessions WHERE token = ?",
            (token,),
        ).fetchone()
        if row is None:
            raise SimAbort(404, {"error": "session_not_found", "token": token})

        scenario_name = row["scenario"]
        revision = row["revision"]
        if revision is None:
            revision = store.current_revision(scenario_name)
        scenario = store.get(scenario_name, revision)
        if scenario is None:
            raise SimAbort(410, {"error": "scenario_unloaded", "scenario": scenario_name})

        state = row["state"]
        context = json.loads(row["context"])
        clock_before = int(row["clock_ms"] or 0)
        timeout_at_ms: int | None = row["timeout_at_ms"]
        target_clock = clock_before + delta_ms

        # entered[state] = 本次 advance 内“进入该状态”的虚拟时刻；
        # 同一状态在同一时刻被再次进入即构成零延迟超时环。初始状态也登记：
        # 其进入时刻可由 (到期时刻 - after_ms) 反推，覆盖从首跳就成环的场景。
        entered: dict[str, int] = {}
        initial_timeout = scenario.timeouts.get(state)
        if timeout_at_ms is not None and initial_timeout is not None:
            entered[state] = timeout_at_ms - initial_timeout.after_ms

        transitions: list[dict[str, Any]] = []
        while timeout_at_ms is not None and timeout_at_ms <= target_clock:
            timeout = scenario.timeouts.get(state)
            if timeout is None:
                # 兜底：状态无超时定义却存在到期时刻（数据不一致），当作超时失效
                timeout_at_ms = None
                break
            if len(transitions) >= MAX_TIMEOUT_STEPS:
                raise SimAbort(422, {
                    "error": "timeout_chain_too_long",
                    "detail": f"一次 advance 内超时迁移超过 {MAX_TIMEOUT_STEPS} 步"
                              "（可能存在正延迟环），已原子回滚",
                    "limit": MAX_TIMEOUT_STEPS,
                    "at_ms": timeout_at_ms,
                    "attempted_transitions": transitions,
                    "rolled_back": True,
                })

            at_ms = timeout_at_ms
            next_state = timeout.next_state

            if entered.get(next_state) == at_ms:
                # 零延迟环：next_state 已在同一虚拟时刻进入过 -> 无限迁移，回滚
                raise SimAbort(409, {
                    "error": "timeout_cycle",
                    "detail": f"检测到零延迟超时环：状态 {next_state} 在同一虚拟时刻 "
                              f"{at_ms}ms 被重复进入，本次 advance 已原子回滚",
                    "at_ms": at_ms,
                    "cycle": {"from": state, "to": next_state},
                    "attempted_transitions": transitions,
                    "rolled_back": True,
                })

            from_state = state
            new_context = _render_timeout_set(
                timeout, state=state, context=context,
                at_ms=at_ms, request_path=f"/__sessions/{token}/advance")
            applied = {k: v for k, v in new_context.items()
                       if k not in context or context[k] != v}
            applied.pop("_seq", None)
            applied.pop(SessionRNG.STATE_KEY, None)

            # 事件先落库（环/渲染异常时随事务一并回滚，不会残留）
            conn.execute(
                """
                INSERT INTO events(token, scenario, revision, method, path, route, rule,
                                   from_state, to_state, status_code)
                VALUES (?, ?, ?, 'ADVANCE', ?, '@timeout', ?, ?, ?, NULL)
                """,
                (token, scenario_name, revision,
                 f"/__sessions/{token}/advance",
                 f"timeout:{from_state}", from_state, next_state),
            )

            transitions.append({
                "index": len(transitions),
                "from": from_state,
                "to": next_state,
                "at": at_ms,
                "after_ms": timeout.after_ms,
                "set": applied,
            })

            # 同一 advance 内新状态产生的超时继续在本循环结算
            entered[next_state] = at_ms
            state = next_state
            context = new_context
            timeout_at_ms = _timeout_due_at(scenario, state, at_ms)

        # 全部到期迁移结算完毕：时钟落到目标时刻，最终状态的到期时刻保留
        conn.execute(
            "UPDATE sessions SET state = ?, context = ?, clock_ms = ?, timeout_at_ms = ?,"
            " updated_at = datetime('now') WHERE token = ?",
            (state, dumps(context), target_clock, timeout_at_ms, token),
        )

        pending_timeout = None
        if timeout_at_ms is not None:
            pending = scenario.timeouts.get(state)
            pending_timeout = {
                "state": state,
                "after_ms": pending.after_ms if pending else None,
                "at_ms": timeout_at_ms,
                "remaining_ms": max(0, timeout_at_ms - target_clock),
            }

        result = {
            "advanced": True,
            "token": token,
            "scenario": scenario_name,
            "revision": revision,
            "clock_ms": {"before": clock_before, "after": target_clock},
            "delta_ms": delta_ms,
            "transitions": transitions,
            "final_snapshot": {
                "state": state,
                "context": {k: v for k, v in context.items()
                            if k not in ("_seq", SessionRNG.STATE_KEY)},
                "seq": dict(context.get("_seq", {}) or {}),
                "random": SessionRNG.info(context),
                "clock_ms": target_clock,
                "timeout_at_ms": timeout_at_ms,
                "pending_timeout": pending_timeout,
            },
        }
    # 事务提交（含每步超时迁移事件）后唤醒事件流订阅者；回滚路径不发
    event_bus.publish()
    return result


# ---------------------------------------------------------------------------
# 请求预演：只读快照、同一份匹配/渲染逻辑、零持久化副作用
# ---------------------------------------------------------------------------

MAX_PREVIEW_STEPS = 50


def _build_diff(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """以 after 相对 before 的视角生成 added/changed/removed 差异（跳过 _seq/_rng）。"""
    added: dict[str, Any] = {}
    changed: dict[str, Any] = {}
    removed: list[str] = []
    keys = set(before) | set(after)
    for key in keys:
        if key in ("_seq", SessionRNG.STATE_KEY):
            continue
        if key not in before:
            added[key] = after[key]
        elif key not in after:
            removed.append(key)
        elif before[key] != after[key]:
            changed[key] = {"before": before[key], "after": after[key]}
    return {
        "added": added,
        "changed": changed,
        "removed": sorted(removed),
    }


def _build_seq_preview(context: dict[str, Any], counters: dict[str, int],
                       seq_calls: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """以 context（步骤前快照）为基准，给本次渲染内的每次 seq() 调用标注 before。"""
    prior = context.get("_seq", {}) or {}
    return [
        {
            **call,
            "before": prior.get(call["name"]),
            "note": "仅预演内自增，不会持久化；随后真实请求仍从预演前的计数开始",
        }
        for call in seq_calls
    ]


def _build_rng_preview(planned: _Planned) -> dict[str, Any] | None:
    """单次渲染的随机序列消耗说明；未启用 seed 的会话返回 None。"""
    if planned.rng is None:
        return None
    return {
        "seed": planned.rng.seed,
        "draws_before": planned.rng_draws_before,
        "draws_after": planned.rng.draws,
        "note": "仅预演内消耗，不会持久化；随后真实请求仍从预演前的随机状态继续",
    }


def _simulate_step(scenario: Scenario, scenario_name: str, revision: int, *, state: str,
                   context: dict[str, Any], method: str, path: str,
                   query: dict[str, Any], headers: dict[str, Any],
                   body: Any) -> tuple[dict[str, Any], str, dict[str, Any]]:
    """在内存快照上执行一次匹配/渲染（真实与预演共用的同一套语义）。

    纯函数：不触碰数据库。返回 (本步结构化结果, 拟产生的下一状态,
    拟产生的下一上下文（含 _seq 计数器）)。路由不匹配、无命中规则或
    渲染失败时抛出 :class:`SimAbort`（与真实引擎的状态码/错误体一致）。
    """
    route, path_params, trace = _match_routes(scenario, method, path)

    bag = {"state": state, "body": body, "query": query,
           "header": headers, "path": path_params}
    rule, rule_trace = evaluate_rules_traced(route.rules, bag)
    trace += rule_trace

    if rule is None:
        raise SimAbort(409, {
            "error": "no_matching_rule",
            "scenario": scenario_name,
            "revision": revision,
            "route": route.id,
            "state": state,
            "trace": trace,
        }, trace=trace)

    planned = _plan_matched(scenario, scenario_name, route, path_params, rule,
                            state=state, context=context, method=method, path=path,
                            body=body, query=query, headers=headers, trace=trace)

    step = {
        "matched": {
            "scenario": planned.scenario_name,
            "revision": revision,
            "route": planned.route.id,
            "rule": planned.rule.name,
            "path_params": planned.path_params,
        },
        "request": {
            "method": method,
            "path": path,
            "query": query,
            "headers": headers,
            "body": body,
        },
        "response": {
            "status_code": planned.status_code,
            "headers": planned.headers,
            "body": planned.body,
            "delay_ms": planned.delay_ms,
        },
        "state_diff": {"before": planned.state, "after": planned.next_state,
                       "changed": planned.state != planned.next_state},
        "context_diff": _build_diff(planned.context, planned.new_context),
        "seq_preview": _build_seq_preview(planned.context, planned.counters,
                                          planned.seq_calls),
        "rng": _build_rng_preview(planned),
        "trace": planned.trace,
    }
    return step, planned.next_state, planned.new_context


def _failure_kind(status_code: int) -> str:
    """把模拟失败归类到 README 中承诺的 route/rule/render 三类。"""
    if status_code in (404, 405):
        return "route"
    if status_code == 409:
        return "rule"
    return "render"


def preview_session(token: str, method: str, path: str, query: dict[str, Any],
                    headers: dict[str, Any], body: Any,
                    default_scenario: str | None) -> tuple[int, dict[str, Any]]:
    """预演一次业务请求，返回 (HTTP 状态码, 结构化结果)。

    预演要求会话已存在（不懒创建），全程只读；返回的状态码/错误体与真实
    请求一致，命中时另外给出拟响应与 state/context 差异。
    """
    with read_tx() as conn:
        row = conn.execute(
            "SELECT scenario, revision, state, context FROM sessions WHERE token = ?",
            (token,),
        ).fetchone()
        if row is None:
            # 与真实请求不同：预演不创建会话，直接按管理接口语义报 404
            return 404, {"error": "session_not_found", "token": token,
                         "detail": "预演要求会话已存在；请先创建会话或发起一次真实业务请求",
                         "preview": {"persisted": False}}

        scenario_name = row["scenario"]
        revision = row["revision"]
        if revision is None:
            revision = store.current_revision(scenario_name)
        scenario = store.get(scenario_name, revision)
        if scenario is None:
            raise SimAbort(410, {"error": "scenario_unloaded", "scenario": scenario_name})
        state = row["state"]
        context = json.loads(row["context"])

        step, _next_state, _next_context = _simulate_step(
            scenario, scenario_name, revision, state=state, context=context,
            method=method, path=path, query=query, headers=headers, body=body)

    # 事务已回滚：以下全部是基于快照的拟结果
    payload = {"preview": {"persisted": False}, "token": token,
               "scenario": scenario_name, "revision": revision, **step}
    return 200, payload


# ---------------------------------------------------------------------------
# 连续请求链预演：基于同一会话快照依次执行，后一步看到前一步的拟变化
# ---------------------------------------------------------------------------

def preview_chain(token: str, requests: list[dict[str, Any]]) -> tuple[int, dict[str, Any]]:
    """连续预演一条有序请求链，返回 (HTTP 状态码, 结构化结果)。

    从会话当前快照出发在纯内存中逐步行进：每一步都能看到此前各步拟产生的
    state、context、seq 计数与随机序列消耗。任一路由/规则/渲染失败立即停止。
    全程只读、不落库；不改变会话快照时，重复调用结果可重复。
    """
    with read_tx() as conn:
        row = conn.execute(
            "SELECT scenario, revision, state, context FROM sessions WHERE token = ?",
            (token,),
        ).fetchone()
        if row is None:
            return 404, {
                "error": "session_not_found", "token": token,
                "detail": "链路预演要求会话已存在；请先创建会话或发起一次真实业务请求",
                "preview": {"persisted": False, "chain": True},
            }

        scenario_name = row["scenario"]
        revision = row["revision"]
        if revision is None:
            revision = store.current_revision(scenario_name)
        scenario = store.get(scenario_name, revision)
        if scenario is None:
            raise SimAbort(410, {"error": "scenario_unloaded", "scenario": scenario_name})
        initial_state = row["state"]
        initial_context = json.loads(row["context"])

        state = initial_state
        context = initial_context
        steps: list[dict[str, Any]] = []
        failed_index: int | None = None
        failure: dict[str, Any] | None = None

        for index, req in enumerate(requests):
            try:
                step, next_state, next_context = _simulate_step(
                    scenario, scenario_name, revision, state=state, context=context,
                    method=req["method"], path=req["path"],
                    query=req["query"], headers=req["headers"], body=req["body"])
            except SimAbort as exc:
                # 路由、规则或渲染失败：立即停止，仅返回此前成功步骤
                failed_index = index
                error_body = dict(exc.body)
                if exc.trace is not None:
                    error_body["trace"] = exc.trace
                failure = {
                    "index": index,
                    "kind": _failure_kind(exc.status_code),
                    "status_code": exc.status_code,
                    "request": {
                        "method": req["method"], "path": req["path"],
                        "query": req["query"], "headers": req["headers"],
                        "body": req["body"],
                    },
                    "error": error_body,
                }
                break

            steps.append({"index": index, **step})
            # 后一步基于前一步拟产生的快照继续
            state = next_state
            context = next_context

        seq_counters = dict(context.get("_seq", {}) or {})

        final_snapshot = {
            "state": state,
            "context": {k: v for k, v in context.items()
                        if k not in ("_seq", SessionRNG.STATE_KEY)},
            "seq": seq_counters,
            "random": SessionRNG.info(context),
            "note": "链路预演的拟最终快照，不会持久化；随后真实请求仍从预演前的快照开始",
        }

        payload: dict[str, Any] = {
            "preview": {"persisted": False, "chain": True},
            "token": token,
            "scenario": scenario_name,
            "revision": revision,
            "steps_completed": len(steps),
            "steps_total": len(requests),
            "stopped": failure is not None,
            "failed_index": failed_index,
            "failure": failure,
            "steps": steps,
            "final_snapshot": final_snapshot,
        }
        return 200, payload


# ---------------------------------------------------------------------------
# 版本回归对比：两个 revision 各自从 initial_state 建独立内存快照，同 seed
# 执行同一条操作链（业务请求 / 虚拟时钟推进），逐步比较响应、状态、context、
# clock_ms 与超时迁移。全程纯内存：不写 sessions/events，也不写任何计数；
# 一侧步骤失败只记录该侧结果，另一侧继续各自的链路。
# ---------------------------------------------------------------------------

MAX_COMPARE_STEPS = 20


class CompareAbort(SimAbort):
    """对比接口的请求级错误（revision 不存在/参数非法），由路由转成 HTTP 响应。"""


def _public_context(context: dict[str, Any]) -> dict[str, Any]:
    """剥离 _seq/_rng 内部键的业务上下文。"""
    return {k: v for k, v in context.items()
            if k not in ("_seq", SessionRNG.STATE_KEY)}


def _pending_timeout(scenario: Scenario, state: str,
                     timeout_at_ms: int | None, clock_ms: int) -> dict[str, Any] | None:
    if timeout_at_ms is None:
        return None
    timeout = scenario.timeouts.get(state)
    return {
        "state": state,
        "after_ms": timeout.after_ms if timeout else None,
        "at_ms": timeout_at_ms,
        "remaining_ms": max(0, timeout_at_ms - clock_ms),
    }


class _CompareSide:
    """一个 revision 的纯内存执行侧：快照只存在于本实例，绝不触碰数据库。"""

    label: str

    def __init__(self, label: str, scenario: Scenario, revision: int, seed: Any):
        self.label = label
        self.scenario = scenario
        self.scenario_name = scenario.name
        self.revision = revision
        self.state = scenario.initial_state
        self.context: dict[str, Any] = {"_seq": {}}
        if seed is not None:
            self.context[SessionRNG.STATE_KEY] = SessionRNG.fresh(seed).snapshot()
        self.clock_ms = 0
        # 进入初始状态（虚拟时刻 0）即按该状态的 after_ms 调度
        self.timeout_at_ms = _timeout_due_at(scenario, self.state, 0)

    def snapshot(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "context": _public_context(self.context),
            "seq": dict(self.context.get("_seq", {}) or {}),
            "random": SessionRNG.info(self.context),
            "clock_ms": self.clock_ms,
            "timeout_at_ms": self.timeout_at_ms,
            "pending_timeout": _pending_timeout(
                self.scenario, self.state, self.timeout_at_ms, self.clock_ms),
        }

    # -- 步骤执行：失败时快照保持不变（与真实/预演的“失败不消耗”语义一致）----

    def request_step(self, req: dict[str, Any]) -> dict[str, Any]:
        clock_before = self.clock_ms
        timeout_before = self.timeout_at_ms
        try:
            step, next_state, next_context = _simulate_step(
                self.scenario, self.scenario_name, self.revision,
                state=self.state, context=self.context,
                method=req["method"], path=req["path"],
                query=req["query"], headers=req["headers"], body=req["body"])
        except SimAbort as exc:
            return self._failure(exc, "request", req, clock_before, timeout_before)

        # 与真实引擎一致：迁移离开原状态（或初始状态首请求）时按当前虚拟
        # 时钟重新调度新状态超时；状态不变则保留原到期时刻
        timeout_after = timeout_before
        if next_state != self.state:
            timeout_after = _timeout_due_at(self.scenario, next_state, self.clock_ms)
        before_state, before_context = self.state, self.context
        self.state = next_state
        self.context = next_context
        self.timeout_at_ms = timeout_after
        return self._ok("request", step, before_state, before_context,
                        clock_before, timeout_before, transitions=[])

    def advance_step(self, delta_ms: int) -> dict[str, Any]:
        clock_before = self.clock_ms
        timeout_before = self.timeout_at_ms
        state = self.state
        context = self.context
        timeout_at_ms = self.timeout_at_ms
        target_clock = clock_before + delta_ms

        entered: dict[str, int] = {}
        initial_timeout = self.scenario.timeouts.get(state)
        if timeout_at_ms is not None and initial_timeout is not None:
            entered[state] = timeout_at_ms - initial_timeout.after_ms

        transitions: list[dict[str, Any]] = []
        try:
            while timeout_at_ms is not None and timeout_at_ms <= target_clock:
                timeout = self.scenario.timeouts.get(state)
                if timeout is None:
                    timeout_at_ms = None
                    break
                if len(transitions) >= MAX_TIMEOUT_STEPS:
                    raise SimAbort(422, {
                        "error": "timeout_chain_too_long",
                        "detail": f"一次 advance 内超时迁移超过 {MAX_TIMEOUT_STEPS} 步"
                                  "（可能存在正延迟环），本步已回滚",
                        "limit": MAX_TIMEOUT_STEPS,
                        "at_ms": timeout_at_ms,
                        "attempted_transitions": transitions,
                        "rolled_back": True,
                    })

                at_ms = timeout_at_ms
                next_state = timeout.next_state
                if entered.get(next_state) == at_ms:
                    raise SimAbort(409, {
                        "error": "timeout_cycle",
                        "detail": f"检测到零延迟超时环：状态 {next_state} 在同一虚拟时刻 "
                                  f"{at_ms}ms 被重复进入，本步已回滚（对比侧快照不变）",
                        "at_ms": at_ms,
                        "cycle": {"from": state, "to": next_state},
                        "attempted_transitions": transitions,
                        "rolled_back": True,
                    })

                from_state = state
                new_context = _render_timeout_set(
                    timeout, state=state, context=context, at_ms=at_ms,
                    request_path="/__scenarios/compare/advance", method="ADVANCE")
                applied = {k: v for k, v in new_context.items()
                           if k not in context or context[k] != v}
                applied.pop("_seq", None)
                applied.pop(SessionRNG.STATE_KEY, None)
                transitions.append({
                    "index": len(transitions),
                    "from": from_state,
                    "to": next_state,
                    "at": at_ms,
                    "after_ms": timeout.after_ms,
                    "set": applied,
                })
                entered[next_state] = at_ms
                state = next_state
                context = new_context
                timeout_at_ms = _timeout_due_at(self.scenario, state, at_ms)
        except SimAbort as exc:
            # 与真实 advance 一致：零延迟环/链过长/渲染失败 -> 本步回滚，
            # 该侧状态、时钟、context、seq、随机序列都保持本步之前的值
            return self._failure(exc, "advance", {"delta_ms": delta_ms},
                                 clock_before, timeout_before)

        before_state, before_context = self.state, self.context
        self.state = state
        self.context = context
        self.clock_ms = target_clock
        self.timeout_at_ms = timeout_at_ms

        step = {
            "delta_ms": delta_ms,
            "transitions": transitions,
        }
        return self._ok("advance", step, before_state, before_context,
                        clock_before, timeout_before, transitions=transitions)

    # -- 结果封装 ---------------------------------------------------------

    def _ok(self, kind: str, detail: dict[str, Any], before_state: str,
            before_context: dict[str, Any], clock_before: int,
            timeout_before: int | None, *,
            transitions: list[dict[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {
            "ok": True,
            "kind": kind,
            "failed": False,
            "state": self.state,
            "state_diff": {"before": before_state, "after": self.state,
                           "changed": before_state != self.state},
            "context": _public_context(self.context),
            "context_diff": _build_diff(before_context, self.context),
            "seq": dict(self.context.get("_seq", {}) or {}),
            "random": SessionRNG.info(self.context),
            "clock_ms": {"before": clock_before, "after": self.clock_ms},
            "timeout_at_ms": {"before": timeout_before, "after": self.timeout_at_ms},
            "pending_timeout": _pending_timeout(
                self.scenario, self.state, self.timeout_at_ms, self.clock_ms),
            "timeout_transitions": transitions,
        }
        if kind == "request":
            result["request"] = detail["request"]
            result["matched"] = detail["matched"]
            result["response"] = detail["response"]
            result["trace"] = detail.get("trace")
        else:
            result["delta_ms"] = detail["delta_ms"]
        return result

    def _failure(self, exc: SimAbort, kind: str, request: dict[str, Any],
                 clock_before: int, timeout_before: int | None) -> dict[str, Any]:
        """本步失败：仅记录该侧结果，快照不变（对侧不受影响、本侧后续继续）。"""
        error_body = dict(exc.body)
        trace = error_body.pop("trace", None) or exc.trace
        result: dict[str, Any] = {
            "ok": False,
            "kind": kind,
            "failed": True,
            "failure": {
                "kind": _compare_failure_kind(kind, exc.status_code),
                "status_code": exc.status_code,
                "error": error_body,
                **({"trace": trace} if trace is not None else {}),
            },
            # 失败不推进任何东西：快照字段保持本步之前的值，便于展示两侧分歧
            "state": self.state,
            "state_diff": {"before": self.state, "after": self.state,
                           "changed": False},
            "context": _public_context(self.context),
            "context_diff": {"added": {}, "changed": {}, "removed": []},
            "seq": dict(self.context.get("_seq", {}) or {}),
            "random": SessionRNG.info(self.context),
            "clock_ms": {"before": clock_before, "after": clock_before},
            "timeout_at_ms": {"before": timeout_before, "after": timeout_before},
            "pending_timeout": _pending_timeout(
                self.scenario, self.state, timeout_before, clock_before),
            "timeout_transitions": [],
        }
        if kind == "request":
            result["request"] = request
        else:
            result["delta_ms"] = request["delta_ms"]
        return result


def _compare_failure_kind(kind: str, status_code: int) -> str:
    """对比链上单步失败的归类：request 为 route/rule/render，advance 为 timeout。"""
    if kind == "advance":
        return "timeout"
    return _failure_kind(status_code)


def _canonical(side_result: dict[str, Any]) -> dict[str, Any]:
    """提取逐步比较的字段：响应、状态、context、clock_ms、超时迁移。

    只含与版本回归相关的可比值；trace/seq/rng 等过程信息留在侧结果原文中。
    """
    canonical: dict[str, Any] = {
        "ok": side_result["ok"],
        "kind": side_result["kind"],
        "response": None,
        "state": side_result["state"],
        "context": side_result["context"],
        "clock_ms": side_result["clock_ms"]["after"],
        "timeout_at_ms": side_result["timeout_at_ms"]["after"],
        "pending_timeout": side_result["pending_timeout"],
        "timeout_transitions": side_result["timeout_transitions"],
        "failure": None,
    }
    if side_result["ok"]:
        if side_result["kind"] == "request":
            canonical["response"] = side_result["response"]
    else:
        canonical["failure"] = side_result["failure"]
    return canonical


def _deep_diff(a: Any, b: Any, prefix: str = "") -> list[dict[str, Any]]:
    """递归比较两个 JSON 值，返回逐字段差异（点路径；列表按下标对齐比较）。"""
    diffs: list[dict[str, Any]] = []
    if isinstance(a, dict) and isinstance(b, dict):
        for key in sorted(set(a) | set(b)):
            path = f"{prefix}.{key}" if prefix else str(key)
            if key not in a:
                diffs.append({"field": path, "a": None, "b": b[key]})
            elif key not in b:
                diffs.append({"field": path, "a": a[key], "b": None})
            else:
                diffs.extend(_deep_diff(a[key], b[key], path))
        return diffs
    if isinstance(a, list) and isinstance(b, list):
        for index in range(max(len(a), len(b))):
            path = f"{prefix}[{index}]"
            if index >= len(a):
                diffs.append({"field": path, "a": None, "b": b[index]})
            elif index >= len(b):
                diffs.append({"field": path, "a": a[index], "b": None})
            else:
                diffs.extend(_deep_diff(a[index], b[index], path))
        return diffs
    if a != b or type(a) is not type(b):
        # 严格区分类型：True 与 1、"200" 与 200 视为差异
        diffs.append({"field": prefix or "$", "a": a, "b": b})
    return diffs


def compare_revisions(name: str, revision_a: int, revision_b: int,
                      seed: Any, steps: list[dict[str, Any]]) -> dict[str, Any]:
    """版本回归对比主入口（纯内存，无任何持久化副作用）。"""
    scenario_a = store.get(name, revision_a)
    if scenario_a is None:
        raise CompareAbort(404, {"error": "revision_not_found",
                                 "name": name, "revision": revision_a})
    scenario_b = store.get(name, revision_b)
    if scenario_b is None:
        raise CompareAbort(404, {"error": "revision_not_found",
                                 "name": name, "revision": revision_b})

    side_a = _CompareSide("a", scenario_a, revision_a, seed)
    side_b = _CompareSide("b", scenario_b, revision_b, seed)

    # 起跑线快照必须在执行链路前捕获（seed 的 draws 此时均为 0）
    initial_snapshots = {
        "a": {"revision": revision_a, **side_a.snapshot()},
        "b": {"revision": revision_b, **side_b.snapshot()},
    }

    results: list[dict[str, Any]] = []
    first_difference: dict[str, Any] | None = None

    for index, step in enumerate(steps):
        if step["type"] == "advance":
            result_a = side_a.advance_step(step["delta_ms"])
            result_b = side_b.advance_step(step["delta_ms"])
        else:
            req = step["request"]
            result_a = side_a.request_step(req)
            result_b = side_b.request_step(req)

        field_diffs = _deep_diff(_canonical(result_a), _canonical(result_b))
        identical = not field_diffs
        entry = {
            "index": index,
            "type": step["type"],
            "identical": identical,
            "differences": field_diffs,
            "a": result_a,
            "b": result_b,
        }
        if step["type"] == "advance":
            entry["delta_ms"] = step["delta_ms"]
        else:
            entry["request"] = req
        results.append(entry)

        if not identical and first_difference is None:
            first_difference = {
                "index": index,
                "type": step["type"],
                "fields": [d["field"] for d in field_diffs],
                "differences": field_diffs,
            }

    return {
        "compare": {"persisted": False},
        "scenario": name,
        "seed": seed,
        "revisions": {"a": revision_a, "b": revision_b},
        "steps_total": len(steps),
        "steps_executed": len(results),
        "identical": first_difference is None,
        "first_difference": first_difference,
        "initial_snapshots": initial_snapshots,
        "steps": results,
        "final_snapshots": {
            "a": {"revision": revision_a, **side_a.snapshot()},
            "b": {"revision": revision_b, **side_b.snapshot()},
        },
    }
