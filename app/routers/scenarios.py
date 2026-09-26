"""管理接口：场景校验 / 不可变版本发布 / 版本列表 / 激活 / 查询 / 删除。"""
from __future__ import annotations

from typing import Annotated, Any, Literal

import yaml
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field, field_validator, model_validator

from ..db import get_conn
from ..engine import MAX_COMPARE_STEPS, CompareAbort, compare_revisions
from ..scenario import ScenarioError, parse_scenario
from ..store import store
from .sessions import PreviewBody, _validate_seed

router = APIRouter(prefix="/__scenarios", tags=["scenarios"])


async def _read_definition(request: Request) -> str:
    raw = (await request.body()).decode("utf-8")
    content_type = request.headers.get("content-type", "")
    if "json" in content_type:
        import json
        data = json.loads(raw)
        if isinstance(data, dict) and "definition" in data:
            if not isinstance(data["definition"], str):
                return yaml.safe_dump(data["definition"], allow_unicode=True, sort_keys=False)
            return data["definition"]
        # 整个 JSON 就当作场景定义
        return yaml.safe_dump(data, allow_unicode=True, sort_keys=False)
    return raw


def _summary(scenario) -> dict[str, Any]:
    return {
        "name": scenario.name,
        "description": scenario.description,
        "initial_state": scenario.initial_state,
        "states": scenario.states,
        "timeouts": [
            {"state": state, "after_ms": timeout.after_ms,
             "next_state": timeout.next_state,
             **({"set": timeout.set} if timeout.set else {})}
            for state, timeout in scenario.timeouts.items()
        ],
        "routes": [
            {
                "id": r.id,
                "path": r.path,
                "methods": r.methods,
                "rules": [
                    {"name": rule.name, "priority": rule.priority,
                     "next_state": rule.next_state}
                    for rule in r.rules
                ],
            }
            for r in scenario.routes
        ],
    }


def _not_found(name: str) -> JSONResponse:
    return JSONResponse(status_code=404,
                        content={"error": "scenario_not_found", "name": name})


# ---------------------------------------------------------------------------
# 版本回归对比：POST /__scenarios/{name}/compare
# ---------------------------------------------------------------------------

class CompareRequestStep(PreviewBody):
    """对比链中的业务请求步骤（字段与单次预演完全一致）。"""
    type: Literal["request"] = "request"


class CompareAdvanceStep(BaseModel):
    """对比链中的虚拟时钟推进步骤。"""
    type: Literal["advance"] = "advance"
    delta_ms: int = Field(
        strict=True,
        description="虚拟时钟推进毫秒数（非负整数）；窗口内到期的超时按时刻连续结算",
    )

    @field_validator("delta_ms")
    @classmethod
    def _validate_delta(cls, value: int) -> int:
        if value < 0:
            raise ValueError("delta_ms 必须是非负整数")
        return value


CompareStep = Annotated[
    CompareRequestStep | CompareAdvanceStep,
    Field(discriminator="type",
          description="操作链步骤：业务请求（type=request，默认）或虚拟时钟推进（type=advance）"),
]


class CompareBody(BaseModel):
    revision_a: int = Field(description="左侧（a）执行的不可变 revision")
    revision_b: int = Field(description="右侧（b）执行的不可变 revision")
    seed: Any = Field(
        default=None,
        description="可选随机种子（字符串/数字/布尔）：两侧以同一 seed 从初始状态"
                    "建立确定性随机序列，uuid4() 等逐值可比",
    )
    steps: list[CompareStep] = Field(
        default_factory=list,
        description=f"有序操作链，最多 {MAX_COMPARE_STEPS} 步；每步为业务请求（"
                    "type=request，缺省即此类型）或虚拟时钟推进（type=advance）",
        json_schema_extra={"maxItems": MAX_COMPARE_STEPS},
    )

    _check_seed = field_validator("seed")(_validate_seed)

    @model_validator(mode="before")
    @classmethod
    def _default_step_type(cls, data: Any) -> Any:
        """省略 type 的步骤按业务请求处理（与预演接口的默认语义一致）。"""
        if isinstance(data, dict) and isinstance(data.get("steps"), list):
            normalized = []
            for raw in data["steps"]:
                if isinstance(raw, dict) and "type" not in raw:
                    raw = {**raw, "type": "request"}
                normalized.append(raw)
            data = {**data, "steps": normalized}
        return data

    @field_validator("steps")
    @classmethod
    def _validate_steps(cls, value: list[Any]) -> list[Any]:
        if len(value) > MAX_COMPARE_STEPS:
            raise ValueError(
                f"版本对比操作链最多 {MAX_COMPARE_STEPS} 步，收到 {len(value)} 步")
        return value


@router.post("/{name}/compare")
async def compare_scenario_versions(name: str, body: CompareBody) -> JSONResponse:
    """**版本回归对比**：两个 revision 从各自 ``initial_state`` 建独立内存快照，
    以相同 seed 执行同一条操作链（业务请求 / 虚拟时钟推进），逐步比较响应、
    状态、context、``clock_ms`` 与超时迁移；首次差异后两侧仍各自继续，
    返回每步双侧结果与 ``first_difference``。任一侧步骤失败只记录该侧结果，
    不中断另一侧。全程纯内存：不写 sessions/events，也不持久化随机计数；
    revision 不存在返回 404。
    """
    steps: list[dict[str, Any]] = []
    for step in body.steps:
        if step.type == "advance":
            steps.append({"type": "advance", "delta_ms": step.delta_ms})
        else:
            steps.append({
                "type": "request",
                "request": {
                    "method": step.method or "GET",
                    "path": step.path,
                    "query": step.query or {},
                    "headers": step.headers or {},
                    "body": step.body if step.body is not None else {},
                },
            })

    try:
        payload = compare_revisions(
            name=name,
            revision_a=body.revision_a,
            revision_b=body.revision_b,
            seed=body.seed,
            steps=steps,
        )
    except CompareAbort as exc:
        return JSONResponse(status_code=exc.status_code, content=exc.body)
    return JSONResponse(payload)


@router.post("/validate")
async def validate_scenario(request: Request) -> JSONResponse:
    """仅校验不入库。"""
    text = await _read_definition(request)
    try:
        scenario = parse_scenario(text)
    except ScenarioError as exc:
        return JSONResponse(status_code=422, content={"valid": False, "error": str(exc)})
    except yaml.YAMLError as exc:
        return JSONResponse(status_code=422, content={"valid": False, "error": f"YAML 解析失败: {exc}"})
    return JSONResponse({"valid": True, "scenario": _summary(scenario)})


@router.get("")
async def list_scenarios() -> JSONResponse:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT s.name, s.current_revision, s.created_at, s.updated_at,
               (SELECT COUNT(*) FROM scenario_revisions r WHERE r.scenario = s.name) AS revisions
        FROM scenarios s ORDER BY s.name
        """
    ).fetchall()
    return JSONResponse({
        "scenarios": [
            {"name": r["name"],
             "current_revision": r["current_revision"],
             "revisions": r["revisions"],
             "created_at": r["created_at"],
             "updated_at": r["updated_at"],
             **_summary(store.get(r["name"]))}
            for r in rows
        ]
    })


@router.get("/{name}")
async def get_scenario(name: str) -> JSONResponse:
    conn = get_conn()
    row = conn.execute(
        "SELECT name, current_revision, created_at, updated_at FROM scenarios WHERE name = ?",
        (name,),
    ).fetchone()
    if row is None:
        return _not_found(name)
    return JSONResponse({
        "name": row["name"],
        "current_revision": row["current_revision"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "scenario": _summary(store.get(name)),
    })


@router.get("/{name}/yaml", response_class=PlainTextResponse)
async def get_scenario_yaml(name: str, revision: int | None = None) -> PlainTextResponse:
    """取回原始 YAML；默认当前版本，可用 ?revision=N 指定历史版本。"""
    conn = get_conn()
    if revision is None:
        revision = store.current_revision(name)
        if revision is None:
            return PlainTextResponse(
                f'{{"error":"scenario_not_found","name":"{name}"}}',
                status_code=404,
                media_type="application/json",
            )
    row = conn.execute(
        "SELECT definition FROM scenario_revisions WHERE scenario = ? AND revision = ?",
        (name, revision),
    ).fetchone()
    if row is None:
        return PlainTextResponse(
            f'{{"error":"revision_not_found","name":"{name}","revision":{revision}}}',
            status_code=404,
            media_type="application/json",
        )
    return PlainTextResponse(row["definition"], media_type="application/yaml")


@router.get("/{name}/revisions")
async def list_revisions(name: str) -> JSONResponse:
    """版本列表：全部不可变 revision，标注当前（新会话默认）版本。"""
    conn = get_conn()
    scenario = conn.execute(
        "SELECT current_revision FROM scenarios WHERE name = ?", (name,)
    ).fetchone()
    if scenario is None:
        return _not_found(name)
    current = scenario["current_revision"]
    rows = conn.execute(
        "SELECT revision, created_at FROM scenario_revisions"
        " WHERE scenario = ? ORDER BY revision",
        (name,),
    ).fetchall()
    return JSONResponse({
        "name": name,
        "current_revision": current,
        "revisions": [
            {"revision": r["revision"], "created_at": r["created_at"],
             "current": r["revision"] == current}
            for r in rows
        ],
    })


@router.get("/{name}/revisions/{revision}")
async def get_revision(name: str, revision: int) -> JSONResponse:
    """查看指定 revision 的解析结构。"""
    conn = get_conn()
    row = conn.execute(
        "SELECT r.revision, r.created_at, s.current_revision"
        " FROM scenario_revisions r JOIN scenarios s ON s.name = r.scenario"
        " WHERE r.scenario = ? AND r.revision = ?",
        (name, revision),
    ).fetchone()
    if row is None:
        return JSONResponse(status_code=404, content={
            "error": "revision_not_found", "name": name, "revision": revision,
        })
    return JSONResponse({
        "name": name,
        "revision": row["revision"],
        "current": row["revision"] == row["current_revision"],
        "created_at": row["created_at"],
        "scenario": _summary(store.get(name, revision)),
    })


@router.post("/{name}/revisions/{revision}/activate")
async def activate_revision(name: str, revision: int) -> JSONResponse:
    """把指定 revision 置为当前版本；只影响之后新建的会话，既有会话不变。"""
    if not store.activate(name, revision):
        return JSONResponse(status_code=404, content={
            "error": "revision_not_found", "name": name, "revision": revision,
        })
    return JSONResponse({
        "activated": True,
        "name": name,
        "current_revision": revision,
        "detail": "当前版本已切换；仅之后新建的会话使用该版本，既有会话仍按各自绑定的 revision 执行",
    })


@router.put("/{name}")
async def upsert_scenario(name: str, request: Request) -> JSONResponse:
    """校验并**发布**场景：产生递增 revision 并置为当前版本，旧版本保留。

    提交内容与当前版本完全相同时幂等返回原 revision（action=unchanged）。
    """
    text = await _read_definition(request)
    try:
        scenario, revision, action = store.publish(name, text)
    except ScenarioError as exc:
        return JSONResponse(status_code=422, content={"valid": False, "error": str(exc)})
    except yaml.YAMLError as exc:
        return JSONResponse(status_code=422, content={"valid": False, "error": f"YAML 解析失败: {exc}"})
    return JSONResponse({
        "loaded": True,
        "action": action,
        "revision": revision,
        "current_revision": store.current_revision(name),
        "scenario": _summary(scenario),
    })


@router.delete("/{name}")
async def delete_scenario(name: str, request: Request) -> JSONResponse:
    force = request.query_params.get("force") in {"1", "true", "yes"}
    conn = get_conn()
    sessions = conn.execute(
        "SELECT COUNT(*) AS c FROM sessions WHERE scenario = ?", (name,)
    ).fetchone()["c"]
    if sessions and not force:
        return JSONResponse(status_code=409, content={
            "error": "scenario_in_use",
            "sessions": sessions,
            "detail": "仍有会话引用该场景；确认级联删除请加 ?force=true",
        })
    if store.current_revision(name) is None:
        return _not_found(name)
    if force:
        conn.execute("DELETE FROM events WHERE scenario = ?", (name,))
        conn.execute("DELETE FROM sessions WHERE scenario = ?", (name,))
        conn.commit()
    store.delete(name)
    return JSONResponse({"deleted": True, "name": name})
