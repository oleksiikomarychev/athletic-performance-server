import os

import httpx
from fastapi import HTTPException
from sqlalchemy import select

from ..config import settings
from ..models.calendar import AppliedCalendarPlan
from ..schemas.macro import MacroRule
from .applied_calendar_plan_service import AppliedCalendarPlanService
from .macro_engine import MacroEngine


async def _workouts(user_id, path, payload=None):
    secret = os.getenv("INTERNAL_GATEWAY_SECRET")
    if not secret:
        raise HTTPException(503, "Не настроена запись макросов")
    headers = {"X-User-Id": user_id, "X-Internal-Secret": secret}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.post(
                f"{settings.WORKOUTS_SERVICE_URL}/internal/macro-previews/{path}", headers=headers, json=payload
            )
            if res.status_code in (404, 409):
                raise HTTPException(res.status_code, res.json().get("detail"))
            res.raise_for_status()
            return res.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(502, "Не удалось подтвердить результат в workouts-service") from exc


async def preview_macros(db, user_id, plan_id, index_offset, observation_workout_id=None, legacy_weights=None):
    engine = MacroEngine(db, user_id)
    if await engine._load_applied_plan(plan_id) is None:
        raise HTTPException(404, "План не найден")
    engine.executed = {tuple(row) for row in await _workouts(user_id, f"{plan_id}/executions")}
    result = await engine.run_for_applied_plan(
        plan_id, index_offset=index_offset, observation_workout_id=observation_workout_id, legacy_weights=legacy_weights
    )
    effects = result.pop("effects")
    result["contract_version"] = 2
    result["set_changes_count"] = len(effects)
    if effects:
        result.update(await _workouts(user_id, str(plan_id), effects))
    return result


async def apply_macros(db, user_id, plan_id, preview_id=None, index_offset=0, mesocycles_only=False):
    if preview_id:
        owner = await db.scalar(
            select(AppliedCalendarPlan.id).where(
                AppliedCalendarPlan.id == plan_id, AppliedCalendarPlan.user_id == user_id
            )
        )
        if owner is None:
            raise HTTPException(404, "План не найден")
        return {"apply_result": await _workouts(user_id, f"{plan_id}/{preview_id}/confirm"), "plan_changes": []}

    engine = MacroEngine(db, user_id)
    plan = await engine._load_applied_plan(plan_id)
    if plan is None:
        raise HTTPException(404, "План не найден")
    if not mesocycles_only:
        macros = await engine._load_plan_macros(plan.calendar_plan_id)
        if any(MacroRule.model_validate_json(m.rule_json).action.type != "Inject_Mesocycle" for m in macros):
            raise HTTPException(409, "Для изменений подходов обновите и подтвердите предпросмотр")
    summary = await engine.run_for_applied_plan(plan_id, index_offset=index_offset, mesocycles_only=True)
    service = AppliedCalendarPlanService(db, user_id)
    results = []
    for item in summary["preview"]:
        for change in item["plan_changes"]:
            p = change["params"]
            results.append(
                await service.inject_mesocycle_into_applied_plan(
                    plan_id,
                    mode=p["mode"],
                    template_id=p.get("template_id"),
                    source_mesocycle_id=p.get("source_mesocycle_id") or p.get("mesocycle_id"),
                    placement=p.get("placement"),
                )
            )
    return {"preview": summary, "plan_changes": results, "apply_result": {"applied": 0, "errors": [], "details": []}}
