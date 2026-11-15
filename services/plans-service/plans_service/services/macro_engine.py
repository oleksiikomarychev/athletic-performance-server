from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from typing import Any

import httpx
from backend_common.workout_sets import integer_change, validate_sets
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..models.calendar import AppliedCalendarPlan, AppliedPlanWorkout
from ..models.macro import PlanMacro
from ..schemas.macro import MacroRule
from .macro_set_calculation import MissingWeightSettings, calculate_set, table_rows


def ordered_sets(sets):
    return sorted(sets, key=lambda s: (s.get("order_index") is None, s.get("order_index") or 0, s["id"]))


class MacroEngine:
    def __init__(self, db: AsyncSession, user_id: str):
        self.db = db
        self.user_id = user_id
        self.workouts = {}
        self.history = {}
        self.working_sets = {}
        self.rows = None
        self.next_local_id = -1
        self.missing_weights = set()
        self.executed = set()
        self.rule_keys = {}
        self.selector_ids = {}

    async def _load_applied_plan(self, applied_plan_id: int) -> AppliedCalendarPlan | None:
        stmt = (
            select(AppliedCalendarPlan)
            .where(AppliedCalendarPlan.id == applied_plan_id)
            .where(AppliedCalendarPlan.user_id == self.user_id)
        )
        res = await self.db.execute(stmt)
        return res.scalars().first()

    async def _load_plan_macros(self, calendar_plan_id: int) -> list[PlanMacro]:
        stmt = (
            select(PlanMacro)
            .where(PlanMacro.calendar_plan_id == calendar_plan_id)
            .where(PlanMacro.is_active.is_(True))
            .order_by(PlanMacro.priority.asc(), PlanMacro.id.asc())
        )
        res = await self.db.execute(stmt)
        return list(res.scalars().all())

    async def _load_applied_workouts(self, applied_plan_id: int) -> list[dict]:
        stmt = (
            select(AppliedPlanWorkout)
            .where(AppliedPlanWorkout.applied_plan_id == applied_plan_id)
            .order_by(AppliedPlanWorkout.order_index.asc())
        )
        res = await self.db.execute(stmt)
        items = list(res.scalars().all())
        return [
            {"workout_id": i.workout_id, "order_index": i.order_index, "weights": i.macro_weight_settings}
            for i in items
        ]

    def _compare(self, op: str, v: Any, target: Any, rng: Any) -> bool:
        if v is None:
            return False
        try:
            fv = float(v)
        except (TypeError, ValueError):
            return False
        if op in (">", "gt"):
            return fv > float(target)
        if op in ("<", "lt"):
            return fv < float(target)
        if op in (">=", "ge"):
            return fv >= float(target)
        if op in ("<=", "le"):
            return fv <= float(target)
        if op in ("=", "==", "eq"):
            return abs(fv - float(target)) < 1e-6
        if op in ("!=", "ne"):
            return abs(fv - float(target)) >= 1e-6
        if op in ("in_range", "in"):
            if isinstance(rng, list | tuple) and len(rng) >= 2:
                a, b = float(rng[0]), float(rng[1])
                lo, hi = (a, b) if a <= b else (b, a)
                return (fv >= lo) and (fv <= hi)
            return False
        if op in ("not_in_range", "not_in"):
            if isinstance(rng, list | tuple) and len(rng) >= 2:
                a, b = float(rng[0]), float(rng[1])
                lo, hi = (a, b) if a <= b else (b, a)
                return not ((fv >= lo) and (fv <= hi))
            return True
        return False

    @staticmethod
    def _e1rm_from_weight_reps(weight: float, reps: int) -> float:
        if reps <= 1:
            return float(weight)
        return float(weight) * (1.0 + (float(reps) / 30.0))

    @staticmethod
    def _trend_stagnates(values: list[float], epsilon_percent: float) -> bool:
        if not values or len(values) < 2:
            return False
        vals = [float(v) for v in values if v is not None]
        if len(vals) < 2:
            return False
        lo = min(vals)
        hi = max(vals)
        mu = sum(vals) / len(vals)
        if mu <= 0:
            return False
        width_pct = (hi - lo) / mu * 100.0
        return width_pct <= float(epsilon_percent)

    @staticmethod
    def _trend_deviates(values: list[float], value_percent: float, direction: str | None) -> bool:
        if not values or len(values) < 2:
            return False
        vals = [float(v) for v in values if v is not None]
        if len(vals) < 2:
            return False
        last = vals[-1]
        prior = vals[:-1]
        mu = sum(prior) / len(prior)
        if mu == 0:
            return False
        delta_pct = (last - mu) / abs(mu) * 100.0
        thr = float(value_percent)
        if direction == "positive":
            return delta_pct >= thr
        if direction == "negative":
            return (-delta_pct) >= thr
        return abs(delta_pct) >= thr

    async def _get(self, url, params=None):
        try:
            response = await self.client.get(url, params=params, headers={"X-User-Id": self.user_id})
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise HTTPException(502, "Не удалось получить данные для расчёта макроса") from exc

    async def _workout(self, wid):
        if wid not in self.workouts:
            data = await self._get(f"{settings.WORKOUTS_SERVICE_URL}/workouts/{wid}")
            if (
                not isinstance(data, dict)
                or data.get("id") != wid
                or not isinstance(data.get("exercise_instances"), list)
            ):
                raise HTTPException(502, "Неверный ответ workouts-service")
            for instance in data["exercise_instances"]:
                if not isinstance(instance, dict) or not all(k in instance for k in ("id", "exercise_list_id", "sets")):
                    raise HTTPException(502, "Неверный экземпляр упражнения")
                sets = instance["sets"]
                if not isinstance(sets, list) or any(
                    not isinstance(s, dict)
                    or type(s.get("id")) is not int
                    or (s.get("order_index") is not None and type(s["order_index"]) is not int)
                    for s in sets
                ):
                    raise HTTPException(502, "Неверный список подходов workouts-service")
                instance["sets"] = ordered_sets(sets)
            self.workouts[wid] = data
        return self.workouts[wid]

    async def _ids(self, target):
        if target.get("exercise_ids"):
            return set(target["exercise_ids"])
        if target.get("exercise_id"):
            return {target["exercise_id"]}
        selector = target.get("selector")
        if not selector:
            return None
        cache_key = json.dumps(selector, sort_keys=True)
        if cache_key in self.selector_ids:
            return self.selector_ids[cache_key]
        filters = {k: {str(v).casefold() for v in vs} for k, vs in selector["value"].items() if vs}
        result = set()
        offset = 0
        while True:
            page = await self._get(
                f"{settings.EXERCISES_SERVICE_URL}/exercises/definitions/", {"offset": offset, "limit": 50}
            )
            if not isinstance(page, list) or any(not isinstance(item, dict) or "id" not in item for item in page):
                raise HTTPException(502, "Некорректный ответ каталога упражнений")
            result.update(
                item["id"] for item in page if all(str(item.get(k, "")).casefold() in vs for k, vs in filters.items())
            )
            if len(page) < 50:
                self.selector_ids[cache_key] = result
                return result
            offset += len(page)

    async def _session(self, wid):
        if wid not in self.history:
            sessions = await self._get(f"{settings.WORKOUTS_SERVICE_URL}/workouts/sessions/{wid}/history")
            if not isinstance(sessions, list) or any(not isinstance(s, dict) for s in sessions):
                raise HTTPException(502, "Неверный формат истории сессий")
            self.history[wid] = next((s for s in sessions if s.get("finished_at")), None)
        return self.history[wid]

    async def _metric(self, metric, wid, ids):
        workout = await self._workout(wid)
        session = await self._session(wid)
        progress = (session or {}).get("progress") or {}
        if metric in ("Readiness_Score", "RPE_Session"):
            if "macro_metrics" in progress:
                return progress["macro_metrics"].get(metric)
            return workout.get("readiness_score" if metric == "Readiness_Score" else "rpe_session")
        instances = [i for i in workout["exercise_instances"] if ids is None or i["exercise_list_id"] in ids]
        completed = progress.get("macro_completed", progress.get("completed")) or {}
        baseline = progress.get("macro_baseline")
        actuals = progress.get("macro_actuals")
        if actuals is not None and "macro_exercises" in progress:
            instances = [
                {"id": int(iid), "sets": ordered_sets([{**part, "id": int(sid)} for sid, part in actuals[iid].items()])}
                for iid, eid in progress["macro_exercises"].items()
                if ids is None or eid in ids
            ]
        if metric == "Total_Reps":
            if ids is not None and not instances:
                return None
            values = [
                p.get("volume", p.get("reps"))
                for inst in instances
                for part in inst["sets"]
                if actuals is None or part["id"] in completed.get(str(inst["id"]), [])
                for p in part.get("subsets") or [part]
            ]
            return sum(values) if None not in values else None
        if not session:
            return None
        values = []
        for inst in instances:
            iid = str(inst["id"])
            finished = set(completed.get(iid, []))
            for s in inst["sets"]:
                if s["id"] not in finished:
                    values.append(None)
                    continue
                sid = str(s["id"])
                actual = (actuals or {}).get(iid, {}).get(sid, s)
                parts = actual.get("subsets") or [actual]
                if metric in ("Performance_Trend", "e1RM"):
                    for part in parts:
                        weight = part.get("working_weight", part.get("weight"))
                        reps = part.get("volume", part.get("reps"))
                        if weight and reps:
                            values.append(self._e1rm_from_weight_reps(weight, reps))
                    continue
                planned = (baseline or {}).get(iid, {}).get(sid)
                if planned is None:
                    raise ValueError("Для дельты отсутствует плановый снимок подхода")
                previous = planned.get("subsets") or [planned]
                if len(parts) != len(previous):
                    raise ValueError("Изменился состав вложенного подхода")
                key, alias = ("effort", "rpe") if metric == "RPE_Delta_From_Plan" else ("volume", "reps")
                if planned.get("set_type") != actual.get("set_type"):
                    raise ValueError("Изменился тип планового подхода")
                if planned.get("subsets") and any(not part.get("macro_part_id") for part in previous + parts):
                    raise ValueError("Для дельты отсутствует идентичность частей исторического подхода")
                deltas = []
                for left, right in zip(previous, parts, strict=True):
                    if left.get("macro_part_id") != right.get("macro_part_id"):
                        raise ValueError("Изменилась идентичность частей подхода")
                    a, b = left.get(key, left.get(alias)), right.get(key, right.get(alias))
                    deltas.append(b - a if a is not None and b is not None else None)
                values.append(tuple(deltas) if len(deltas) > 1 else deltas[0])
            # Серия последовательных подходов ограничена одним экземпляром.
            values.append(None)
        if metric in ("Performance_Trend", "e1RM"):
            return max((v for v in values if v is not None), default=None)
        return values

    async def _matches(self, rule, observations):
        if not observations:
            return False
        trigger, cond = rule["trigger"], rule["condition"]
        metric, op = trigger["metric"], cond["op"]
        count = cond["n"] if op in {"holds_for", "stagnates_for", "deviates_from_avg"} else 1
        if len(observations) < count:
            return False
        ids = await self._ids(trigger)
        if ids == set():
            return False
        series = []
        if op in {"holds_for", "stagnates_for", "deviates_from_avg"}:
            observations = [wid for wid in observations if await self._session(wid)]
            if len(observations) < count:
                return False
        for wid in observations[-count:]:
            value = await self._metric(metric, wid, ids)
            if isinstance(value, list):
                if op == "holds_for_sets":
                    run = 0
                    for v in value:
                        parts = v if isinstance(v, tuple) else (v,)
                        matched = all(
                            self._compare(cond["relation"], part, cond.get("value"), cond.get("range"))
                            for part in parts
                        )
                        run = run + 1 if matched else 0
                        if run >= cond["n_sets"]:
                            return True
                    return False
                known = [p for v in value for p in (v if isinstance(v, tuple) else (v,)) if p is not None]
                value = sum(known) / len(known) if known else None
            if value is None:
                return False
            series.append(value)
        if op == "stagnates_for":
            return self._trend_stagnates(series, cond["epsilon_percent"])
        if op == "deviates_from_avg":
            return self._trend_deviates(series, cond["value_percent"], cond.get("direction"))
        return all(
            self._compare(cond["relation"] if op == "holds_for" else op, v, cond.get("value"), cond.get("range"))
            for v in series
        )

    async def _changes(self, action, wid, inst, weight_context):
        key = (wid, inst["id"])
        sets = self.working_sets.setdefault(key, deepcopy(inst["sets"]))
        eid = inst["exercise_list_id"]
        value = action["params"]["value"]
        validate_sets(sets)
        patches = []

        def add(before, after, changes, alternatives=None):
            patches.append(
                dict(
                    workout_id=wid,
                    exercise_id=eid,
                    instance_id=inst["id"],
                    set_id=(before or after)["id"],
                    before=before,
                    after=after,
                    changes=changes,
                    alternatives=alternatives or [],
                )
            )

        if action["type"] == "Adjust_Sets":
            value = integer_change(value)
            if value > 0:
                validate_sets(sets + [sets[-1]] * value if sets else [])
            if value > 0 and not sets:
                raise ValueError("Нет образца для добавления подхода")
            if value > 0:
                for _ in range(value):
                    clone = deepcopy(sets[-1])
                    clone["id"] = self.next_local_id
                    self.next_local_id -= 1
                    # При NULL порядок задаётся возрастающим id, который назначит владелец.
                    if clone.get("order_index") is not None:
                        clone["order_index"] += 1
                    sets.append(clone)
                    add(None, clone, {"action": "add_set", "template": clone})
            else:
                for _ in range(min(len(sets), -value)):
                    add(sets.pop(), None, {"action": "remove_set"})
            validate_sets(sets)
            return patches

        if self.rows is None:
            try:
                self.rows = table_rows(await self._get(f"{settings.RPE_SERVICE_URL}/rpe/table"))
            except (ValueError, TypeError) as exc:
                raise HTTPException(502, "Некорректная таблица RPE") from exc
        maximum = (weight_context.get("effective_maxima") or {}).get(str(eid))
        rounding = (maximum, weight_context["rounding_step"], weight_context["rounding_mode"]) if maximum else None
        for index, before in enumerate(sets):
            try:
                after, alternatives = calculate_set(
                    before, action["type"], action["params"]["mode"], value, self.rows, rounding
                )
            except MissingWeightSettings:
                self.missing_weights.add(eid)
                continue
            except ValueError as exc:
                raise ValueError(f"Упражнение {eid}, подход {before['id']}: {exc}") from exc
            if after != before or alternatives:
                changes = {k: v for k, v in after.items() if v != before.get(k)}
                add(before, after, changes, alternatives)
                sets[index] = after
        validate_sets(sets)
        return patches

    async def run_for_applied_plan(
        self, applied_plan_id, index_offset=0, observation_workout_id=None, mesocycles_only=False, legacy_weights=None
    ):
        plan = await self._load_applied_plan(applied_plan_id)
        if plan is None:
            raise HTTPException(404, "План не найден")
        macros = await self._load_plan_macros(plan.calendar_plan_id)
        ordered = await self._load_applied_workouts(applied_plan_id)
        idx = (plan.current_workout_index or 0) + (index_offset or 0)
        if observation_workout_id is not None:
            observed = next((w for w in ordered if w["workout_id"] == observation_workout_id), None)
            if not observed:
                raise HTTPException(404, "Тренировка отсутствует в плане")
            idx = observed["order_index"] + 1
        observations = [w["workout_id"] for w in ordered if w["order_index"] < idx]
        candidates = {w["workout_id"]: w["weights"] or {} for w in ordered if w["order_index"] >= idx}
        preview = []
        async with httpx.AsyncClient(timeout=8) as self.client:
            for macro in macros:
                try:
                    rule = MacroRule.model_validate_json(macro.rule_json).model_dump(mode="json", exclude_none=True)
                    action = rule["action"]
                    fingerprint = hashlib.sha256(json.dumps(rule, sort_keys=True).encode()).hexdigest()
                    rule_key = f"{macro.id}:{observations[-1] if observations else 0}:{fingerprint}"
                    if mesocycles_only and action["type"] != "Inject_Mesocycle":
                        continue
                    count = rule["duration"]["count"]
                    targets = []
                    for wid in candidates:
                        data = await self._workout(wid)
                        if (
                            data.get("started_at")
                            or data.get("completed_at")
                            or data.get("status") in {"completed", "in_progress", "active"}
                        ):
                            continue
                        targets.append(wid)
                        if len(targets) == count:
                            break
                    if action["type"] == "Inject_Mesocycle" and rule["condition"]["op"] not in {
                        "holds_for",
                        "deviates_from_avg",
                        "stagnates_for",
                    }:
                        # Простые условия вставки существующий DSL проверяет по каждой цели.
                        matched = [wid for wid in targets if await self._matches(rule, [wid])]
                    else:
                        matched = targets if targets and await self._matches(rule, observations) else []
                    if action["type"] != "Inject_Mesocycle":
                        matched = [wid for wid in matched if (wid, rule_key) not in self.executed]
                    patches, plan_changes = [], []
                    if matched and action["type"] == "Inject_Mesocycle":
                        plan_changes.append({"type": action["type"], "params": action["params"]})
                    elif matched:
                        ids = await self._ids(action.get("target", {}))
                        for wid in matched:
                            for inst in (await self._workout(wid))["exercise_instances"]:
                                if ids is None or inst["exercise_list_id"] in ids:
                                    context = candidates[wid]
                                    if str(inst["exercise_list_id"]) not in context.get("effective_maxima", {}):
                                        context = legacy_weights or context
                                    changes = await self._changes(action, wid, inst, context)
                                    patches.extend(changes)
                                    if changes:
                                        self.rule_keys.setdefault((wid, inst["id"]), set()).add(rule_key)
                    preview.append(
                        dict(
                            macro_id=macro.id,
                            name=macro.name,
                            priority=macro.priority,
                            is_active=True,
                            duration=rule["duration"],
                            target_workouts=targets,
                            matched_workouts=matched,
                            patches=patches,
                            plan_changes=plan_changes,
                        )
                    )
                except (ValidationError, ValueError, TypeError, KeyError) as exc:
                    raise HTTPException(422, f"Макрос {macro.id}: {exc}") from exc
        if self.missing_weights:
            return dict(
                applied_plan_id=applied_plan_id,
                preview=[],
                effects=[],
                needs_weight_settings=sorted(self.missing_weights),
                observation_workout_id=observations[-1] if observations else None,
            )
        effects = []
        for (wid, iid), after in self.working_sets.items():
            inst = next(i for i in self.workouts[wid]["exercise_instances"] if i["id"] == iid)
            if after != inst["sets"]:
                effects.append(
                    dict(
                        workout_id=wid,
                        instance_id=iid,
                        exercise_id=inst["exercise_list_id"],
                        before=inst["sets"],
                        after=after,
                        rule_keys=sorted(self.rule_keys.get((wid, iid), [])),
                    )
                )
        return dict(
            applied_plan_id=applied_plan_id,
            calendar_plan_id=plan.calendar_plan_id,
            macros_evaluated=len(macros),
            actions_applied=0,
            preview=preview,
            effects=effects,
            observation_workout_id=observations[-1] if observations else None,
        )
