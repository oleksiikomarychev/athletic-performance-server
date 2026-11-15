from uuid import uuid4

from backend_common.workout_sets import validate_sets
from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..database import get_db
from ..dependencies import get_current_user_id
from ..models import MacroExecution, MacroPreview, Workout, WorkoutExercise, WorkoutSession, WorkoutSet
from ..redis_client import invalidate_workout_cache
from ..schemas.workout import WorkoutSetResponse
from ..services.exercise_instance_service import ExerciseInstanceService


def internal(x_internal_secret: str | None = Header(None, alias="X-Internal-Secret")):
    if not settings.INTERNAL_GATEWAY_SECRET or x_internal_secret != settings.INTERNAL_GATEWAY_SECRET:
        raise HTTPException(403, "Forbidden")


router = APIRouter(prefix="/internal/macro-previews", dependencies=[Depends(internal)])


class InstanceChange(BaseModel):
    workout_id: int
    instance_id: int
    exercise_id: int
    before: list[WorkoutSetResponse]
    after: list[WorkoutSetResponse]
    rule_keys: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_transition(self):
        validate_sets([s.model_dump() for s in self.after])
        before_ids = {s.id for s in self.before}
        after_ids = [s.id for s in self.after]
        if len(after_ids) != len(set(after_ids)) or any(
            sid == 0 or (sid > 0 and sid not in before_ids) for sid in after_ids
        ):
            raise ValueError("Неверная идентичность подходов в пакете")
        return self


@router.post("/{plan_id}/executions")
async def executions(plan_id: int, db: AsyncSession = Depends(get_db), user_id: str = Depends(get_current_user_id)):
    rows = await db.execute(
        select(MacroExecution.workout_id, MacroExecution.rule_key).where(
            MacroExecution.plan_id == plan_id, MacroExecution.user_id == user_id
        )
    )
    return [list(row) for row in rows]


@router.post("/{plan_id}")
async def prepare(
    plan_id: int,
    effects: list[InstanceChange],
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    if any(not effect.rule_keys for effect in effects):
        raise HTTPException(409, "Для подготовки безопасного пакета обновите plans-service")
    preview = MacroPreview(
        id=str(uuid4()),
        user_id=user_id,
        plan_id=plan_id,
        effects=[effect.model_dump(mode="json") for effect in effects],
    )
    db.add(preview)
    preview_id = preview.id
    await db.commit()
    return {"preview_id": preview_id}


def conflict():
    return HTTPException(409, "Тренировка изменилась. Обновите предпросмотр.")


async def write_effect(db, user_id, effect):
    iid = effect["instance_id"]
    instance = await db.scalar(
        select(WorkoutExercise)
        .where(
            WorkoutExercise.id == iid,
            WorkoutExercise.user_id == user_id,
            WorkoutExercise.workout_id == effect["workout_id"],
            WorkoutExercise.exercise_id == effect["exercise_id"],
        )
        .with_for_update()
    )
    if instance is None:
        raise conflict()
    sets = list(
        (
            await db.scalars(
                select(WorkoutSet)
                .where(WorkoutSet.exercise_id == iid)
                .order_by(WorkoutSet.order_index.asc().nulls_last(), WorkoutSet.id)
                .with_for_update()
            )
        ).all()
    )
    if [WorkoutSetResponse.model_validate(s).model_dump() for s in sets] != effect["before"]:
        raise conflict()
    existing = {s.id: s for s in sets}
    for payload in effect["after"]:
        sid = payload["id"]
        if sid < 0:
            row = WorkoutSet(exercise_id=iid)
            db.add(row)
        else:
            row = existing.pop(sid, None)
            if row is None:
                raise conflict()
        for key, value in payload.items():
            if key != "id":
                setattr(row, key, value)
    for row in existing.values():
        await db.delete(row)
    await db.flush()


@router.post("/{plan_id}/{preview_id}/confirm")
async def confirm(
    plan_id: int, preview_id: str, db: AsyncSession = Depends(get_db), user_id: str = Depends(get_current_user_id)
):
    async with db.begin():
        preview = await db.scalar(
            select(MacroPreview)
            .where(MacroPreview.id == preview_id, MacroPreview.user_id == user_id, MacroPreview.plan_id == plan_id)
            .with_for_update()
        )
        if preview is None:
            raise HTTPException(404, "Предпросмотр не найден")
        effects = sorted(preview.effects, key=lambda e: (e["workout_id"], e["instance_id"]))
        workout_ids = sorted({e["workout_id"] for e in effects})
        result = preview.result
        if result is None:
            if any(not effect.get("rule_keys") for effect in effects):
                raise HTTPException(409, "Предпросмотр создан прежней версией. Обновите предпросмотр.")
            workouts = list(
                (
                    await db.scalars(
                        select(Workout)
                        .where(
                            Workout.id.in_(workout_ids), Workout.user_id == user_id, Workout.applied_plan_id == plan_id
                        )
                        .order_by(Workout.id)
                        .with_for_update()
                    )
                ).all()
            )
            if len(workouts) != len(workout_ids) or any(
                w.started_at or w.completed_at or w.status in {"active", "in_progress", "completed"} for w in workouts
            ):
                raise conflict()
            started_session = await db.scalar(
                select(WorkoutSession.id)
                .where(WorkoutSession.workout_id.in_(workout_ids), WorkoutSession.user_id == user_id)
                .limit(1)
            )
            if started_session is not None:
                raise conflict()
            claims = {(e["workout_id"], key) for e in effects for key in e.get("rule_keys", [])}
            recorded = set(
                (
                    await db.execute(
                        select(MacroExecution.workout_id, MacroExecution.rule_key).where(
                            MacroExecution.plan_id == plan_id,
                            MacroExecution.user_id == user_id,
                            MacroExecution.workout_id.in_(workout_ids),
                        )
                    )
                ).all()
            )
            if claims & recorded:
                raise HTTPException(409, "Правило уже применено по этому наблюдению. Обновите предпросмотр.")
            for wid, key in sorted(claims):
                db.add(MacroExecution(user_id=user_id, plan_id=plan_id, workout_id=wid, rule_key=key))
            for effect in effects:
                InstanceChange.model_validate(effect)
                await write_effect(db, user_id, effect)
            result = dict(
                applied=len(effects),
                status="applied" if effects else "no_changes",
                errors=[],
                details=[{k: e[k] for k in ("workout_id", "instance_id", "exercise_id")} for e in effects],
            )
            preview.result = result
    # Повтор после потерянного ответа также восстанавливает затронутые кеши.
    if effects:
        service = ExerciseInstanceService(db, user_id)
        for e in effects:
            await service._invalidate_local_cache(workout_id=e["workout_id"], instance_id=e["instance_id"])
        await invalidate_workout_cache(user_id, workout_ids=workout_ids)
    return result
