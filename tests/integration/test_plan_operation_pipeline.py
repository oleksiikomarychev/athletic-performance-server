"""HTTP + PostgreSQL + Redis/Celery contract tests on isolated schemas.

Requires CONTRACT_TEST_POSTGRES_URL and CONTRACT_TEST_REDIS_URL. Never uses the
application database: every test creates and drops its own random schemas.
"""

import asyncio
import os
import socket
import threading
import time
import uuid
from datetime import datetime

import httpx
import pytest
import uvicorn
from celery.contrib.testing.worker import start_worker
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from plans_service import dependencies as pd
from plans_service import models as pm
from plans_service.celery_app import celery_app as plans_celery
from plans_service.routers import applied_calendar_plans as plan_routes
from plans_service.services.applied_calendar_plan_service import AppliedCalendarPlanService
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from workouts_service import database as wd
from workouts_service import dependencies as wdep
from workouts_service import models as wm
from workouts_service.celery_app import celery_app as workouts_celery
from workouts_service.routers import internal_purge, macro_previews, sessions, workout_generation, workouts
from workouts_service.tasks import workout_tasks as wt
from workouts_service.workout_calculation import WorkoutCalculator


class Server:
    def __init__(self, app):
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.url = f"http://127.0.0.1:{self.sock.getsockname()[1]}"
        self.server = uvicorn.Server(uvicorn.Config(app, log_level="error", lifespan="off"))
        self.thread = threading.Thread(target=self.server.run, kwargs={"sockets": [self.sock]}, daemon=True)

    def start(self):
        self.thread.start()
        deadline = time.monotonic() + 10
        while not self.server.started and time.monotonic() < deadline:
            time.sleep(0.01)
        assert self.server.started

    def stop(self):
        self.server.should_exit = True
        self.thread.join(timeout=10)
        self.sock.close()
        assert not self.thread.is_alive()


@pytest.fixture
async def pipeline(monkeypatch):
    pg = os.getenv("CONTRACT_TEST_POSTGRES_URL")
    redis = os.getenv("CONTRACT_TEST_REDIS_URL")
    if not pg or not redis:
        pytest.skip("Isolated PostgreSQL and Redis URLs are required")
    token = uuid.uuid4().hex
    admin = create_async_engine(pg, poolclass=NullPool)
    schemas = ["plans_" + token, "workouts_" + token]
    engines = []
    factories = []
    servers = []
    worker = None
    config_keys = ["broker_url", "result_backend", "task_default_queue", "task_always_eager"]
    old_configs = [(app, {key: app.conf[key] for key in config_keys}) for app in (plans_celery, workouts_celery)]
    try:
        for schema, base in zip(schemas, (pm.Base, wd.Base)):
            async with admin.begin() as conn:
                await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
            engine = create_async_engine(
                pg, poolclass=NullPool, connect_args={"server_settings": {"search_path": schema}}
            )
            engines.append(engine)
            async with engine.begin() as conn:
                await conn.run_sync(base.metadata.create_all)
            factories.append(async_sessionmaker(engine, expire_on_commit=False))
        pf, wf = factories

        async def plans_db():
            async with pf() as db:
                yield db

        async def workouts_db():
            async with wf() as db:
                yield db

        async def user(request: Request):
            return request.headers.get("x-user-id", "pipeline-user")

        async def no_maxes(*args, **kwargs):
            return []

        async def no_exercises(*args, **kwargs):
            return {}

        monkeypatch.setattr(AppliedCalendarPlanService, "_fetch_user_maxes_by_ids", no_maxes)
        monkeypatch.setattr(AppliedCalendarPlanService, "_ensure_exercises_present", no_exercises)
        monkeypatch.setattr(AppliedCalendarPlanService, "_fetch_exercise_metadata", no_exercises)
        monkeypatch.setattr(WorkoutCalculator, "_fetch_user_maxes", no_maxes)
        monkeypatch.setenv("INTERNAL_GATEWAY_SECRET", "pipeline-secret")
        monkeypatch.setenv("WORKOUT_GENERATION_CHUNK_SIZE", "1")
        monkeypatch.setattr(internal_purge.settings, "INTERNAL_GATEWAY_SECRET", "pipeline-secret")
        monkeypatch.setattr(wt, "AsyncSessionLocal", wf)

        # Worker sessions use the same isolated schema but a fresh engine per task.
        def task_engine(*args, **kwargs):
            engine = create_async_engine(
                pg, poolclass=NullPool, connect_args={"server_settings": {"search_path": schemas[0]}}
            )
            return engine, async_sessionmaker(engine, expire_on_commit=False)

        monkeypatch.setattr("backend_common.database.create_async_engine_and_session", task_engine)
        state = {
            "generate_calls": 0,
            "fail_generate": None,
            "fail_cleanup": 0,
            "cleanup_calls": 0,
            "start_first": False,
            "malformed_generate": False,
        }
        wa = FastAPI()
        wa.dependency_overrides[wd.get_db] = workouts_db
        wa.dependency_overrides[wdep.get_current_user_id] = user
        wa.include_router(workouts.router, prefix="/workouts")
        wa.include_router(workout_generation.router, prefix="/workouts")
        wa.include_router(internal_purge.router)
        wa.include_router(macro_previews.router)
        wa.include_router(sessions.router, prefix="/workouts")

        @wa.middleware("http")
        async def inject_failure(request: Request, call_next):
            if request.url.path.endswith("/generate"):
                state["generate_calls"] += 1
                if state["generate_calls"] == state["fail_generate"]:
                    if state["start_first"]:
                        from workouts_service.services.session_service import SessionService

                        async with wf() as db:
                            workout = (await db.execute(select(wm.Workout).order_by(wm.Workout.id))).scalars().first()
                            session = await SessionService(db, user_id="pipeline-user").start_workout_session(
                                workout.id
                            )
                            session.progress = {"completed_sets": [1]}
                            await db.commit()
                    if state["malformed_generate"]:
                        return JSONResponse({"workout_ids": [42, 43]})
                    return JSONResponse({"detail": "injected generation failure"}, status_code=500)
            if request.url.path.endswith("/cleanup"):
                state["cleanup_calls"] += 1
                if state["fail_cleanup"]:
                    state["fail_cleanup"] -= 1
                    return JSONResponse({"detail": "injected cleanup outage"}, status_code=503)
            return await call_next(request)

        pa = FastAPI()
        pa.dependency_overrides[pd.get_db] = plans_db
        pa.dependency_overrides[pd.get_current_user_id] = user
        from plans_service.routers import macros

        pa.include_router(plan_routes.router, prefix="/plans")
        pa.include_router(macros.router, prefix="/plans")
        for app in (wa, pa):
            server = Server(app)
            server.start()
            servers.append(server)
        monkeypatch.setenv("WORKOUTS_SERVICE_URL", servers[0].url)
        from plans_service.config import settings as plan_settings

        monkeypatch.setattr(plan_settings, "WORKOUTS_SERVICE_URL", servers[0].url)
        monkeypatch.setattr(plan_settings, "RPE_SERVICE_URL", servers[0].url)
        import json
        from pathlib import Path

        @wa.get("/rpe/table")
        def rpe_table():
            return json.loads((Path(__file__).resolve().parents[2] / "services/rpe-service/rpe_table.json").read_text())

        # Use the actual gateway HTTP proxy; identity verification is outside this contract.
        from gateway_app import main as gateway_main
        from gateway_app.routes.plans import plans_applied_router, plans_calendar_router
        from gateway_app.routes.workouts import workouts_router

        monkeypatch.setattr(gateway_main, "PLANS_SERVICE_URL", servers[1].url)
        monkeypatch.setattr(gateway_main, "WORKOUTS_SERVICE_URL", servers[0].url)
        monkeypatch.setenv("PLANS_SERVICE_URL", servers[1].url)
        ga = FastAPI()

        @ga.middleware("http")
        async def identity(request: Request, call_next):
            request.state.user = {"uid": state.get("user", "pipeline-user")}
            return await call_next(request)

        ga.include_router(plans_calendar_router)
        ga.include_router(plans_applied_router)
        ga.include_router(workouts_router)
        gateway_server = Server(ga)
        gateway_server.start()
        servers.append(gateway_server)
        for app in (plans_celery, workouts_celery):
            app.conf.update(
                broker_url=redis, result_backend=redis, task_always_eager=False, task_default_queue="plans.tasks"
            )
        plans_celery.set_default()
        plans_celery.set_current()
        plans_celery.loader.import_default_modules()
        worker = start_worker(
            plans_celery,
            pool="solo",
            concurrency=1,
            perform_ping_check=False,
            queues=["plans.tasks", "workouts.tasks"],
            shutdown_timeout=15,
        )
        await asyncio.to_thread(worker.__enter__)

        async with pf() as db:
            plan = pm.CalendarPlan(id=1, root_plan_id=1, user_id="pipeline-user", name="Pipeline", duration_weeks=1)
            db.add(plan)
            await db.flush()
            meso = pm.Mesocycle(calendar_plan_id=1, name="M", duration_weeks=1, order_index=0)
            db.add(meso)
            await db.flush()
            micro = pm.Microcycle(mesocycle_id=meso.id, name="MC", days_count=2, order_index=0)
            db.add(micro)
            await db.flush()
            db.add_all(
                [
                    pm.PlanWorkout(
                        microcycle_id=micro.id,
                        day_label="Day 1",
                        order_index=0,
                        created_at=datetime(2026, 11, 1),
                        updated_at=datetime(2026, 11, 1),
                    ),
                    pm.PlanWorkout(
                        microcycle_id=micro.id,
                        day_label="Day 2",
                        order_index=1,
                        created_at=datetime(2026, 11, 1),
                        updated_at=datetime(2026, 11, 1),
                    ),
                ]
            )
            await db.commit()
        async with httpx.AsyncClient(base_url=servers[2].url, timeout=20) as client:
            yield client, pf, wf, state, servers[0].url
    finally:
        if worker is not None:
            await asyncio.to_thread(worker.__exit__, None, None, None)
        for server in reversed(servers):
            await asyncio.to_thread(server.stop)
        for app, config in old_configs:
            app.conf.update(config)
        for engine in engines:
            await engine.dispose()
        async with admin.begin() as conn:
            for schema in schemas:
                await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin.dispose()


COMPUTE = {
    "compute_weights": False,
    "rounding_step": 2.5,
    "generate_workouts": True,
    "start_date": "2026-11-15T00:00:00Z",
}


async def wait_for(check, timeout=12):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = await check()
        if result:
            return result
        await asyncio.sleep(0.05)
    raise AssertionError("Pipeline condition did not complete in time")


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup_outage", [False, True])
@pytest.mark.parametrize("preserve_started", [False, True])
async def test_second_batch_failure_rolls_back_plan_and_cleans_workouts(
    pipeline, monkeypatch, cleanup_outage, preserve_started
):
    client, pf, wf, state, _ = pipeline
    state["fail_generate"] = 2
    state["fail_cleanup"] = 2 if cleanup_outage else 0
    state["start_first"] = preserve_started
    if cleanup_outage:
        cleanup_task = plans_celery.tasks["plans.cleanup_failed_application"]
        production_retry = cleanup_task.retry

        def immediate_retry(*args, **kwargs):
            kwargs["countdown"] = 0  # Exercise a real retry without the production minute-long delay.
            return production_retry(*args, **kwargs)

        monkeypatch.setattr(cleanup_task, "retry", immediate_retry)
    response = await client.post("/api/v1/plans/applied-plans/apply/1", params={"user_max_ids": "1"}, json=COMPUTE)
    assert response.status_code == 500
    assert state["generate_calls"] == 2

    async def cleaned():
        async with wf() as db:
            return not list(
                (await db.execute(select(wm.Workout).where(wm.Workout.applied_plan_id.is_not(None)))).scalars()
            )

    await wait_for(cleaned)
    assert state["cleanup_calls"] >= (3 if cleanup_outage else 1)
    async with wf() as db:
        remaining = list((await db.execute(select(wm.Workout))).scalars())
        assert len(remaining) == int(preserve_started)
        if preserve_started:
            assert remaining[0].applied_plan_id is None and remaining[0].plan_order_index is None
            progress = (await db.execute(select(wm.WorkoutSession.progress))).scalar_one()
            assert progress == {"completed_sets": [1]}
    async with pf() as db:
        assert not list((await db.execute(select(pm.AppliedCalendarPlan))).scalars())


@pytest.mark.asyncio
@pytest.mark.parametrize("async_replacement", [False, True])
async def test_explicit_replacement_retains_completed_history_sync_and_async(pipeline, async_replacement):
    client, pf, wf, state, _ = pipeline
    first = await client.post("/api/v1/plans/applied-plans/apply/1", params={"user_max_ids": "1"}, json=COMPUTE)
    assert first.status_code == 200, first.text
    old_id = first.json()["id"]
    async with wf() as db:
        workout = (await db.execute(select(wm.Workout).order_by(wm.Workout.id))).scalars().first()
        history_id = workout.id
        workout.status = "completed"
        workout.completed_at = datetime(2026, 11, 15)
        await db.commit()
    rejected = await client.post("/api/v1/plans/applied-plans/apply/1", params={"user_max_ids": "1"}, json=COMPUTE)
    assert rejected.status_code == 409
    path = "/api/v1/plans/applied-plans/apply" + ("-async" if async_replacement else "") + "/1"
    result = await client.post(path, params={"user_max_ids": "1", "replace_active": "true"}, json=COMPUTE)
    assert result.status_code == 200, result.text
    if async_replacement:
        task_id = result.json()["task_id"]

        async def done():
            response = await client.get(f"/api/v1/plans/applied-plans/tasks/{task_id}")
            data = response.json()
            return data if data.get("status") in ("SUCCESS", "FAILURE", "success", "failure") else None

        data = await wait_for(done)
        assert str(data["status"]).upper() == "SUCCESS", data
    async with pf() as db:
        plans = list((await db.execute(select(pm.AppliedCalendarPlan).order_by(pm.AppliedCalendarPlan.id))).scalars())
        assert len(plans) == 2
        assert plans[0].id == old_id and not plans[0].is_active
        assert plans[1].is_active
    async with wf() as db:
        history = await db.get(wm.Workout, history_id)
        assert history.status == "completed" and history.completed_at == datetime(2026, 11, 15)
        assert history.applied_plan_id == old_id


@pytest.mark.asyncio
async def test_async_mass_edit_and_shift_preserve_command_filters(pipeline):
    _, _, wf, _, workouts_url = pipeline
    dates = [datetime(2026, 11, 15), datetime(2026, 11, 16), datetime(2026, 11, 17)]
    async with wf() as db:
        for index, date in enumerate(dates):
            w = wm.Workout(
                user_id="pipeline-user",
                name=f"W{index}",
                applied_plan_id=9,
                plan_order_index=index,
                status="completed" if index == 2 else "pending",
                scheduled_for=date,
                workout_type="generated",
            )
            db.add(w)
            await db.flush()
            ex = wm.WorkoutExercise(user_id="pipeline-user", workout_id=w.id, exercise_id=1)
            db.add(ex)
            await db.flush()
            db.add(wm.WorkoutSet(exercise_id=ex.id, volume=5, working_weight=10))
        await db.commit()

    async def finished(task_id):
        from celery.result import AsyncResult

        result = AsyncResult(task_id, app=plans_celery)
        if not result.ready():
            return None
        assert result.successful(), str(result.result)
        return result.result

    async with httpx.AsyncClient(base_url=workouts_url, timeout=20) as client:
        mass = {
            "mode": "apply",
            "filter": {"plan_order_indices": [0, 2], "only_future": False},
            "actions": {"set_weight": 35},
        }
        response = await client.post("/workouts/applied-plans/9/mass-edit-sets-async", json=mass)
        assert response.status_code == 200, response.text
        task_id = response.json()["task_id"]
        await wait_for(lambda: finished(task_id))
        async with wf() as db:
            weights = list(
                (await db.execute(select(wm.WorkoutSet.working_weight).order_by(wm.WorkoutSet.id))).scalars()
            )
            assert weights == [35, 10, 10]

        shift = {
            "from_date": "2026-11-15T00:00:00Z",
            "to_date": "2026-11-15T23:59:00Z",
            "days": 3,
            "only_future": False,
            "mode": "preview",
        }
        response = await client.post("/workouts/applied-plans/9/shift-schedule-async", json=shift)
        assert response.status_code == 200, response.text
        task_id = response.json()["task_id"]
        result = await wait_for(lambda: finished(task_id))
        assert result["summary"]["mode"] == "preview"
        async with wf() as db:
            actual = list((await db.execute(select(wm.Workout.scheduled_for).order_by(wm.Workout.id))).scalars())
            assert actual == dates
        shift["mode"] = "apply"
        response = await client.post("/workouts/applied-plans/9/shift-schedule-async", json=shift)
        assert response.status_code == 200, response.text
        task_id = response.json()["task_id"]
        await wait_for(lambda: finished(task_id))
        async with wf() as db:
            actual = list((await db.execute(select(wm.Workout.scheduled_for).order_by(wm.Workout.id))).scalars())
            assert actual == [datetime(2026, 11, 18), dates[1], dates[2]]


@pytest.mark.asyncio
@pytest.mark.parametrize("async_mode", [False, True])
async def test_mass_edit_failure_has_no_partial_write_over_http_or_worker(pipeline, monkeypatch, async_mode):
    from workouts_service.services.exercise_instance_service import ExerciseInstanceService

    _, _, wf, _, url = pipeline
    async with wf() as db:
        w = wm.Workout(user_id="pipeline-user", name="Rollback", applied_plan_id=9, plan_order_index=0)
        db.add(w)
        await db.flush()
        ex = wm.WorkoutExercise(user_id="pipeline-user", workout_id=w.id, exercise_id=1)
        db.add(ex)
        await db.flush()
        db.add_all(
            [wm.WorkoutSet(exercise_id=ex.id, working_weight=10), wm.WorkoutSet(exercise_id=ex.id, working_weight=20)]
        )
        await db.commit()
    original = ExerciseInstanceService.update_set
    calls = 0

    async def fail_second(self, *args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected second-set failure")
        return await original(self, *args, **kwargs)

    monkeypatch.setattr(ExerciseInstanceService, "update_set", fail_second)
    # Observe the failed attempt without waiting for a production retry countdown.
    monkeypatch.setattr(plans_celery.tasks["workouts.applied_plan_mass_edit_sets"], "max_retries", 0)
    path = "/workouts/applied-plans/9/mass-edit-sets" + ("-async" if async_mode else "")
    payload = {
        "mode": "apply",
        "filter": {"plan_order_indices": [0], "only_future": False},
        "actions": {"set_weight": 50},
    }
    async with httpx.AsyncClient(base_url=url, timeout=20) as client:
        response = await client.post(path, json=payload)
        if async_mode:
            assert response.status_code == 200, response.text
            from celery.result import AsyncResult

            task_id = response.json()["task_id"]

            async def failed():
                task = AsyncResult(task_id, app=plans_celery)
                return task.failed()

            await wait_for(failed)
        else:
            assert response.status_code == 500, response.text
    async with wf() as db:
        weights = list((await db.execute(select(wm.WorkoutSet.working_weight).order_by(wm.WorkoutSet.id))).scalars())
        assert weights == [10, 20]


@pytest.mark.asyncio
async def test_schedule_shift_skips_workout_completed_while_waiting_for_lock(pipeline):
    _, _, wf, _, url = pipeline
    original_date = datetime(2026, 11, 15)
    async with wf() as db:
        w = wm.Workout(
            user_id="pipeline-user", name="Concurrent", applied_plan_id=9, scheduled_for=original_date, status="pending"
        )
        db.add(w)
        await db.commit()
        workout_id = w.id
    async with wf() as completing, httpx.AsyncClient(base_url=url, timeout=20) as client:
        w = (
            await completing.execute(select(wm.Workout).where(wm.Workout.id == workout_id).with_for_update())
        ).scalar_one()
        w.status = "completed"
        await completing.flush()
        request = asyncio.create_task(
            client.post(
                "/workouts/applied-plans/9/shift-schedule",
                json={"from_date": "2026-11-01T00:00:00Z", "days": 5, "only_future": False, "mode": "apply"},
            )
        )
        await asyncio.sleep(0.15)
        assert not request.done(), "Shift must wait for the row lock protecting completion"
        await completing.commit()
        response = await request
        assert response.status_code == 200, response.text
        assert response.json()["workouts_shifted"] == 0
    async with wf() as db:
        w = await db.get(wm.Workout, workout_id)
        assert w.status == "completed" and w.scheduled_for == original_date


@pytest.mark.asyncio
async def test_malformed_generation_success_does_not_commit_partial_plan(pipeline):
    client, pf, wf, state, _ = pipeline
    state["fail_generate"] = 2
    state["malformed_generate"] = True
    response = await client.post("/api/v1/plans/applied-plans/apply/1", params={"user_max_ids": "1"}, json=COMPUTE)
    assert response.status_code == 500
    async with pf() as db:
        assert not list((await db.execute(select(pm.AppliedCalendarPlan))).scalars())
    async with wf() as db:
        assert not list((await db.execute(select(wm.Workout))).scalars())


@pytest.mark.asyncio
async def test_async_application_domain_error_is_failure_not_success(pipeline):
    client, pf, _, _, _ = pipeline
    response = await client.post(
        "/api/v1/plans/applied-plans/apply-async/999", params={"user_max_ids": "1"}, json=COMPUTE
    )
    assert response.status_code == 200, response.text
    task_id = response.json()["task_id"]

    async def terminal():
        data = (await client.get(f"/api/v1/plans/applied-plans/tasks/{task_id}")).json()
        return data if data["status"] in ("SUCCESS", "FAILURE") else None

    result = await wait_for(terminal)
    assert result["status"] == "FAILURE", result
    async with pf() as db:
        assert not list((await db.execute(select(pm.AppliedCalendarPlan))).scalars())


@pytest.mark.asyncio
async def test_confirmed_set_macros_pipeline(pipeline, monkeypatch):
    import json
    from unittest.mock import AsyncMock

    from plans_service.services.applied_calendar_plan_service import AppliedCalendarPlanService
    from workouts_service.services.session_service import SessionService

    client, pf, wf, state, workouts_url = pipeline
    response = await client.post("/api/v1/plans/applied-plans/apply/1", params={"user_max_ids": "1"}, json=COMPUTE)
    assert response.status_code == 200, response.text
    pid = response.json()["id"]
    base = f"/api/v1/plans/applied-plans/{pid}"
    async with wf() as db:
        previous, future = (await db.scalars(select(wm.Workout).order_by(wm.Workout.id))).all()
        previous.readiness_score = 3
        service = SessionService(db, user_id="pipeline-user")
        past_session = await service.start_workout_session(previous.id)
        await service.finish_session(past_session.id)
        for position, eid in enumerate((8, 8, 9)):
            inst = wm.WorkoutExercise(user_id="pipeline-user", workout_id=future.id, exercise_id=eid)
            db.add(inst)
            await db.flush()
            db.add(
                wm.WorkoutSet(
                    exercise_id=inst.id,
                    volume=2,
                    intensity=95,
                    effort=10,
                    working_weight=95,
                    order_index=0,
                    set_type="cluster" if position == 1 else "normal",
                    subsets=[dict(volume=2, intensity=95, effort=10, working_weight=95)] * 2 if position == 1 else None,
                )
            )
        await db.commit()
    async with pf() as db:
        plan = await db.get(pm.AppliedCalendarPlan, pid)
        plan.current_workout_index = 1
        link = await db.scalar(select(pm.AppliedPlanWorkout).where(pm.AppliedPlanWorkout.workout_id == future.id))
        assert link.macro_weight_settings["rounding_mode"] == "nearest"
        link.macro_weight_settings = dict(effective_maxima={"8": 100}, rounding_step=2.5, rounding_mode="nearest")
        for rank, kind in enumerate(("Adjust_Reps", "Adjust_Sets")):
            rule = dict(
                trigger={"metric": "Readiness_Score"},
                condition={"op": "<", "value": 4},
                action={"type": kind, "target": {"exercise_ids": [8]}, "params": {"mode": "by_Value", "value": 1}},
            )
            db.add(pm.PlanMacro(calendar_plan_id=1, name=kind, priority=rank, rule_json=json.dumps(rule)))
        insert_rule = dict(
            trigger={"metric": "Total_Reps"},
            condition={"op": ">", "value": 0},
            action={"type": "Inject_Mesocycle", "params": {"mode": "by_Template", "template_id": 99}},
        )
        db.add(pm.PlanMacro(calendar_plan_id=1, name="insert", priority=5, rule_json=json.dumps(insert_rule)))
        await db.commit()

    async def preview():
        r = await client.post(base + "/run-macros", params={"observation_workout_id": previous.id})
        assert r.status_code == 200, r.text
        return r.json()

    rule_url = "/api/v1/plans/calendar-plans/1/macros/"
    load = {
        "name": "load",
        "rule": {
            **rule,
            "action": {
                "type": "Adjust_Load",
                "target": {"exercise_ids": [8]},
                "params": {"mode": "by_Percent", "value": -120},
            },
        },
    }
    assert (await client.post(rule_url, json=load)).status_code == 422
    load["rule"]["action"]["params"]["value"] = -10
    created = await client.post(rule_url, json=load)
    assert created.status_code == 201, created.text
    load_result = (await preview())["preview"][-1]["patches"]
    assert {p["exercise_id"] for p in load_result} == {8}
    assert len({p["instance_id"] for p in load_result}) == 2
    assert (await client.delete(rule_url + str(created.json()["id"]))).status_code == 204

    insertion = AsyncMock(return_value={"applied": True})
    monkeypatch.setattr(AppliedCalendarPlanService, "inject_mesocycle_into_applied_plan", insertion)
    shown = await preview()
    assert shown["preview"][-1]["plan_changes"] == [insert_rule["action"]]
    separate = await client.post(base + "/apply-macros", json={"mesocycles_only": True})
    assert separate.json()["apply_result"]["applied"] == 0
    assert separate.json()["plan_changes"] == [{"applied": True}]
    insertion.assert_awaited_once()
    insertion.reset_mock()
    patches = shown["preview"][0]["patches"]
    assert len(patches) == 2 and len({p["instance_id"] for p in patches}) == 2
    assert all(
        p["alternatives"] and all(s["working_weight"] == 92.5 for s in p["after"].get("subsets") or [p["after"]])
        for p in patches
    )
    competitor = await preview()
    confirmation = {"preview_id": shown["preview_id"]}
    async with pf() as db:
        (await db.get(pm.AppliedCalendarPlan, pid)).current_workout_index = 2
        await db.commit()
    assert (await client.post(base + "/apply-macros", json={})).status_code == 409
    assert (await client.post(base + "/apply-macros", json={"preview_id": str(uuid.uuid4())})).status_code == 404
    async with httpx.AsyncClient(base_url=workouts_url) as c:
        assert (await c.post(f"/internal/macro-previews/{pid}", json=[])).status_code == 403
    # Значения старых планов не подменяются угаданным шагом или максимумом.
    async with pf() as db:
        (await db.get(pm.AppliedPlanWorkout, link.id)).macro_weight_settings = None
        await db.commit()
    legacy = await preview()
    assert legacy["needs_weight_settings"] == [8] and "preview_id" not in legacy
    recovered = await client.post(
        base + "/run-macros",
        params={"observation_workout_id": previous.id},
        json={"legacy_weights": {"effective_maxima": {"8": 100}, "rounding_step": 5, "rounding_mode": "floor"}},
    )
    assert recovered.status_code == 200 and all(
        s["working_weight"] == 90
        for p in recovered.json()["preview"][0]["patches"]
        for s in p["after"].get("subsets") or [p["after"]]
    )
    async with pf() as db:
        (await db.get(pm.AppliedPlanWorkout, link.id)).macro_weight_settings = dict(
            effective_maxima={"8": 100}, rounding_step=2.5, rounding_mode="nearest"
        )
        await db.commit()
    original = macro_previews.write_effect
    calls = 0

    async def fail_second(*args):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("synthetic second write failure")
        await original(*args)

    monkeypatch.setattr(macro_previews, "write_effect", fail_second)
    assert (await client.post(base + "/apply-macros", json=confirmation)).status_code == 502
    async with wf() as db:
        assert list(await db.scalars(select(wm.WorkoutSet.volume))) == [2, 2, 2]
    monkeypatch.setattr(macro_previews, "write_effect", original)
    responses = await asyncio.gather(*(client.post(base + "/apply-macros", json=confirmation) for _ in range(2)))
    assert all(r.status_code == 200 for r in responses), [r.text for r in responses]
    assert (await client.post(base + "/apply-macros", json={"preview_id": competitor["preview_id"]})).status_code == 409
    async with wf() as db:
        assert len((await db.scalars(select(wm.MacroExecution))).all()) == 2
    insertion.assert_not_awaited()
    assert responses[0].json() == responses[1].json()
    assert responses[0].json()["apply_result"]["applied"] == 2
    readback = (await client.get(f"/api/v1/workouts/{future.id}")).json()["exercise_instances"]
    for instance in readback:
        if instance["exercise_list_id"] == 8:
            sets = instance["sets"]
            assert len(sets) == len({s["id"] for s in sets}) == 2
            if sets[0]["set_type"] == "cluster":
                assert all(s["set_type"] == "cluster" and len(s["subsets"]) == 2 for s in sets)
            assert all(
                (p["reps"], p["intensity"], p["rpe"], p["working_weight"]) == (3, 93, 10, 92.5)
                for s in sets
                for p in s["subsets"] or [s]
            )
        else:
            assert [s["volume"] for s in instance["sets"]] == [2]
    task = await client.post(base + "/apply-macros-async", json=confirmation)
    assert task.status_code in (200, 202), task.text

    async def done():
        r = await client.get("/api/v1/plans/applied-plans/tasks/" + task.json()["task_id"])
        return r.json() if r.json().get("status", "").upper() in ("SUCCESS", "FAILURE") else None

    assert (await wait_for(done))["status"].upper() == "SUCCESS"
    # Новый preview того же наблюдения не повторяет уже выполненные правила.
    repeated = await preview()
    assert repeated["set_changes_count"] == 0 and "preview_id" not in repeated
    async with pf() as db:
        for macro in await db.scalars(
            select(pm.PlanMacro).where(pm.PlanMacro.name.in_(["Adjust_Reps", "Adjust_Sets"]))
        ):
            updated_rule = json.loads(macro.rule_json)
            updated_rule["condition"]["value"] = 5
            macro.rule_json = json.dumps(updated_rule)
        await db.commit()
    # Новая версия правила имеет собственное срабатывание.
    async with wf() as db:
        suggestion = await SessionService(db, user_id="pipeline-user")._compute_macro_suggestion(pid, previous.id)
        assert suggestion["macro_preview"]["preview_id"]
        (await db.get(wm.WorkoutSession, past_session.id)).macro_suggestion = suggestion
        await db.commit()
    suggested = await client.get(f"/api/v1/workouts/sessions/{previous.id}/macro-suggestion")
    assert suggested.status_code == 200 and suggested.json() == suggestion
    flutter_project = os.getenv("CONTRACT_FLUTTER_PROJECT")
    if flutter_project:
        import subprocess

        proc = await asyncio.to_thread(
            subprocess.run,
            [
                "flutter",
                "test",
                "--no-pub",
                "test/macro_confirmation_test.dart",
                "--plain-name",
                "живой gateway",
                "--reporter",
                "expanded",
                f"--dart-define=MACRO_GATEWAY={client.base_url}",
                f"--dart-define=MACRO_WORKOUT={previous.id}",
                f"--dart-define=MACRO_PLAN={pid}",
            ],
            cwd=flutter_project,
            capture_output=True,
            text=True,
            timeout=150,
        )
        assert proc.returncode == 0, proc.stdout + proc.stderr
        async with wf() as db:
            receipt = await db.get(wm.MacroPreview, suggestion["macro_preview"]["preview_id"])
            assert receipt.result["applied"] == 2
            assert len((await db.scalars(select(wm.WorkoutSet))).all()) == 7
    # Доставка послесессионной задачи дважды использует тот же snapshot и observation id.
    from workouts_service.tasks import session_tasks

    monkeypatch.setattr(session_tasks, "AsyncSessionLocal", wf)
    monkeypatch.setattr(SessionService, "_prepare_user_max_payload", AsyncMock(return_value=None))
    monkeypatch.setattr(SessionService, "_post_social_workout_completion", AsyncMock())
    for _ in range(2):
        processed = session_tasks.finish_session_postprocess_task.apply_async(
            kwargs={"session_id": past_session.id, "user_id": "pipeline-user"}
        )
        result = await asyncio.to_thread(processed.get, timeout=15, disable_sync_subtasks=False)
        assert result["ok"]
    async with pf() as db:
        observed = await db.scalar(select(pm.AppliedPlanWorkout).where(pm.AppliedPlanWorkout.workout_id == previous.id))
        assert observed.session_advanced
    async with wf() as db:
        saved = await db.get(wm.WorkoutSession, past_session.id)
        assert saved.macro_suggestion["macro_preview"]["preview_id"] == suggestion["macro_preview"]["preview_id"]
    state["user"] = "other-owner"
    assert (await client.post(base + "/apply-macros", json=confirmation)).status_code == 404
    task = await client.post(base + "/apply-macros-async", json=confirmation)
    assert task.status_code in (200, 202)
    assert (await wait_for(done))["status"].upper() == "FAILURE"
    state.pop("user")
    assert (await client.post(f"/api/v1/workouts/internal/macro-previews/{pid}", json=[])).status_code == 404
    async with pf() as db:
        for macro in await db.scalars(
            select(pm.PlanMacro).where(pm.PlanMacro.name.in_(["Adjust_Reps", "Adjust_Sets"]))
        ):
            updated_rule = json.loads(macro.rule_json)
            updated_rule["condition"]["value"] = 6
            macro.rule_json = json.dumps(updated_rule)
        await db.commit()
    new_preview = await preview()
    async with wf() as db:
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as denied:
            await macro_previews.confirm(pid, new_preview["preview_id"], db, "another-user")
        assert denied.value.status_code == 404
    async with wf() as db:
        with pytest.raises(HTTPException) as denied:
            await macro_previews.confirm(pid + 1, new_preview["preview_id"], db, "pipeline-user")
        assert denied.value.status_code == 404
    async with wf() as db:
        row = await db.scalar(select(wm.WorkoutSet).order_by(wm.WorkoutSet.id))
        row.volume += 1
        await db.commit()
    assert (
        await client.post(base + "/apply-macros", json={"preview_id": new_preview["preview_id"]})
    ).status_code == 409
    fresh = await preview()
    async with wf() as db:
        session = await SessionService(db, user_id="pipeline-user").start_workout_session(future.id)
    assert (await client.post(base + "/apply-macros", json={"preview_id": fresh["preview_id"]})).status_code == 409
    async with wf() as db:
        row = await db.scalar(select(wm.WorkoutSet).order_by(wm.WorkoutSet.id))
        before = row.volume
        row.volume += 1
        await db.commit()
        service = SessionService(db, user_id="pipeline-user")
        await service.complete_all_sets(session.id, True)
        finished = await service.finish_session(session.id)
        iid, sid = str(row.exercise_id), str(row.id)
        assert row.id in finished.progress["completed"][iid]
        assert finished.progress["macro_baseline"][iid][sid]["volume"] == before
        assert finished.progress["macro_actuals"][iid][sid]["volume"] == before + 1


@pytest.mark.asyncio
async def test_macro_weights_follow_microcycle_normalization(pipeline, monkeypatch):
    from unittest.mock import AsyncMock

    client, pf, _, _, _ = pipeline
    async with pf() as db:
        first = await db.scalar(select(pm.Microcycle))
        first.days_count, first.normalization_value, first.normalization_unit = 1, 10, "percentage"
        second = pm.Microcycle(mesocycle_id=first.mesocycle_id, name="MC2", order_index=1, days_count=1)
        db.add(second)
        await db.flush()
        workouts = list(await db.scalars(select(pm.PlanWorkout).order_by(pm.PlanWorkout.order_index)))
        workouts[1].microcycle_id, workouts[1].day_label = second.id, "Day 1"
        workouts[1].updated_at = datetime(2026, 11, 15)
        for workout in workouts:
            ex = pm.PlanExercise(
                plan_workout_id=workout.id,
                exercise_definition_id=8,
                exercise_name="X",
                order_index=0,
                created_at=datetime(2026, 11, 15),
                updated_at=datetime(2026, 11, 15),
            )
            db.add(ex)
            await db.flush()
            db.add(
                pm.PlanSet(
                    plan_exercise_id=ex.id,
                    set_type="drop",
                    order_index=0,
                    subsets=[dict(intensity=95, effort=10, volume=2), dict(intensity=80, rpe=8, reps=6)],
                    created_at=datetime(2026, 11, 15),
                    updated_at=datetime(2026, 11, 15),
                )
            )
        await db.commit()
    monkeypatch.setattr(
        AppliedCalendarPlanService,
        "_fetch_user_maxes_by_ids",
        AsyncMock(return_value=[dict(id=1, exercise_id=8, max_weight=100, rep_max=1)]),
    )
    monkeypatch.setattr(WorkoutCalculator, "get_true_1rm_from_user_max", AsyncMock(return_value=100))
    response = await client.post(
        "/api/v1/plans/applied-plans/apply/1", params={"user_max_ids": "1"}, json={**COMPUTE, "compute_weights": True}
    )
    assert response.status_code == 200, response.text
    async with pf() as db:
        linked = list(await db.scalars(select(pm.AppliedPlanWorkout).order_by(pm.AppliedPlanWorkout.order_index)))
        assert [w.macro_weight_settings["effective_maxima"]["8"] for w in linked] == pytest.approx([100, 110])
        for workout, weights in zip(linked, ([95, 80], [105, 87.5]), strict=True):
            response = await client.get(f"/api/v1/workouts/{workout.workout_id}")
            (instance,) = response.json()["exercise_instances"]
            (compound,) = instance["sets"]
            assert compound["set_type"] == "drop"
            assert [part["reps"] for part in compound["subsets"]] == [2, 6]
            assert [part["working_weight"] for part in compound["subsets"]] == weights


@pytest.mark.asyncio
async def test_macro_migrations_on_existing_data():
    from importlib.util import module_from_spec, spec_from_file_location
    from pathlib import Path

    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    pg = os.getenv("CONTRACT_TEST_POSTGRES_URL")
    if not pg:
        pytest.skip("Isolated PostgreSQL URL required")
    schema = "macro_migrate_" + uuid.uuid4().hex
    engine = create_async_engine(pg, poolclass=NullPool)
    root = Path(__file__).resolve().parents[2] / "services"
    async with engine.connect() as connection:
        txn = await connection.begin()
        try:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await connection.execute(text(f'SET LOCAL search_path TO "{schema}"'))
            await connection.execute(text("CREATE TABLE applied_plan_workouts (id INTEGER PRIMARY KEY)"))
            await connection.execute(text("INSERT INTO applied_plan_workouts VALUES (1)"))

            def migrate(conn):
                with Operations.context(MigrationContext.configure(conn)):
                    for filename in (
                        "plans-service/alembic/versions/20261115_macro_weight_settings.py",
                        "plans-service/alembic/versions/20261115_macro_session_once.py",
                        "workouts-service/alembic/versions/20261115_macro_previews.py",
                        "workouts-service/alembic/versions/20261115_macro_executions.py",
                    ):
                        source = spec_from_file_location("migration", root / filename)
                        module = module_from_spec(source)
                        source.loader.exec_module(module)
                        module.upgrade()

            await connection.run_sync(migrate)
            assert (
                await connection.execute(text("SELECT id,macro_weight_settings FROM applied_plan_workouts"))
            ).one() == (1, None)
            assert (await connection.execute(text("SELECT count(*) FROM macro_previews"))).scalar_one() == 0
            assert (await connection.execute(text("SELECT count(*) FROM macro_executions"))).scalar_one() == 0
            assert (
                await connection.execute(text("SELECT session_advanced FROM applied_plan_workouts"))
            ).scalar_one() is False
        finally:
            await txn.rollback()
    await engine.dispose()


@pytest.mark.asyncio
async def test_macro_and_editor_share_workout_lock(pipeline):
    from fastapi import HTTPException
    from workouts_service.services.exercise_instance_service import ExerciseInstanceService

    _, _, wf, _, workouts_url = pipeline
    async with wf() as db:
        workout = wm.Workout(user_id="pipeline-user", name="Lock", applied_plan_id=1)
        db.add(workout)
        await db.flush()
        instance = wm.WorkoutExercise(workout_id=workout.id, user_id="pipeline-user", exercise_id=8)
        db.add(instance)
        await db.flush()
        row = wm.WorkoutSet(exercise_id=instance.id, volume=6, intensity=80, effort=8, working_weight=80)
        db.add(row)
        await db.flush()
        from workouts_service.schemas.workout import WorkoutSetResponse

        before = WorkoutSetResponse.model_validate(row).model_dump()
        after = {**before, "volume": 7, "effort": 9}
        effect = dict(
            workout_id=workout.id,
            instance_id=instance.id,
            exercise_id=8,
            before=[before],
            after=[after],
            rule_keys=["synthetic:observation:version"],
        )
        preview = wm.MacroPreview(id=str(uuid.uuid4()), user_id="pipeline-user", plan_id=1, effects=[effect])
        db.add(preview)
        await db.commit()
    async with wf() as editor_db:
        # Редактор держит блокировку до commit, исполнитель обязан дождаться результата.
        service = ExerciseInstanceService(editor_db, "pipeline-user")
        await service.update_set(instance.id, row.id, {"volume": 9}, commit=False)

        async def confirm():
            async with wf() as db:
                return await macro_previews.confirm(1, preview.id, db, "pipeline-user")

        task = asyncio.create_task(confirm())
        await asyncio.sleep(0.1)
        assert not task.done()
        await editor_db.commit()
        with pytest.raises(HTTPException) as error:
            await task
        assert error.value.status_code == 409
    async with wf() as db:
        assert (await db.get(wm.WorkoutSet, row.id)).volume == 9
        assert (await db.get(wm.MacroPreview, preview.id)).result is None

    async with wf() as locked_db:
        await locked_db.scalar(select(wm.Workout).where(wm.Workout.id == workout.id).with_for_update())

        async def edit():
            async with wf() as db:
                return await ExerciseInstanceService(db, "pipeline-user").update_set(
                    instance.id, row.id, {"volume": 10}
                )

        task = asyncio.create_task(edit())
        await asyncio.sleep(0.1)
        assert not task.done()
        await locked_db.commit()
        await task
    async with wf() as db:
        assert (await db.get(wm.WorkoutSet, row.id)).volume == 10

    legacy_effect = {key: value for key, value in effect.items() if key != "rule_keys"}
    async with httpx.AsyncClient(base_url=workouts_url) as client:
        rejected = await client.post(
            "/internal/macro-previews/1",
            json=[legacy_effect],
            headers={"X-Internal-Secret": "pipeline-secret", "X-User-Id": "pipeline-user"},
        )
        assert rejected.status_code == 409
    legacy_id = str(uuid.uuid4())
    async with wf() as db:
        db.add(wm.MacroPreview(id=legacy_id, user_id="pipeline-user", plan_id=1, effects=[legacy_effect]))
        await db.commit()
    async with wf() as db:
        with pytest.raises(HTTPException) as error:
            await macro_previews.confirm(1, legacy_id, db, "pipeline-user")
        assert error.value.status_code == 409
    receipt = {"applied": 1, "status": "applied", "errors": [], "details": []}
    async with wf() as db:
        (await db.get(wm.MacroPreview, legacy_id)).result = receipt
        await db.commit()
    async with wf() as db:
        assert await macro_previews.confirm(1, legacy_id, db, "pipeline-user") == receipt
        assert (await db.get(wm.WorkoutSet, row.id)).volume == 10


@pytest.mark.asyncio
async def test_session_index_advances_once_through_gateway(pipeline):
    client, pf, _, _, _ = pipeline
    response = await client.post("/api/v1/plans/applied-plans/apply/1", params={"user_max_ids": "1"}, json=COMPUTE)
    assert response.status_code == 200
    pid = response.json()["id"]
    async with pf() as db:
        link = await db.scalar(select(pm.AppliedPlanWorkout).order_by(pm.AppliedPlanWorkout.order_index))
    url = f"/api/v1/plans/applied-plans/{pid}/advance-index"
    results = await asyncio.gather(
        *(client.post(url, params={"observation_workout_id": link.workout_id}) for _ in range(2))
    )
    assert all(r.status_code == 200 and r.json()["current_workout_index"] == 1 for r in results)
    async with pf() as db:
        assert (await db.get(pm.AppliedPlanWorkout, link.id)).session_advanced
        assert (await db.get(pm.AppliedCalendarPlan, pid)).current_workout_index == 1
