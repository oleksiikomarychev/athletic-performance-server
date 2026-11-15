from __future__ import annotations

import asyncio
import concurrent.futures
from typing import Any

from celery import shared_task
from celery.utils.log import get_task_logger
from fastapi import HTTPException

from ..celery_app import PLANS_TASK_QUEUE
from ..database import DATABASE_URL
from ..schemas.calendar_plan import ApplyPlanComputeSettings
from ..services.applied_calendar_plan_service import AppliedCalendarPlanService
from ..services.macro_apply import apply_macros

logger = get_task_logger(__name__)


def _run_async(coro_or_func):
    if asyncio.iscoroutinefunction(coro_or_func):
        coro = coro_or_func()
    else:
        coro = coro_or_func

    def run_in_new_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            try:
                pending = asyncio.all_tasks(loop)
                if pending:
                    for task in pending:
                        task.cancel()
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            finally:
                loop.close()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(run_in_new_loop)
        return future.result()


def _apply_plan_sync(
    plan_id: int,
    user_id: str,
    compute_data: dict[str, Any],
    user_max_ids: list[int],
    *,
    replace_active: bool = False,
    progress: Any | None = None,
) -> dict[str, Any]:
    async def _run_async_session():
        from backend_common.database import create_async_engine_and_session

        task_engine, TaskAsyncSessionLocal = create_async_engine_and_session(
            DATABASE_URL,
            pool_size=5,
            max_overflow=5,
            pool_pre_ping=False,
            pool_recycle=3600,
        )

        async with TaskAsyncSessionLocal() as session:
            service = AppliedCalendarPlanService(session, user_id)
            compute = ApplyPlanComputeSettings.model_validate(compute_data)
            try:
                result = await service.apply_plan(
                    plan_id, compute, user_max_ids, replace_active=replace_active, progress=progress
                )
                if hasattr(result, "model_dump"):
                    return result.model_dump()
                return result
            except HTTPException as exc:
                raise ValueError(f"{exc.status_code}: {exc.detail}") from exc
            finally:
                await task_engine.dispose()

    return _run_async(_run_async_session)


@shared_task(
    bind=True,
    name="plans.cleanup_failed_application",
    queue=PLANS_TASK_QUEUE,
    max_retries=8,
)
def cleanup_failed_application_task(self, *, user_id: str, applied_plan_id: int) -> None:
    async def cleanup() -> None:
        service = AppliedCalendarPlanService(None, user_id)
        await service._cleanup_failed_application(applied_plan_id)

    try:
        _run_async(cleanup)
    except Exception as exc:
        logger.exception("cleanup_failed_application_retry applied_plan_id=%s", applied_plan_id)
        raise self.retry(exc=exc, countdown=60)


async def _apply_macros_async(
    applied_plan_id: int,
    user_id: str,
    index_offset: int,
    preview_id: str | None = None,
    mesocycles_only: bool = False,
) -> dict[str, Any]:
    from backend_common.database import create_async_engine_and_session

    task_engine, TaskAsyncSessionLocal = create_async_engine_and_session(
        DATABASE_URL,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=False,
        pool_recycle=3600,
    )

    try:
        async with TaskAsyncSessionLocal() as session:
            return await apply_macros(session, user_id, applied_plan_id, preview_id, index_offset, mesocycles_only)
    finally:
        await task_engine.dispose()


@shared_task(
    bind=True,
    name="plans.apply_plan",
    queue=PLANS_TASK_QUEUE,
    max_retries=2,
)
def apply_plan_task(
    self,
    *,
    plan_id: int,
    user_id: str,
    compute: dict[str, Any],
    user_max_ids: list[int],
    replace_active: bool = False,
) -> dict[str, Any]:
    try:

        def _progress(meta: dict[str, Any]) -> None:
            try:
                self.update_state(state="PROGRESS", meta=meta)
            except Exception:
                # Progress reporting must not break the task
                return

        return _apply_plan_sync(
            plan_id=plan_id,
            user_id=user_id,
            compute_data=compute,
            user_max_ids=user_max_ids,
            replace_active=replace_active,
            progress=_progress,
        )
    except ValueError:
        # Domain errors (including a conflict after enqueue) are terminal failures.
        raise
    except Exception as exc:
        logger.exception("apply_plan_task_failed", exc_info=exc)
        raise self.retry(exc=exc, countdown=60)


@shared_task(
    bind=True,
    name="plans.apply_macros",
    queue=PLANS_TASK_QUEUE,
    max_retries=2,
)
def apply_plan_macros_task(
    self,
    *,
    applied_plan_id: int,
    user_id: str,
    index_offset: int,
    preview_id: str | None = None,
    mesocycles_only: bool = False,
) -> dict[str, Any]:
    try:
        return _run_async(
            _apply_macros_async(
                applied_plan_id=applied_plan_id,
                user_id=user_id,
                index_offset=index_offset,
                preview_id=preview_id,
                mesocycles_only=mesocycles_only,
            )
        )
    except HTTPException as exc:
        if exc.status_code < 500 or not preview_id:
            raise ValueError(exc.detail) from exc
        raise self.retry(exc=exc, countdown=60)
    except Exception as exc:
        logger.exception("apply_plan_macros_task_failed", exc_info=exc)
        raise self.retry(exc=exc, countdown=60)
