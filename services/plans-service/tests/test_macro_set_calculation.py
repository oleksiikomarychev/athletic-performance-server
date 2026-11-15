import json
from pathlib import Path

import pytest
from plans_service.services.macro_set_calculation import calculate_set, table_rows


@pytest.fixture(scope="module")
def rows():
    path = Path(__file__).resolve().parents[2] / "rpe-service" / "rpe_table.json"
    return table_rows(json.loads(path.read_text()))


def test_entire_table_has_no_drift_at_target(rows):
    for intensity, effort, volume in rows:
        before = dict(volume=volume, intensity=intensity, effort=effort, working_weight=80)
        for action in ("Adjust_Load", "Adjust_Reps"):
            after, alternatives = calculate_set(before, action, "to_Target", effort, rows, None)
            assert after == before
            assert not alternatives


def test_unreachable_pair_is_presented_as_alternative(rows):
    before = dict(volume=2, intensity=95, effort=10, working_weight=95)
    after, alternatives = calculate_set(before, "Adjust_Reps", "by_Value", 1, rows, (100, 2.5, "nearest"))
    assert (after["volume"], after["intensity"], after["effort"]) == (3, 93, 10)
    assert after["working_weight"] == 92.5
    assert alternatives[0]["requested"] == {"volume": 3, "intensity": 95}
    assert before["volume"] == 2


@pytest.mark.parametrize(("mode", "expected"), [("floor", 92.5), ("nearest", 92.5), ("ceil", 95)])
def test_rounding_modes(rows, mode, expected):
    before = dict(volume=2, intensity=95, effort=10, working_weight=95)
    result, _ = calculate_set(before, "Adjust_Reps", "by_Value", 1, rows, (100, 2.5, mode))
    assert result["working_weight"] == expected


@pytest.mark.parametrize("value", [-120, float("nan"), "bad", True])
def test_reject_invalid_percent(value):
    from plans_service.schemas.macro import MacroAction

    with pytest.raises(ValueError):
        MacroAction(type="Adjust_Load", params={"mode": "by_Percent", "value": value})


@pytest.mark.asyncio
async def test_validate_existing_rules_and_ai_prompt(monkeypatch):
    from unittest.mock import AsyncMock

    from agent_service.services import macros_manager
    from plans_service.schemas.macro import MacroAction

    assert MacroAction(type="Adjust_Load", params={"mode": "by_Percent", "value": 0.90}).params["value"] == 0.90
    with pytest.raises(ValueError):
        MacroAction(type="Adjust_Sets", params={"mode": "by_Value", "value": 1.3})
    mock = AsyncMock()
    monkeypatch.setattr(macros_manager, "generate_structured_output", mock)
    await macros_manager.generate_macro_payload("lower by 10%", {"test": 8})
    prompt = mock.await_args.kwargs["prompt"]
    assert '"value": -10' in prompt and '"test": 8' in prompt and "lower by 10%" in prompt


@pytest.mark.asyncio
async def test_tag_filter_pages_empty_failure_recovery():
    import httpx
    from fastapi import HTTPException
    from plans_service.services.macro_engine import MacroEngine

    eng = MacroEngine(None, "u")
    target = {"selector": {"value": {"equipment": ["barbell"]}}}
    offsets = []
    responses = iter(
        [
            httpx.Response(200, json=[]),
            httpx.Response(503),
            httpx.Response(200, json=[{"id": i, "equipment": "other"} for i in range(50)]),
            httpx.Response(200, json=[{"id": 51, "equipment": "Barbell"}]),
        ]
    )

    def respond(request):
        offsets.append(request.url.params["offset"])
        return next(responses)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as eng.client:
        assert not await eng._matches(
            dict(trigger={"metric": "Total_Reps", **target}, condition={"op": "<", "value": 1}), [1]
        )
        eng.selector_ids.clear()  # Новый расчёт после восстановления источника.
        with pytest.raises(HTTPException) as error:
            await eng._ids(target)
        assert error.value.status_code == 502
        assert await eng._ids(target) == {51}
        assert offsets == ["0", "0", "0", "50"]


@pytest.mark.asyncio
async def test_real_wire_total_reps_and_range_history():
    from unittest.mock import AsyncMock

    from plans_service.services.macro_engine import MacroEngine
    from workouts_service.schemas.workout import WorkoutExerciseResponse

    inst = WorkoutExerciseResponse(id=2, exercise_id=8, sets=[dict(id=1, volume=5), dict(id=2, volume=5)])
    eng = MacroEngine(None, "u")
    eng.history = {1: {"progress": {}}, 2: {"progress": {}}}
    eng._get = AsyncMock(
        side_effect=[{"id": wid, "exercise_instances": [inst.model_dump(by_alias=True)]} for wid in (2, 1)]
    )
    r = dict(trigger={"metric": "Total_Reps"}, condition={"op": "<", "value": 1})
    assert not await eng._matches(r, [2])
    r["condition"] = {"op": "holds_for", "n": 2, "relation": "in_range", "range": [9, 11]}
    assert await eng._matches(r, [1, 2])
    assert not await eng._matches(r, [])


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["drop", "rest_pause", "cluster"])
async def test_clone_full_composite_without_rpe_service(kind):
    from plans_service.services.macro_engine import MacroEngine

    eng = MacroEngine(None, "u")
    inst = dict(
        id=7,
        exercise_list_id=8,
        sets=[
            dict(id=21, order_index=0, set_type=kind, subsets=[dict(volume=5, weight=40), dict(volume=4, weight=35)])
        ],
    )
    (p,) = await eng._changes(dict(type="Adjust_Sets", params={"value": 1}), 1, inst, {})
    assert p["after"]["id"] != 21
    assert p["after"]["set_type"] == kind and p["after"]["subsets"] == inst["sets"][0]["subsets"]
    assert p["after"]["subsets"] is not inst["sets"][0]["subsets"]


@pytest.mark.asyncio
async def test_delta_uses_completed_set_identity_and_reports_missing_baseline():
    from plans_service.services.macro_engine import MacroEngine

    eng = MacroEngine(None, "u")
    eng.workouts[1] = {
        "exercise_instances": [dict(id=4, exercise_list_id=8, sets=[dict(id=3, volume=6), dict(id=2, volume=8)])]
    }
    progress = {
        "completed": {"4": [2]},
        "macro_baseline": {"4": {"2": {"volume": 5}}},
        "macro_actuals": {"4": {"2": {"volume": 7}}},
    }
    eng.history[1] = {"progress": progress}
    assert await eng._metric("Reps_Delta_From_Plan", 1, {8}) == [None, 2, None]
    progress.pop("macro_baseline")
    with pytest.raises(ValueError, match="снимок"):
        await eng._metric("Reps_Delta_From_Plan", 1, {8})


def test_generated_changes_always_select_an_admissible_row(rows):
    allowed = set(rows)
    for i, e, v in rows:
        source = dict(volume=v, intensity=i, effort=e, working_weight=i)
        for action, mode, value in [
            ("Adjust_Reps", "by_Value", 1),
            ("Adjust_Load", "by_Percent", -10),
            ("Adjust_Reps", "to_Target", 7.5),
            ("Adjust_Load", "to_Target", 8),
        ]:
            after, alternatives = calculate_set(source, action, mode, value, rows, (100, 1, "nearest"))
            triple = (after["intensity"], after["effort"], after["volume"])
            assert triple in allowed
            assert after["working_weight"] == after["intensity"]
            assert all(item["requested"] != item["proposed"] for item in alternatives)


@pytest.mark.parametrize("weight", [-5, 40, 999])
def test_inconsistent_weight_is_rejected_before_change(rows, weight):
    before = dict(volume=6, intensity=80, effort=8, working_weight=weight)
    with pytest.raises(ValueError, match="вес"):
        calculate_set(before, "Adjust_Load", "by_Percent", -10, rows, (100, 2.5, "nearest"))
    assert before["working_weight"] == weight


@pytest.mark.parametrize("value", [101, -101, 100000000000000000001, 1e308, "NaN", "1.2"])
def test_integer_budget_is_checked_without_allocating_sets(value):
    from plans_service.schemas.macro import MacroAction

    with pytest.raises(ValueError):
        MacroAction(type="Adjust_Sets", params={"mode": "by_Value", "value": value})


def test_all_ordered_action_pairs_preserve_invariants(rows):
    from copy import deepcopy
    from itertools import product

    from backend_common.workout_sets import SetSnapshot

    actions = [
        ("Adjust_Reps", "by_Value", 1),
        ("Adjust_Load", "by_Percent", -10),
        ("Adjust_Load", "to_Target", 7),
        ("Adjust_Reps", "to_Target", 9),
    ]
    allowed = set(rows)
    for i, e, v in rows:
        source = dict(intensity=i, effort=e, volume=v, working_weight=i)
        for first, second in product(actions, repeat=2):
            original = deepcopy(source)
            middle, _ = calculate_set(source, *first, rows, (100, 1, "nearest"))
            result, _ = calculate_set(middle, *second, rows, (100, 1, "nearest"))
            snapshot = SetSnapshot.parse(result)
            assert (snapshot.intensity, snapshot.effort, snapshot.volume) in allowed
            assert snapshot.weight == snapshot.intensity
            assert source == original


@pytest.mark.asyncio
async def test_completed_fact_and_composite_set_boundaries():
    from plans_service.schemas.macro import MacroRule
    from plans_service.services.macro_engine import MacroEngine

    eng = MacroEngine(None, "u")
    eng.workouts[1] = {"exercise_instances": [dict(id=4, exercise_list_id=8, sets=[dict(id=2, volume=99)])]}
    eng.history[1] = {
        "progress": {
            "completed": {"4": [2]},
            "macro_exercises": {"4": 8},
            "macro_baseline": {
                "4": {"2": {"subsets": [dict(volume=5, macro_part_id="a"), dict(volume=5, macro_part_id="b")]}}
            },
            "macro_actuals": {
                "4": {"2": {"subsets": [dict(volume=6, macro_part_id="a"), dict(volume=6, macro_part_id="b")]}}
            },
        }
    }
    eng.history[1]["progress"]["macro_completed"] = {"4": [2]}
    eng.history[1]["progress"]["macro_metrics"] = {"Readiness_Score": 3}
    eng.history[1]["progress"]["completed"] = {}
    assert await eng._metric("Total_Reps", 1, {8}) == 12
    assert await eng._metric("Readiness_Score", 1, None) == 3
    rule = MacroRule.model_validate(
        dict(
            trigger={"metric": "Reps_Delta_From_Plan", "exercise_ids": [8]},
            condition={"op": "holds_for_sets", "n_sets": 2, "relation": "in_range", "range": [1, 2]},
            action={"type": "Adjust_Sets", "params": {"mode": "by_Value", "value": 1}},
        )
    ).model_dump(mode="json", exclude_none=True)
    assert not await eng._matches(rule, [1])
    rule["condition"]["n_sets"] = 1
    assert await eng._matches(rule, [1])
    eng.history[1]["progress"]["macro_actuals"]["4"]["2"]["subsets"].reverse()
    with pytest.raises(ValueError, match="идентичность"):
        await eng._metric("Reps_Delta_From_Plan", 1, {8})


def test_aliases_and_nested_structure_are_validated():
    from backend_common.workout_sets import SetSnapshot

    for source in [
        dict(volume=6, reps=7),
        dict(effort=float("nan")),
        dict(volume=True),
        dict(subsets=[dict(subsets=[dict(volume=1)])]),
    ]:
        with pytest.raises(ValueError):
            SetSnapshot.parse(source)


def test_owner_rejects_invalid_identity_and_values():
    from workouts_service.routers.macro_previews import InstanceChange

    base = dict(workout_id=1, instance_id=2, exercise_id=8, before=[dict(id=3, volume=6)])
    for after in [[dict(id=4, volume=6)], [dict(id=3, volume=6)] * 2, [dict(id=3, working_weight=-5)]]:
        with pytest.raises(ValueError):
            InstanceChange(**base, after=after)


@pytest.mark.asyncio
async def test_history_window_skips_unfinished_observation():
    from plans_service.services.macro_engine import MacroEngine

    eng = MacroEngine(None, "u")
    eng.workouts = {i: {"readiness_score": 3, "exercise_instances": []} for i in (1, 2, 3)}
    eng.history = {1: {"progress": {}}, 2: None, 3: {"progress": {}}}
    rule = dict(
        trigger={"metric": "Readiness_Score"}, condition={"op": "holds_for", "n": 2, "relation": "<", "value": 4}
    )
    assert await eng._matches(rule, [1, 2, 3])


@pytest.mark.asyncio
async def test_set_count_budget_and_delete_then_add_fail_safely():
    from plans_service.services.macro_engine import MacroEngine

    eng = MacroEngine(None, "u")
    inst = dict(id=7, exercise_list_id=8, sets=[dict(id=21, volume=6, order_index=0)])
    await eng._changes(dict(type="Adjust_Sets", params={"value": -1}), 1, inst, {})
    with pytest.raises(ValueError, match="образца"):
        await eng._changes(dict(type="Adjust_Sets", params={"value": 1}), 1, inst, {})
    assert len(inst["sets"]) == 1
    eng = MacroEngine(None, "u")
    inst["sets"] = [dict(id=i, volume=6, order_index=i) for i in range(1000)]
    with pytest.raises(ValueError, match="слишком много"):
        await eng._changes(dict(type="Adjust_Sets", params={"value": 1}), 1, inst, {})
    assert len(eng.working_sets[(1, 7)]) == 1000
