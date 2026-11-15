"""Расчёт полного подхода на дискретной таблице RPE."""

import math
from copy import deepcopy
from dataclasses import dataclass
from typing import Literal

from backend_common.workout_sets import SetSnapshot, integer_change

Row = tuple[int, int, int]
WeightSettings = tuple[float, float, str]


@dataclass(frozen=True)
class WeightContext:
    maximum: float
    step: float
    mode: Literal["floor", "ceil", "nearest"]

    def __post_init__(self):
        if not all(math.isfinite(v) for v in (self.maximum, self.step)) or self.maximum <= 0 or self.step < 0:
            raise ValueError("Некорректные настройки расчёта веса")
        if self.mode not in {"floor", "ceil", "nearest"}:
            raise ValueError("Неизвестный режим округления")

    def weight(self, intensity):
        raw = self.maximum * (intensity / 100.0)
        ratio = raw / self.step if self.step else raw
        if not math.isfinite(ratio):
            raise ValueError("Результат расчёта веса выходит за числовой диапазон")
        rounding = {"floor": math.floor, "ceil": math.ceil, "nearest": round}[self.mode]
        return rounding(ratio) * self.step if self.step else raw


@dataclass(frozen=True)
class SetAction:
    kind: Literal["Adjust_Load", "Adjust_Reps"]
    mode: Literal["by_Percent", "by_Value", "to_Target"]
    value: float

    def __post_init__(self):
        supported = {"Adjust_Load": {"by_Percent", "to_Target"}, "Adjust_Reps": {"by_Value", "to_Target"}}
        if self.mode not in supported.get(self.kind, set()):
            raise ValueError("Недопустимое действие над подходом")
        if isinstance(self.value, bool) or not math.isfinite(self.value):
            raise ValueError("Значение действия должно быть конечным числом")
        if self.mode == "by_Value":
            integer_change(self.value)
        if self.mode == "by_Percent" and self.value <= -100:
            raise ValueError("Изменение нагрузки должно быть больше -100%")
        if self.mode == "to_Target" and not 1 <= self.value <= 10:
            raise ValueError("Целевой RPE должен быть от 1 до 10")


class MissingWeightSettings(ValueError):
    """Для исторического плана нужны явные настройки веса."""


def table_rows(table: dict) -> list[Row]:
    if not isinstance(table, dict) or not table:
        raise ValueError("Таблица RPE недоступна")
    if any(not isinstance(efforts, dict) or not efforts for efforts in table.values()):
        raise ValueError("Неверный формат строки таблицы RPE")
    rows = [(int(i), int(e), v) for i, efforts in table.items() for e, v in efforts.items()]
    if not rows or any(not (0 < i <= 100 and 1 <= e <= 10 and type(v) is int and v > 0) for i, e, v in rows):
        raise ValueError("Некорректные значения таблицы RPE")
    return rows


def working_weight(intensity: float, settings: WeightSettings) -> float:
    return WeightContext(*settings).weight(intensity)


def calculate_set(
    before: dict, action: str, mode: str, value: float, rows: list[Row], settings: WeightSettings | None
) -> tuple[dict, list[dict]]:
    intent = SetAction(action, mode, value)
    snapshot = SetSnapshot.parse(before)
    context = WeightContext(*settings) if settings else None
    after = deepcopy(before)
    if snapshot.parts:
        alternatives = []
        for index, part in enumerate(before["subsets"]):
            result, suggested = calculate_set(part, action, mode, value, rows, settings)
            after["subsets"][index] = result
            alternatives.extend(dict(item, subset=index + 1) for item in suggested)
        return after, alternatives

    intensity, effort, volume = snapshot.intensity, snapshot.effort, snapshot.volume
    if intensity is None or volume is None:
        raise ValueError("Для расчёта нужны интенсивность и повторы")
    if (
        context
        and snapshot.weight is not None
        and not math.isclose(snapshot.weight, context.weight(intensity), rel_tol=1e-9, abs_tol=1e-7)
    ):
        raise ValueError("Рабочий вес расходится с интенсивностью и максимумом. Уточните параметры подхода")
    baseline = dict(intensity=intensity, effort=effort, volume=volume)
    if mode == "to_Target":
        fixed = "volume" if action == "Adjust_Load" else "intensity"
        requested = {"effort": value, fixed: baseline[fixed]}
        priority = ["effort", fixed]
    elif action == "Adjust_Reps":
        requested = {"volume": volume + integer_change(intent.value), "intensity": intensity}
        if requested["volume"] < 1:
            raise ValueError("В подходе должен остаться хотя бы один повтор")
        priority = ["volume", "intensity"]
    else:
        requested = {"intensity": intensity * (1 + value / 100), "volume": volume}
        priority = ["volume", "intensity"]

    # Сначала явная цель и фиксированная величина, затем наименьший дрейф
    # остальных полей. Последние три ключа разрешают равенство независимо от порядка JSON.
    def distance(row: Row) -> tuple:
        candidate = dict(zip(("intensity", "effort", "volume"), row, strict=True))
        return (
            *(abs(candidate[key] - requested[key]) for key in priority),
            abs(row[0] - intensity),
            abs(row[1] - effort) if effort is not None else 0,
            abs(row[2] - volume),
            *row,
        )

    if not rows:
        raise ValueError("Таблица RPE пуста")
    selected = min(rows, key=distance)
    result = dict(zip(("intensity", "effort", "volume"), selected, strict=True))
    after.update(result)
    for alias, field in (("reps", "volume"), ("rpe", "effort")):
        if alias in before:
            after[alias] = result[field]
    if selected[0] != intensity:
        if settings:
            weight = working_weight(selected[0], settings)
            after["working_weight"] = weight
            if "weight" in before:
                after["weight"] = weight
        elif before.get("working_weight") is not None or before.get("weight") is not None:
            raise MissingWeightSettings("Для изменения рабочего веса нужны исходный 1ПМ и настройки округления плана")
    result_snapshot = SetSnapshot.parse(after)
    if (result_snapshot.intensity, result_snapshot.effort, result_snapshot.volume) not in rows:
        raise ValueError("Результат отсутствует в таблице RPE")
    approximate = any(not math.isclose(result[k], v, abs_tol=1e-9) for k, v in requested.items())
    return after, [{"requested": requested, "proposed": result}] if approximate else []
