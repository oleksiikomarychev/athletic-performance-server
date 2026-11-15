"""Проверяемые снимки подходов на границах вычисления и записи."""

import math
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

MAX_SET_CHANGE = 100
MAX_SET_PARTS = 1000


def integer_change(value) -> int:
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("Ожидается целое смещение повторов или подходов") from exc
    if not number.is_finite() or number != number.to_integral_value() or abs(number) > MAX_SET_CHANGE:
        raise ValueError(f"Смещение должно быть целым от -{MAX_SET_CHANGE} до {MAX_SET_CHANGE}")
    return int(number)


def numeric(data, name, alias=None):
    values = [data[k] for k in (name, alias) if k and data.get(k) is not None]
    if not values:
        return None
    if any(isinstance(v, bool) or not isinstance(v, int | float) or not math.isfinite(v) for v in values):
        raise ValueError(f"Поле {name} должно быть конечным числом")
    if any(v != values[0] for v in values):
        raise ValueError(f"Поля {name} и {alias} противоречат друг другу")
    return values[0]


@dataclass(frozen=True)
class SetSnapshot:
    volume: int | None
    intensity: float | None
    effort: float | None
    weight: float | None
    parts: tuple["SetSnapshot", ...] = ()

    @classmethod
    def parse(cls, data: dict, *, nested=False):
        if not isinstance(data, dict):
            raise ValueError("Подход должен быть объектом")
        parts = data["subsets"] if data.get("subsets") is not None else []
        if not isinstance(parts, list) or len(parts) > MAX_SET_PARTS:
            raise ValueError("Недопустимый размер составного подхода")
        if parts:
            if nested:
                raise ValueError("Вложение составных подходов друг в друга не поддерживается")
            return cls(None, None, None, None, tuple(cls.parse(p, nested=True) for p in parts))
        volume = numeric(data, "volume", "reps")
        intensity = numeric(data, "intensity")
        effort = numeric(data, "effort", "rpe")
        weight = numeric(data, "working_weight", "weight")
        if volume is not None and (volume < 1 or int(volume) != volume):
            raise ValueError("Число повторов должно быть положительным целым")
        if intensity is not None and not 0 < intensity <= 100:
            raise ValueError("Интенсивность должна быть больше 0 и не больше 100")
        if effort is not None and not 1 <= effort <= 10:
            raise ValueError("RPE должен быть от 1 до 10")
        if weight is not None and weight < 0:
            raise ValueError("Рабочий вес должен быть неотрицательным")
        return cls(int(volume) if volume is not None else None, intensity, effort, weight)


def validate_sets(sets: list[dict]):
    if len(sets) > MAX_SET_PARTS or sum(len(s.get("subsets") or []) or 1 for s in sets) > MAX_SET_PARTS:
        raise ValueError("В экземпляре слишком много подходов или частей")
    for data in sets:
        try:
            SetSnapshot.parse(data)
        except ValueError as exc:
            raise ValueError(f"Подход {data.get('id', '?')}: {exc}") from exc
