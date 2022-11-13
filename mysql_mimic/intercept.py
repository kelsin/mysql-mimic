from __future__ import annotations

from typing import Any

from sqlglot import expressions as exp
from mysql_mimic.variables import DEFAULT


def setitem_kind(setitem: exp.SetItem) -> str:
    kind = setitem.text("kind")
    if not kind:
        return "VARIABLE"

    if kind in {"GLOBAL", "PERSIST", "PERSIST_ONLY", "SESSION", "LOCAL"}:
        return "VARIABLE"

    return kind


def value_to_expression(value: Any) -> exp.Expression:
    if value is True:
        return exp.TRUE.copy()
    if value is False:
        return exp.FALSE.copy()
    if value is None:
        return exp.NULL.copy()
    if isinstance(value, (int, float)):
        return exp.Literal.number(value)
    return exp.Literal.string(str(value))


def expression_to_value(expression: exp.Expression) -> Any:
    if expression == exp.TRUE:
        return True
    if expression == exp.FALSE:
        return False
    if expression == exp.NULL:
        return None
    if isinstance(expression, exp.Literal) and not expression.args.get("is_string"):
        return float(expression.this)
    if isinstance(expression, exp.Literal):
        return expression.name
    if expression.name == "DEFAULT":
        return DEFAULT
    if expression.name == "ON":
        return True
    if expression.name == "OFF":
        return False
    return expression.name
