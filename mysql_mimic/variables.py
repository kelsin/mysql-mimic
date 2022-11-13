from __future__ import annotations

import abc
from typing import Any, Callable

from mysql_mimic.charset import CharacterSet, Collation
from mysql_mimic.errors import MysqlError, ErrorCode


class Default:
    ...


DEFAULT = Default()

SYSTEM_VARIABLES = {
    # name: (type, default, dynamic)
    "autocommit": (bool, True, True),
    "version": (str, "8.0.29", False),
    "version_comment": (str, "mysql-mimic", False),
    "character_set_client": (str, CharacterSet.utf8mb4.name, True),
    "character_set_results": (str, CharacterSet.utf8mb4.name, True),
    "character_set_server": (str, CharacterSet.utf8mb4.name, True),
    "character_set_connection": (str, CharacterSet.utf8mb4.name, True),
    "character_set_database": (str, CharacterSet.utf8mb4.name, True),
    "collation_server": (str, Collation.utf8mb4_general_ci.name, True),
    "collation_database": (str, Collation.utf8mb4_general_ci.name, True),
    "collation_connection": (str, Collation.utf8mb4_general_ci.name, True),
    "transaction_isolation": (str, "READ-COMMITTED", True),
    "sql_mode": (str, "ANSI", True),
    "lower_case_table_names": (int, 0, True),
    "external_user": (str, "", False),
}


class Variables(abc.ABC):
    def __init__(self) -> None:
        self.values: dict[str, Any] = {}

    def get_schema(self, name: str) -> tuple[Callable, Any, bool]:
        schema = self.schema.get(name)
        if not schema:
            raise MysqlError(
                f"Unknown variable: {name}", code=ErrorCode.UNKNOWN_SYSTEM_VARIABLE
            )
        return schema

    def set(self, name: str, value: Any, force: bool = False) -> None:
        type_, default, dynamic = self.get_schema(name)

        if not dynamic and not force:
            raise MysqlError(
                f"Variable is not dynamic: {name}", code=ErrorCode.PARSE_ERROR
            )

        if value is DEFAULT:
            self.values[name] = default
        else:
            self.values[name] = type_(value)

    def get(self, name: str) -> Any:
        if name in self.values:
            return self.values[name]
        _, default, _ = self.get_schema(name)

        return default

    def list(self) -> list[tuple[str, str]]:
        return [(name, self.get(name)) for name in sorted(self.schema)]

    @property
    @abc.abstractmethod
    def schema(self) -> dict[str, tuple[type, Any, bool]]:
        ...


class GlobalVariables(Variables):
    def __init__(self, schema: dict[str, tuple[type, Any, bool]] | None = None):
        self._schema = schema or SYSTEM_VARIABLES
        super().__init__()

    @property
    def schema(self) -> dict[str, tuple[type, Any, bool]]:
        return self._schema


class SessionVariables(Variables):
    def __init__(self, global_variables: Variables):
        self.global_variables = global_variables
        super().__init__()

    @property
    def schema(self) -> dict[str, tuple[type, Any, bool]]:
        return self.global_variables.schema
