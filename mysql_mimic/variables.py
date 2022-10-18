from collections import UserDict
from typing import Dict, Any

from mysql_mimic.charset import CharacterSet, Collation

DEFAULT_VARIABLES = {
    "version": "8.0.29",
    "version_comment": "mysql-mimic",
    "character_set_client": CharacterSet.utf8mb4.name,
    "character_set_results": CharacterSet.utf8mb4.name,
    "character_set_server": CharacterSet.utf8mb4.name,
    "collation_server": Collation.utf8mb4_general_ci.name,
    "collation_database": Collation.utf8mb4_general_ci.name,
    "transaction_isolation": "READ-COMMITTED",
    "sql_mode": "",
    "lower_case_table_names": 0,
    "external_user": "",
}


class SystemVariables(UserDict):
    def __init__(self, defaults: Dict[str, Any] = None):
        self.defaults = defaults or DEFAULT_VARIABLES
        super().__init__(self.defaults)

    @property
    def server_charset(self) -> CharacterSet:
        return CharacterSet[self.get("character_set_results", "utf8mb4")]

    @server_charset.setter
    def server_charset(self, val: CharacterSet) -> None:
        self["character_set_results"] = val.name

    @property
    def client_charset(self) -> CharacterSet:
        return CharacterSet[self.get("character_set_client", "utf8mb4")]

    @client_charset.setter
    def client_charset(self, val: CharacterSet) -> None:
        self["character_set_client"] = val.name

    @property
    def external_user(self) -> str:
        return self["external_user"]

    @external_user.setter
    def external_user(self, val: str) -> None:
        self["external_user"] = val

    @property
    def mysql_version(self) -> str:
        return self["version"]
