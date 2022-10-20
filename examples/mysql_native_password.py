import logging
import asyncio
import sqlite3
from typing import Sequence, Optional, Dict

from mysql_mimic import (
    MysqlServer,
    Session,
    IdentityProvider,
    MysqlNativePasswordAuthPlugin,
    AuthPlugin,
    User,
    AllowedResult,
)


class CustomIdentityProvider(IdentityProvider):
    def __init__(self, passwords: Dict[str, str]):
        # Storing passwords in plain text isn't safe.
        # This is done for demonstration purposes.
        # It's better to store the password hash, as returned by `MysqlNativePasswordAuthPlugin.create_auth_string`
        self.passwords = passwords

    def get_plugins(self) -> Sequence[AuthPlugin]:
        return [MysqlNativePasswordAuthPlugin()]

    async def get_user(self, username: str) -> Optional[User]:
        password = self.passwords.get(username)
        if password:
            return User(
                name=username,
                auth_string=MysqlNativePasswordAuthPlugin.create_auth_string(password),
                auth_plugin=MysqlNativePasswordAuthPlugin.name,
            )
        return None


class SqliteProxySession(Session):
    def __init__(self) -> None:
        self.conn = sqlite3.connect(":memory:")

    async def query(self, sql: str, attrs: Dict[str, str]) -> AllowedResult:
        print(f"Received query: {sql}")
        cursor = self.conn.cursor()
        cursor.execute(sql)
        try:
            rows = cursor.fetchall()
            columns = cursor.description and [c[0] for c in cursor.description]
            return rows, columns
        finally:
            cursor.close()


async def main() -> None:
    logging.basicConfig()
    identity_provider = CustomIdentityProvider(passwords={"user": "password"})
    server = MysqlServer(
        session_factory=SqliteProxySession, identity_provider=identity_provider
    )
    await server.start_server(port=3306)
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
