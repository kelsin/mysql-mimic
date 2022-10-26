import logging
import asyncio
import sqlite3
from typing import Sequence, Optional, Dict

from mysql_mimic import (
    MysqlServer,
    Session,
    IdentityProvider,
    AuthPlugin,
    User,
    AllowedResult,
)
from mysql_mimic.auth import AbstractMysqlClearPasswordAuthPlugin

logger = logging.getLogger(__name__)


# Storing plain text passwords is not safe.
# This is just done for demonstration purposes.
USERS = {
    "user1": "password1",
    "user2": "password2",
}


class CustomAuthPlugin(AbstractMysqlClearPasswordAuthPlugin):
    name = "custom_plugin"

    async def check(self, username: str, password: str) -> Optional[str]:
        return username if USERS.get(username) == password else None


class CustomIdentityProvider(IdentityProvider):
    def get_plugins(self) -> Sequence[AuthPlugin]:
        return [CustomAuthPlugin()]

    async def get_user(self, username: str) -> Optional[User]:
        # Because we're storing users/passwords in an external system (the USERS dictionary, in this case),
        # we just assume all users exist.
        return User(name=username, auth_plugin=CustomAuthPlugin.name)


class SqliteProxySession(Session):
    def __init__(self) -> None:
        self.conn = sqlite3.connect(":memory:")

    async def query(self, sql: str, attrs: Dict[str, str]) -> AllowedResult:
        logger.info("Received query: %s", sql)
        cursor = self.conn.cursor()
        cursor.execute(sql)
        try:
            rows = cursor.fetchall()
            columns = cursor.description and [c[0] for c in cursor.description]
            return rows, columns
        finally:
            cursor.close()


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    identity_provider = CustomIdentityProvider()
    server = MysqlServer(
        session_factory=SqliteProxySession, identity_provider=identity_provider
    )
    await server.start_server(port=3306)
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
    # To connect with the MySQL command line interface:
    # mysql -h127.0.0.1 -P3306 -uuser1 -ppassword1 --enable-cleartext-plugin
