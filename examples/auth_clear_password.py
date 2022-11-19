import logging
import asyncio

from mysql_mimic import (
    MysqlServer,
    IdentityProvider,
    User,
)
from mysql_mimic.auth import AbstractClearPasswordAuthPlugin

logger = logging.getLogger(__name__)


# Storing plain text passwords is not safe.
# This is just done for demonstration purposes.
USERS = {
    "user1": "password1",
    "user2": "password2",
}


class CustomAuthPlugin(AbstractClearPasswordAuthPlugin):
    name = "custom_plugin"

    async def check(self, username, password):
        return username if USERS.get(username) == password else None


class CustomIdentityProvider(IdentityProvider):
    def get_plugins(self):
        return [CustomAuthPlugin()]

    async def get_user(self, username):
        # Because we're storing users/passwords in an external system (the USERS dictionary, in this case),
        # we just assume all users exist.
        return User(name=username, auth_plugin=CustomAuthPlugin.name)


async def main():
    logging.basicConfig(level=logging.INFO)
    identity_provider = CustomIdentityProvider()
    server = MysqlServer(identity_provider=identity_provider)
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
    # To connect with the MySQL command line interface:
    # mysql -h127.0.0.1 -P3306 -uuser1 -ppassword1 --enable-cleartext-plugin
