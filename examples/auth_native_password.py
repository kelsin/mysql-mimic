import logging
import asyncio

from mysql_mimic import (
    MysqlServer,
    IdentityProvider,
    NativePasswordAuthPlugin,
    User,
)

logger = logging.getLogger(__name__)


class CustomIdentityProvider(IdentityProvider):
    def __init__(self, passwords):
        # Storing passwords in plain text isn't safe.
        # This is done for demonstration purposes.
        # It's better to store the password hash, as returned by `NativePasswordAuthPlugin.create_auth_string`
        self.passwords = passwords

    def get_plugins(self):
        return [NativePasswordAuthPlugin()]

    async def get_user(self, username):
        password = self.passwords.get(username)
        if password:
            return User(
                name=username,
                auth_string=NativePasswordAuthPlugin.create_auth_string(password),
                auth_plugin=NativePasswordAuthPlugin.name,
            )
        return None


async def main():
    logging.basicConfig(level=logging.DEBUG)
    identity_provider = CustomIdentityProvider(passwords={"user": "password"})
    server = MysqlServer(identity_provider=identity_provider)
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
