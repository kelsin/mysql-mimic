import argparse
import logging
import asyncio
import os
import sys
import time

from sqlglot.executor import execute

from mysql_mimic import (
    MysqlServer,
    IdentityProvider,
    NativePasswordAuthPlugin,
    User,
    Session,
)

logger = logging.getLogger(__name__)

SCHEMA = {
    "test": {
        "x": {
            "a": "INT",
        }
    }
}

TABLES = {
    "test": {
        "x": [
            {"a": 1},
            {"a": 2},
            {"a": 3},
        ]
    }
}


class MySession(Session):
    async def query(self, expression, sql, attrs):
        result = execute(expression, schema=SCHEMA, tables=TABLES)
        return result.rows, result.columns

    async def schema(self):
        return SCHEMA


class CustomIdentityProvider(IdentityProvider):
    def __init__(self):
        self.passwords = {"user": "password"}

    def get_plugins(self):
        return [NativePasswordAuthPlugin()]

    async def get_user(self, username):
        password = self.passwords.get(username)
        if password is not None:
            return User(
                name=username,
                auth_string=NativePasswordAuthPlugin.create_auth_string(password)
                if password
                else None,
                auth_plugin=NativePasswordAuthPlugin.name,
            )
        return None


async def wait_for_port(port, host="localhost", timeout=5.0):
    start_time = time.time()
    while True:
        try:
            _ = await asyncio.open_connection(host, port)
            break
        except OSError:
            await asyncio.sleep(0.01)
            if time.time() - start_time >= timeout:
                raise TimeoutError()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("test_dir")
    parser.add_argument("-p", "--port", type=int, default=3308)
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)
    identity_provider = CustomIdentityProvider()
    server = MysqlServer(identity_provider=identity_provider, session_factory=MySession)
    await server.start_server(port=args.port)
    await wait_for_port(port=args.port)
    process = await asyncio.create_subprocess_shell(
        "make test",
        env={**os.environ, "PORT": str(args.port)},
        cwd=args.test_dir,
    )
    return_code = await process.wait()
    server.close()
    await server.wait_closed()
    return return_code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
