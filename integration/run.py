import argparse
import logging
import asyncio
import os
import sys
import tempfile
import time

import k5test
from sqlglot.executor import execute

from mysql_mimic import (
    MysqlServer,
    IdentityProvider,
    NativePasswordAuthPlugin,
    User,
    Session,
)
from mysql_mimic.auth import KerberosAuthPlugin

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
    def __init__(self, krb5_service, krb5_realm):
        self.users = {
            "user": {"auth_plugin": "mysql_native_password", "password": "password"},
            "krb5_user": {"auth_plugin": "authentication_kerberos"},
        }
        self.krb5_service = krb5_service
        self.krb5_realm = krb5_realm

    def get_plugins(self):
        return [
            NativePasswordAuthPlugin(),
            KerberosAuthPlugin(service=self.krb5_service, realm=self.krb5_realm),
        ]

    async def get_user(self, username):
        user = self.users.get(username)
        if user is not None:
            auth_plugin = user["auth_plugin"]

            if auth_plugin == "mysql_native_password":
                password = user.get("password")
                return User(
                    name=username,
                    auth_string=NativePasswordAuthPlugin.create_auth_string(password)
                    if password
                    else None,
                    auth_plugin=NativePasswordAuthPlugin.name,
                )
            elif auth_plugin == "authentication_kerberos":
                return User(name=username, auth_plugin=KerberosAuthPlugin.name)
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


def setup_krb5(krb5_user):
    realm = k5test.K5Realm()
    krb5_user_princ = f"{krb5_user}@{realm.realm}"
    realm.addprinc(krb5_user_princ, realm.password(krb5_user))
    realm.kinit(krb5_user_princ, realm.password(krb5_user))
    return realm


def write_jaas_conf(realm, debug=False):
    conf = f"""
MySQLConnectorJ {{
     com.sun.security.auth.module.Krb5LoginModule
     required
     debug={str(debug).lower()}
     useTicketCache=true
     ticketCache="{realm.env["KRB5CCNAME"]}";
}};
    """
    path = tempfile.mktemp()
    with open(path, "w") as f:
        f.write(conf)

    return path


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("test_dir")
    parser.add_argument("-p", "--port", type=int, default=3308)
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)
    realm = None
    jaas_conf = None

    try:
        realm = setup_krb5(krb5_user="krb5_user")
        os.environ.update(realm.env)

        jaas_conf = write_jaas_conf(realm)
        os.environ["JAAS_CONFIG"] = jaas_conf

        krb5_service = realm.host_princ[: realm.host_princ.index("@")]
        identity_provider = CustomIdentityProvider(
            krb5_service=krb5_service, krb5_realm=realm.realm
        )
        server = MysqlServer(
            identity_provider=identity_provider, session_factory=MySession
        )

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
    finally:
        if realm:
            realm.stop()
        if jaas_conf:
            os.remove(jaas_conf)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
