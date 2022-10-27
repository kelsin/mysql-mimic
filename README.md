# MySQL-Mimic

[![Tests](https://github.com/kelsin/mysql-mimic/actions/workflows/tests.yml/badge.svg)](https://github.com/kelsin/mysql-mimic/actions/workflows/tests.yml)

Pure-python implementation of the MySQL server [wire protocol](https://dev.mysql.com/doc/internals/en/client-server-protocol.html).

This can be used to create applications that act as a MySQL server.

MySQL-Mimic doesn't parse SQL - it only handles the wire protocol. For parsing, check out [sqlglot](https://github.com/tobymao/sqlglot). 

## Installation

```shell
pip install mysql-mimic
```

## Usage

A minimal use case might look like this:

```python
import asyncio

from mysql_mimic import MysqlServer, Session


class MySession(Session):
    async def init(self, connection):
        print(f"new session: {connection}")
  
    async def query(self, sql, attrs):
        print(f"received query: {sql}")
        return [("a", 1), ("b", 2)], ["col1", "col2"]
  
    async def close(self):
        print("session closed")


if __name__ == "__main__":
    server = MysqlServer(session_factory=MySession)
    asyncio.run(server.serve_forever())
```

See [examples](./examples) for more examples.

## Authentication

MySQL-mimic has built in support for several standard MySQL authentication plugins:
- [mysql_native_password](https://dev.mysql.com/doc/refman/8.0/en/native-pluggable-authentication.html)
  - The client sends hashed passwords to the server, and the server stores hashed passwords. See the documentation for more details on how this works.
  - [example](examples/auth_native_password.py)
- [mysql_clear_password](https://dev.mysql.com/doc/refman/8.0/en/cleartext-pluggable-authentication.html)
  - The client sends passwords to the server as clear text, without hashing or encryption.
  - This is typically used as the client plugin for a custom server plugin. As such, MySQL-mimic provides an abstract class, [`mysql_mimic.auth.AbstractClearPasswordAuthPlugin`](mysql_mimic/auth.py), which can be extended.
  - [example](examples/auth_clear_password.py)
- [mysql_no_login](https://dev.mysql.com/doc/refman/8.0/en/no-login-pluggable-authentication.html)
  - The server prevents clients from directly authenticating as an account. See the documentation for relevant use cases. 

By default, a session naively accepts whatever username the client provides.

Plugins are provided to the server by implementing [`mysql_mimic.IdentityProvider`](mysql_mimic/auth.py), which configures all available plugins and a callback for fetching users.

Custom plugins can be created by extending [`mysql_mimic.auth.AuthPlugin`](mysql_mimic/auth.py).

## Development

You can install dependencies with `make deps`. 

You can format your code with `make format`. 

You can lint with `make lint`. 

You can check type annotations with `make types`.

You can run tests with `make test`. This will build a coverage report in `./htmlcov/index.html`. 

You can run all the checks with `make check`.

You can build a pip package with `make build`.
