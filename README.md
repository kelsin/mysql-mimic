# MySQL-Mimic

[![Tests](https://github.com/kelsin/mysql-mimic/actions/workflows/tests.yml/badge.svg)](https://github.com/kelsin/mysql-mimic/actions/workflows/tests.yml)

Pure-python implementation of the MySQL server [wire protocol](https://dev.mysql.com/doc/internals/en/client-server-protocol.html).

This can be used to create applications that act as a MySQL server.

MySQL-Mimic doesn't parse SQL - it only handles the wire protocol. For parsing, check out [sqlglot](https://github.com/tobymao/sqlglot). 

## Installation

```shell
pip install mysql-mimic
```

# Usage

This library is meant to be used as the basis for a proxy SQL service. A minimal
use case might look like this:

```python
import asyncio

from mysql_mimic import MysqlServer, Session


class MySession(Session):
    async def init(self, connection):
        print(f"new session: {connection}")
  
    async def query(self, sql):
        print(f"received query: {sql}")
        return [("a", 1), ("b", 2)], ["col1", "col2"]
  
    async def close(self):
        print("session closed")


if __name__ == "__main__":
    server = MysqlServer(session_factory=MySession)
    asyncio.run(server.serve_forever())
```

See [examples](./examples) for more examples.

## Todo

- Add support for (at least) the `mysql_native_password` authentication method
  with another callback.
- *Eventually* Compression support
- *Eventually* SSL support

## Development

You can install dependencies with `make deps`. You can format your code with
`make format`. You can lint with `make lint`. You can run tests with `make
test`. This will build a coverage report in `./htmlcov/index.html`. You can
build a pip package with `make build`.
