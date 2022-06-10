# MySql-Mimic

[![Tests](https://github.com/kelsin/mysql-mimic/actions/workflows/tests.yml/badge.svg)](https://github.com/kelsin/mysql-mimic/actions/workflows/tests.yml)

Python implementation of the mysql server wire protocol in order to create a SQL
proxy. This allows you to create a server that mysql clients can connect to in
order to run queries.

## Python Support

Tests run in 3.8, 3.9, and 3.10. The package should work in older versions but
tests won't run due to the usage of the AsyncIO test helpers in 3.8.

## Todo

- *Minor* Proper connection IDs
- Proper types support. Right now all data is returned as a string type, we need
  to properly inspect the DataFrame and return proper column information.
- Add support for (at least) the `mysql_native_password` authentication method
  with another callback.
- *Eventually* Compression support
- *Eventually* SSL support

The last two items shouldn't block us pushing a pypi package.

## Usage

Until we finish more of the Todo items above we won't be publishing on
pypi. Until then download the repo and install locally into your projects:

``` shell
# Download
git checkout https://github.com/kelsin/mysql-mimic.git

# To use in your project:
cd <your project>
pip install -e <path_to_mysql_mimic>

# Or to develop
cd mysql-mimic
make deps
```

This library is meant to be used as the basis for a proxy SQL service. A minimal
use case might look like this:

``` python
import asyncio
import pandas

from mysql_mimic.server import MysqlServer

# Handlers take in a string query and should return a
# pandas.DataFrame with the expected data. In order to
# make the mysql cli client work you should handle the
# @@version_comment query like below.
def query_handler(query):
    if query.lower() == "select @@version_comment limit 1":
        return pandas.DataFrame(
            data={"@@version_comment": ["MySql-Mimic Python Proxy - MIT"]}
        )

    return pandas.DataFrame(data={"col1": ["foo", "bar"], "col2": [1.0, 2.0]})

# Run the server
if __name__ == "__main__":
    server = MysqlServer(handler=query_handler)
    asyncio.run(server.start())
```

You can pass a `socket` keyword argument to `MysqlServer` to listen on a unix
socket. You can pass `host` or `port` to change the defaults of `127.0.0.1` and
`3306`. You can pass any keyword arguments you want to pass onto the `asyncio`
server creation methods in the `MysqlServer.start` method.

The `mysql_mimic/server.py` file is runnable with our default handler (returns
static data for every query) by using `make run` in the repository.

## Development

You can install dependencies with `make deps`. You can format your code with
`make format`. You can lint with `make lint`. You can run tests with `make
test`. This will build a coverage report in `./htmlcov/index.html`. You can
build a pip package with `make build`.
