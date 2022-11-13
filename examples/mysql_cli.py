import logging
import asyncio
from sqlglot.executor import execute
from sqlglot.optimizer import optimize

from mysql_mimic import MysqlServer, Session

logger = logging.getLogger(__name__)


class MySession(Session):
    def __init__(self):
        super().__init__()
        self._data = {
            "test": {
                "x": [
                    {"a": 1},
                    {"a": 2},
                    {"a": 3},
                ]
            }
        }
        self._schema = {
            "test": {
                "x": {
                    "a": "INT",
                }
            }
        }

    async def handle_query(self, expression, sql, attrs):
        logger.info("Received query: %s", sql)
        expression = optimize(expression, schema=self._schema, db=self.database)
        result = execute(expression.sql(), schema=self._schema, tables=self._data)
        return result.rows, result.columns

    async def schema(self):
        return self._schema


async def main():
    logging.basicConfig(level=logging.INFO)
    server = MysqlServer(session_factory=MySession)
    await server.start_unix_server(
        # By default, the `mysql` command tries to connect to this socket
        path="/tmp/mysql.sock"
    )
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
