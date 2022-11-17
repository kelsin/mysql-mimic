import logging
import asyncio
from sqlglot.executor import execute
from sqlglot.optimizer import optimize

from mysql_mimic import MysqlServer, Session


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
        expression = optimize(expression, schema=SCHEMA, db=self.database)
        result = execute(expression.sql(), schema=SCHEMA, tables=TABLES)
        return result.rows, result.columns

    async def schema(self):
        return SCHEMA


async def main():
    logging.basicConfig(level=logging.INFO)
    server = MysqlServer(session_factory=MySession)
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
