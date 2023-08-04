import logging
import asyncio
import sqlite3
from mysql_mimic import (
    MysqlServer,
    Session,
)

logger = logging.getLogger(__name__)


class DbapiProxyStreamingSession(Session):
    def __init__(self):
        super().__init__()
        self.conn = sqlite3.connect(":memory:")

    async def query(self, expression, sql, attrs):
        cursor = self.conn.cursor()
        cursor.execute(expression.sql(dialect="sqlite"))
        if cursor.description:
            column_names = [column[0] for column in cursor.description]
            return self.fetch_data(cursor), column_names
        return None

    async def fetch_data(self, cursor):
        while True:
            rows = cursor.fetchmany(1000)
            for r in rows:
                yield r

            # not sure if this is the right place for closing the cursor
            cursor.close()
            break


async def main():
    logging.basicConfig(level=logging.DEBUG)
    server = MysqlServer(session_factory=DbapiProxyStreamingSession)
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
