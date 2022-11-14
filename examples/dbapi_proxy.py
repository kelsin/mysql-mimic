import logging
import asyncio
import sqlite3

from mysql_mimic import MysqlServer, Session

logger = logging.getLogger(__name__)


class DbapiProxySession(Session):
    def __init__(self):
        super().__init__()
        self.conn = sqlite3.connect(":memory:")

    async def query(self, expression, sql, attrs):
        cursor = self.conn.cursor()
        cursor.execute(expression.sql(dialect="sqlite"))
        try:
            rows = cursor.fetchall()
            if cursor.description:
                columns = [c[0] for c in cursor.description]
                return rows, columns
            return None
        finally:
            cursor.close()


async def main():
    logging.basicConfig(level=logging.INFO)
    server = MysqlServer(session_factory=DbapiProxySession)
    await server.start_server()
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
