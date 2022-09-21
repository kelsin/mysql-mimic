import logging
import asyncio
import sqlite3

from mysql_mimic import MysqlServer, Session


class SqliteProxySession(Session):
    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("CREATE TABLE x (a INT)")
        self.conn.execute("INSERT INTO x VALUES (1)")
        self.conn.execute("INSERT INTO x VALUES (2)")

    async def query(self, sql, attrs):
        print(f"Received query: {sql}")
        cursor = self.conn.cursor()
        cursor.execute(sql)
        try:
            rows = cursor.fetchall()
            columns = cursor.description and [c[0] for c in cursor.description]
            return rows, columns
        finally:
            cursor.close()


async def main():
    logging.basicConfig()
    server = MysqlServer(session_factory=SqliteProxySession)
    await server.start_unix_server(
        # By default, the `mysql` command tries to connect to this socket
        path="/tmp/mysql.sock"
    )
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
