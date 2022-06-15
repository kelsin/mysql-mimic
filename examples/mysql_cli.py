import logging
import asyncio

from mysql_mimic import MysqlServer, Session


class MySqlCliCompatibleSession(Session):
    async def query(self, sql):
        # The MySQL CLI asks for this
        if sql.lower() == "select @@version_comment limit 1":
            return [["MySql-Mimic Python Proxy - MIT"]], ["@@version_comment"]

        return [("foo", 1.0), ("bar", 2.0)], ["col1", "col2"]


async def main():
    logging.basicConfig()
    server = MysqlServer(session_factory=MySqlCliCompatibleSession)
    await server.start_unix_server(
        # By default, the `mysql` command tries to connect to this socket
        path="/tmp/mysql.sock"
    )
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
