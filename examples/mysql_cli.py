import logging
import asyncio
import pandas as pd

from mysql_mimic.server import MysqlServer, Session


class MySqlCliCompatibleSession(Session):
    async def query(self, sql):
        # The MySQL CLI asks for this
        if sql.lower() == "select @@version_comment limit 1":
            return pd.DataFrame(
                data={"@@version_comment": ["MySql-Mimic Python Proxy - MIT"]}
            )

        return pd.DataFrame(data={"col1": ["foo", "bar"], "col2": [1.0, 2.0]})


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
