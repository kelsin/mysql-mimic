import logging
import asyncio

from mysql_mimic import MysqlServer, Session


class MySession(Session):
    async def generate_rows(self, n):
        for i in range(n):
            if i % 100 == 0:
                logging.info("Pretending to fetch another batch of results...")
                await asyncio.sleep(1)
            yield i,

    async def query(self, expression, sql, attrs):
        return self.generate_rows(1000), ["a"]


async def main():
    logging.basicConfig(level=logging.DEBUG)
    server = MysqlServer(session_factory=MySession)
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
