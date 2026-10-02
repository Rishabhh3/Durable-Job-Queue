import asyncio
from dotenv import load_dotenv
load_dotenv()

from djq.db import dispose_engine, session_scope
from djq.queue import enqueue


async def main():
    async with session_scope() as session:
        for name in ["Rishabh", "Sam", "Alex"]:
            await enqueue(session, "greet", {"name": name})
    await dispose_engine()
    print("added 3 jobs")


asyncio.run(main())