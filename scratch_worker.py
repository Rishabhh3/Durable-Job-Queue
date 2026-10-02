import asyncio
from dotenv import load_dotenv
load_dotenv()

from djq.worker import register, run


@register("greet")
async def greet(args):
    print(f"hello {args.get('name', 'world')}")
    await asyncio.sleep(1)


asyncio.run(run())