"""A real worker process, used only by the crash test.

`python -m djq.worker` starts a worker with no handlers registered, because
handlers attach themselves via the @register decorator at import time. So the
crash test launches this file instead: it registers one handler, then hands
over to the normal worker loop. Everything below the handler is the real
worker, unmodified.

How long the handler sleeps comes from DJQ_CRASH_TEST_SLEEP, not from the job
args, because the same job has to be slow on its first run (so we can kill the
worker mid-job) and quick on its second (so the test doesn't take a minute).

No load_dotenv() here on purpose. The test passes a complete environment to
this process, and reading .env could point it at the development database
instead of the test one.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from djq.worker import register, run

SLEEP_SECONDS = float(os.environ.get("DJQ_CRASH_TEST_SLEEP", "60"))


@register("crash_test")
async def crash_test_handler(args: dict[str, Any]) -> None:
    """Sleeps, so the test has a window in which to kill this process."""
    await asyncio.sleep(SLEEP_SECONDS)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run())


if __name__ == "__main__":
    main()