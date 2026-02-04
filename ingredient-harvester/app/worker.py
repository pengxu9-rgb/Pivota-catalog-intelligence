from __future__ import annotations

import os

from redis import Redis
from rq import Connection, Worker

from app.config import settings


def main() -> None:
    if not settings.redis_url:
        raise SystemExit("REDIS_URL is required to run worker (or set HARVESTER_QUEUE_MODE=inline).")
    qname = os.getenv("HARVESTER_RQ_QUEUE", "ingredient-harvester")
    redis_conn = Redis.from_url(settings.redis_url)
    with Connection(redis_conn):
        worker = Worker([qname])
        worker.work(with_scheduler=False)


if __name__ == "__main__":
    main()

