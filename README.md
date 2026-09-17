# Durable Job Queue

A job queue built on plain Postgres, with a small HTTP API for putting work in
and checking on it.

No Redis, no RabbitMQ, no Celery. Just a database table and careful SQL.

**Status: in progress.** See [What works so far](#what-works-so-far) below.

---

## The idea

A job queue has one promise to keep: if you hand it a job, that job runs
exactly once, even if a worker crashes halfway through.

Most people reach for a dedicated queue tool to get that. But Postgres can do
it, and doing it yourself means facing the problems those tools hide: how do
two workers avoid grabbing the same job, how do you notice a worker died, and
how do you retry without hammering a failing service.

This project does it the hard way on purpose.

---

## How it stays reliable

**Two workers never get the same job.** When a worker looks for work, it locks
the row it picks and tells Postgres to skip any rows another worker has already
locked. No waiting, no duplicates.

**A dead worker doesn't lose its job.** A worker doesn't own a job forever. It
gets a lease, a short window of time, and has to keep renewing it while it
works. If the worker crashes, nobody renews, the lease runs out, and another
worker picks the job up.

**Renewals happen often enough to be safe.** The renewal interval has to be
well under the lease length, otherwise one slow moment could cost a perfectly
healthy worker its job. The settings refuse to start if you get this wrong.

**Failures back off instead of piling on.** A job that fails waits before
trying again, and waits longer each time. After a set number of attempts it
stops and gets set aside for a human to look at.

**Workers don't wake up in lockstep.** Each worker adds a small random wobble
to its waiting time, so ten workers don't all hit the database at the same
instant.

---

## What works so far

- [x] Postgres running in Docker, with a separate database for tests
- [x] Settings loaded from the environment, with validation that catches bad
      combinations at startup
- [ ] Database tables and migrations
- [ ] Adding and claiming jobs
- [ ] Worker process with leases and renewals
- [ ] Retries and the set-aside table for dead jobs
- [ ] HTTP API
- [ ] Tests, including crash tests and multiple workers racing

---

## Getting started

You need Docker and Python 3.11 or newer.

```bash
# 1. Start Postgres
docker compose up -d

# 2. Set up Python
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
pip install -e .

# 3. Copy the settings file
cp .env.example .env
```

To wipe the database and start clean:

```bash
docker compose down -v && docker compose up -d
```

The `-v` matters. The setup script that creates the test database only runs
when the data folder is empty, so a plain restart won't re-run it.

---

## Settings

Everything is configured through environment variables, all starting with
`DJQ_`. Every one has a sensible default, so the project runs with no setup at
all. See `.env.example` for the full list with explanations.

The ones you're most likely to touch:

| Variable                | Default            | What it does                                              |
| ----------------------- | ------------------ | --------------------------------------------------------- |
| `DJQ_DATABASE_URL`      | local dev database | Where Postgres is                                         |
| `DJQ_LEASE_SECONDS`     | `30`               | How long a worker holds a job before it's considered dead |
| `DJQ_HEARTBEAT_SECONDS` | `10`               | How often a working worker renews its hold                |
| `DJQ_MAX_ATTEMPTS`      | `5`                | Tries before a job is set aside                           |
| `DJQ_BATCH_SIZE`        | `1`                | Jobs claimed per check                                    |

Outside of development, `DJQ_DATABASE_URL` is required. The app refuses to
start without it rather than quietly connecting to a local dev database.

---

## Choices worth explaining

**Postgres instead of a queue tool.** The point of the project is to build the
reliability guarantees, not to configure something that already has them.
Postgres also means jobs live in the same database as your other data, so
adding a job and updating a record can happen together or not at all.

**Tests run against real Postgres, never SQLite.** The row-locking behaviour
this queue depends on doesn't exist in SQLite. A test suite on SQLite would
pass while proving nothing about the thing that actually matters.

**The worker is its own process, not part of the web server.** Running jobs
inside the API process is the shortcut that quietly breaks the durability
promise, because restarting the server would kill jobs mid-run.

**Settings are read-only and validated at startup.** Bad configuration should
stop the program immediately with a clear message, not cause strange behaviour
an hour later.

---

## Layout

```
djq/            queue and API code
scripts/        database setup scripts
docker-compose.yml
```

---

## Related

[Agent Sandbox Runner](https://github.com/Rishabhh3/Agent-Sandbox-Runner) is a sandbox runner that executes
untrusted commands in a locked-down container. The plan is for Warden to be
what actually runs the jobs this queue hands out.
