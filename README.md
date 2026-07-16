# QueueCTL

A CLI-based background job queue system with worker processes, exponential backoff retries, a Dead Letter Queue (DLQ), and persistent SQLite storage.

## Features

- Enqueue shell-command jobs as JSON
- Multiple parallel worker threads with job locking (no duplicate execution)
- Exponential backoff retries: `delay = base ^ attempts` seconds
- Dead Letter Queue for permanently failed jobs
- Configurable `max-retries` and `backoff-base`
- Graceful worker shutdown (finish current job before exit)
- Persistent storage across restarts

## Requirements

- Python 3.10+

## Setup

```bash
git clone <your-repo-url>
cd queuectl

python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS/Linux
source .venv/bin/activate

pip install -e ".[dev]"
```



## Quick Start

```bash
# Configure defaults
queuectl config set max-retries 3
queuectl config set backoff-base 2
queuectl config show

# Enqueue jobs
queuectl enqueue "{\"id\":\"job1\",\"command\":\"echo hello\"}"
queuectl enqueue "{\"id\":\"job2\",\"command\":\"sleep 2\"}"

# Start workers (in a separate terminal)
queuectl worker start --count 3

# Monitor
queuectl status
queuectl list --state pending
queuectl list --state completed

# Stop workers gracefully
queuectl worker stop
```



## CLI Commands


| Command                             | Description                       |
| ----------------------------------- | --------------------------------- |
| `queuectl enqueue '<json>'`         | Add a job to the queue            |
| `queuectl worker start --count N`   | Start N worker threads            |
| `queuectl worker stop`              | Gracefully stop workers           |
| `queuectl status`                   | Job counts, config, worker status |
| `queuectl list [--state STATE]`     | List jobs                         |
| `queuectl dlq list`                 | List dead-letter jobs             |
| `queuectl dlq retry <job-id>`       | Requeue a dead job                |
| `queuectl config set <key> <value>` | Set config                        |
| `queuectl config show`              | Show config                       |




### Job JSON Schema

```json
{
  "id": "unique-job-id",
  "command": "echo 'Hello World'",
  "max_retries": 3
}
```

Required: `id`, `command`. Optional: `max_retries` (falls back to global config).

### Job States


| State        | Meaning                       |
| ------------ | ----------------------------- |
| `pending`    | Waiting for a worker          |
| `processing` | Currently executing           |
| `completed`  | Succeeded (exit code 0)       |
| `failed`     | Failed but eligible for retry |
| `dead`       | Exceeded max retries (DLQ)    |




## Architecture

```
CLI (Typer)
   │
   ├── QueueService ── enqueue / list / status / DLQ
   │
   ├── JobStorage (SQLite + WAL)
   │      └── atomic claim via UPDATE ... WHERE state IN (pending, failed)
   │
   └── WorkerManager
          ├── N worker threads
          ├── subprocess shell execution
          └── retry + exponential backoff on failure
```



### Persistence

All data lives under `~/.queuectl/` by default:

- `queue.db` — SQLite job store
- `config.json` — runtime configuration
- `worker.pid` / `worker.stop` — worker lifecycle files

Override with:

```bash
export QUEUECTL_DATA_DIR=/path/to/data   # Linux/macOS
set QUEUECTL_DATA_DIR=D:\path\to\data      # Windows
```



### Retry Logic

On failure (non-zero exit code or command error):

1. Increment `attempts`
2. If `attempts > max_retries` → move to `dead` (DLQ)
3. Else → set state to `failed` and schedule `next_retry_at = now + base^attempts`



### Concurrency Safety

Workers claim jobs with an atomic SQLite transaction:

```sql
UPDATE jobs SET state='processing' ... WHERE id=? AND state IN ('pending','failed')
```

Only one worker can claim a given job.

## Assumptions & Trade-offs

- Commands run via `shell=True` for simplicity (matches assignment examples like `sleep 2`)
- Workers run as threads inside one process (`--count N`), not separate OS processes
- Single-machine SQLite is sufficient for this scope; not distributed
- Backoff uses wall-clock scheduling (`next_retry_at`), not in-memory timers
- Stale `processing` jobs (30+ min) are reset to `pending` on worker poll



## Testing

```bash
# Unit/integration tests
pytest -v

# End-to-end validation script
python scripts/validate.py
```



### Test Scenarios Covered

1. Successful job completion
2. Failure → retry → DLQ
3. Concurrent workers without duplicate claims
4. Invalid duplicate job IDs
5. Persistence across service restarts
6. DLQ retry


## Project Structure

```
queuectl/
├── src/queuectl/
│   ├── cli.py          # Typer CLI entrypoint
│   ├── models.py       # Job model and states
│   ├── storage.py      # SQLite persistence + locking
│   ├── queue.py        # Queue operations
│   ├── worker.py       # Worker pool + execution
│   ├── backoff.py      # Exponential backoff
│   ├── config.py       # Paths and constants
│   └── settings.py     # Config file manager
├── tests/
├── scripts/validate.py
├── pyproject.toml
└── README.md
```



