# QueueCTL — Background Job Queue System

QueueCTL is a robust, CLI-based background job queue system implemented in Python. It manages background jobs using parallel worker processes, performs automatic retries with exponential backoff, maintains a Dead Letter Queue (DLQ) for permanently failed jobs, and features automatic crash recovery for mid-job scenarios.

---

## 🛠️ Technologies Used

- Python 3.8: Core programming language utilizing built-in standard modules (`subprocess`, `threading`, `signal`, `json`, `datetime`, `sqlite3`).
- SQLite with WAL Mode: Embedded database engine configured with Write-Ahead Logging (`PRAGMA journal_mode=WAL;`) and immediate write transaction locking (`BEGIN IMMEDIATE`) for safe multi-process concurrency across separate terminals.
- Unittest / Pytest: Comprehensive automated test suite validating all 5 evaluation scenarios (job completion, retries & DLQ, multi-worker concurrency, crash recovery, and restart persistence).

---

## 📁 Repository Structure & File Explanation

```
QueueCTL/
├── queuectl/
│   ├── __init__.py      # Package initialization & version info
│   ├── models.py        # Job dataclass, JobState enum, ISO 8601 formatting utilities
│   ├── db.py            # SQLite connection pool, WAL mode setup, atomic BEGIN IMMEDIATE claiming, crash recovery scan
│   ├── queue.py         # High-level QueueManager for enqueue, listing, status, and DLQ retries
│   ├── worker.py        # Multi-worker runner, shell subprocess execution, heartbeat loop, signal handlers
│   └── cli.py           # Command-line interface parser (argparse) matching spec contracts
├── tests/
│   └── test_queuectl.py # Automated test suite covering scenarios 1–5
├── main.py              # Cross-platform Python entry point script
├── queuectl.bat         # Windows CMD executable wrapper
├── pyproject.toml       # Python package build & entrypoint configuration
├── DECISIONS.md         # In-depth answers to mandatory design review questions
└── README.md            # Complete documentation, setup guide, and architectural walkthrough
```

### Detailed File Explanations

1. [`queuectl/models.py`]:
   - Defines `JobState` enum (`pending`, `processing`, `completed`, `failed`, `dead`).
   - Defines `Job` dataclass with fields: `id`, `command`, `state`, `attempts`, `max_retries`, `created_at`, `updated_at`, `run_at`, `worker_id`, `heartbeat_at`.
   - Provides `to_dict()` and `from_dict()` for strict JSON output compliance (`queuectl list --state <state> --json`).

2. [`queuectl/db.py`]:
   - Manages SQLite storage using `PRAGMA journal_mode=WAL;` and `PRAGMA busy_timeout=5000;`.
   - Implements `claim_job()` with `BEGIN IMMEDIATE` transaction locking to guarantee atomic job reservation across processes.
   - Includes automatic stale job recovery scanning (detecting jobs in `processing` state whose `heartbeat_at` timestamp is older than 10 seconds).

3. [`queuectl/queue.py`]:
   - High-level business logic wrapping database operations.
   - Manages enqueuing with automatic UUID generation and default config values (`max-retries`, `backoff-base`).
   - Provides DLQ retry logic resetting job attempts to 0.

4. [`queuectl/worker.py`]:
   - Spawns and manages worker threads/processes.
   - Runs background thread for each active job updating `heartbeat_at` every 2 seconds.
   - Executes job commands via `subprocess.run(command, shell=True)`.
   - Captures `SIGTERM` / `SIGINT` signals and monitors cross-process DB stop signal for graceful shutdown.

5. [`queuectl/cli.py`]:
   - Parses command-line inputs using `argparse`.
   - Supports both positional raw JSON strings (e.g. `'{"id":"job1","command":"sleep 2"}'`) and CLI flags (`--id`, `--command`).
   - Ensures `queuectl list --state <state> --json` prints raw JSON arrays directly to `stdout`.

---

## ⚙️ Setup & Installation

### Option 1: Direct Python Execution (No Installation Needed)
```bash
python main.py --help
```

### Option 2: Windows CMD Wrapper
```cmd
queuectl status
```

### Option 3: Standard Package Install (Editable)
```bash
pip install -e .
queuectl --help
```

---

## 🚀 CLI Commands & Usage Examples

### 1. Enqueue Background Jobs
```bash
# Via JSON payload
python main.py enqueue '{"id":"job1","command":"sleep 2"}'

# Via command options
python main.py enqueue --id job2 --command "echo 'Hello World'" --max-retries 3
```

### 2. Start Worker Processes
```bash
# Start 3 worker threads in foreground (blocks until stopped)
python main.py worker start --count 3
```

### 3. Gracefully Stop Workers from Another Terminal
```bash
python main.py worker stop
```

### 4. View Queue Status
```bash
python main.py status
```

### 5. List Jobs by State
```bash
# Human readable list
python main.py list --state pending

# Pure JSON output (matching automated test contract)
python main.py list --state pending --json
```

### 6. Dead Letter Queue (DLQ) & Manual Retry
```bash
# List DLQ jobs
python main.py dlq list --json

# Retry a dead job
python main.py dlq retry job1
```

### 7. Manage System Configuration
```bash
# Set backoff base (e.g., base=2 -> delays: 2s, 4s, 8s)
python main.py config set backoff-base 2

# Set default max retries
python main.py config set max-retries 3

# View config
python main.py config get backoff-base
```

---

## 🔬 How the System Works

### 1. Job Lifecycle & State Transitions
```
                +------------+
                |  pending   | <-------------------------+
                +-----+------+                           |
                      | (claim_job)                      | (dlq retry)
                      v                                  |
                +------------+                           |
                | processing |                           |
                +--+------+--+                           |
                   |      |                              |
         (exit 0)  |      | (exit non-zero)              |
     +-------------+      +--------------+               |
     |                                   |               |
     v                                   v               |
+----+------+                     +------+-----+         |
| completed |                     |   failed   |         |
+-----------+                     +------+-----+         |
                                         |               |
                         (attempts >=    | (attempts <   |
                          max_retries)   |  max_retries) |
                                         v               |
                                   +-----+------+        |
                                   |    dead    +--------+
                                   |   (DLQ)    |
                                   +------------+
```

### 2. Multi-Process Atomic Job Claiming
When multiple workers run concurrently (even across separate terminal sessions), they query SQLite for available jobs.
- SQLite WAL mode + `BEGIN IMMEDIATE` transaction locking guarantees that only one worker can execute write transactions at a time.
- The transaction queries `WHERE (state = 'pending' OR state = 'failed') AND run_at <= NOW()` and updates the state to `processing` in a single isolated block.
- Other workers waiting on `BEGIN IMMEDIATE` receive database locked status or wait on busy timeout (5000ms), preventing any duplicate job execution.

### 3. Automatic SIGKILL Crash Recovery (< 15s Delay)
If a worker process is terminated abruptly via `SIGKILL`:
1. No cleanup handlers can execute; the job is left in `processing` state in the database.
2. Active workers update a `heartbeat_at` timestamp every 2 seconds while running jobs.
3. Every time any worker executes `claim_job()`, it performs a stale job recovery scan:
   - Queries `WHERE state = 'processing' AND heartbeat_at < NOW - 10s`.
   - Any job whose heartbeat is older than 10 seconds is flagged as crashed.
   - If `attempts < max_retries`, the job is reset to `pending` with `run_at = NOW()`.
   - If `attempts >= max_retries`, the job is moved to `dead` (DLQ).
4. The job is immediately reclaimed and executed by an active worker. Total worst-case recovery time is **under 15 seconds**.

---

## 🧪 Running Automated Tests

Run the built-in test suite covering all 5 evaluation scenarios:

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

### Test Output
```
test_scenario_1_basic_job_completion ... ok
test_scenario_2_failing_job_and_dlq ... ok
test_scenario_3_multi_worker_concurrency ... ok
test_scenario_4_sigkill_crash_recovery ... ok
test_scenario_5_persistence_across_restart ... ok

----------------------------------------------------------------------
Ran 5 tests in 1.492s

OK
```
