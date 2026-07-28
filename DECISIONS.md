# QueueCTL Architectural Decisions & Design Rationale

This document addresses the five core architectural questions for QueueCTL, outlining the trade-offs, guarantees, and technical design decisions.

---

### Question 1: Atomic Job Claiming Across OS Processes

**Exact Line References:**  
File: [`queuectl/db.py`] 
- **Line 142**: `conn.execute("BEGIN IMMEDIATE")`
- **Lines 166–175**: `SELECT id ... FROM jobs WHERE (state = 'pending' OR state = 'failed') AND run_at <= ? ORDER BY created_at ASC LIMIT 1`
- **Lines 185–196**: `UPDATE jobs SET state = 'processing', attempts = ?, updated_at = ?, worker_id = ?, heartbeat_at = ? WHERE id = ?`
- **Line 199**: `conn.execute("COMMIT")`

#### Why this operation is atomic across separate OS processes:
SQLite relies on file-level locks provided by the operating system kernel. When a process issues `BEGIN IMMEDIATE`:
1. SQLite acquires a **RESERVED lock** on the database file.
2. In SQLite's locking model (especially in Write-Ahead Logging / WAL mode), only **one process at a time** can hold a RESERVED or EXCLUSIVE write transaction lock.
3. If Worker A issues `BEGIN IMMEDIATE` at line 142, any concurrent Worker B attempting to run `BEGIN IMMEDIATE` is blocked by SQLite's kernel file lock until Worker A completes its `COMMIT` at line 199.
4. During this locked transaction, Worker A reads the oldest eligible job, updates its state to `processing`, increments `attempts`, assigns its `worker_id` and initial `heartbeat_at`, and commits.
5. When Worker B subsequently enters `BEGIN IMMEDIATE`, the job claimed by Worker A is already in `processing` state and is no longer returned by Worker B's query. This guarantees **zero double-claiming** across independent OS processes and terminals.

---

### Question 2: SIGKILL Crash Recovery Walkthrough

#### Step-by-Step Scenario:
Suppose Worker A claims Job `job-101` and is killed with `SIGKILL` mid-job.

1. **State at time of SIGKILL**:
   - Job `job-101` has `state = 'processing'`, `attempts = 1`, and `heartbeat_at = "2026-07-27T16:00:00Z"`.
   - Because `SIGKILL` abruptly terminates the OS process, no Python signal handler or cleanup code runs. The job remains in `processing` state in SQLite.

2. **Detection & Recovery Mechanism**:
   - Active workers run periodic heartbeat updates (every 2 seconds) while processing jobs.
   - When any active worker attempts to claim a job (`db.claim_job()`), lines 145–163 of [`queuectl/db.py`] perform a **Stale Job Recovery Scan**:
     ```sql
     SELECT id, attempts, max_retries FROM jobs
     WHERE state = 'processing' AND (heartbeat_at IS NULL OR heartbeat_at < ?);
     ```
   - The stale cutoff threshold is set to **10 seconds** prior to current time. Since Worker A is dead, its heartbeat stopped updating.
   - Once current time reaches `16:00:10Z` (> 10s after last heartbeat), the scanner detects `job-101`.

3. **State Transition**:
   - If `attempts < max_retries`: `job-101` state is updated back to `pending`, `run_at = now()`, `worker_id = NULL`, and `heartbeat_at = NULL`.
   - If `attempts >= max_retries`: `job-101` is moved to `dead` (DLQ).

4. **Re-execution**:
   - Immediately following the recovery step within the same `BEGIN IMMEDIATE` transaction, lines 166–175 select `job-101` (now `pending`), and transition it to `processing` under Worker B.

#### Worst-Case Delay Before Recovery:
- **Heartbeat Interval**: 2.0 seconds.
- **Stale Cutoff Threshold**: 10.0 seconds.
- **Worker Poll Sleep Interval**: 0.5 seconds.
- **Worst-Case Recovery Delay**: **~10.5 to 12.0 seconds** (well within the required < 60 seconds limit).

---

### Question 3: DLQ Retry Behavior (`dlq retry <id>`)

#### Does `dlq retry` reset `attempts`?
**Yes**, `queuectl dlq retry <id>` resets `attempts` to `0`.

#### Rationale & Justification:
- When a job lands in the Dead Letter Queue (`dead`), it has exhausted all automated exponential backoff retry attempts (e.g., 3 failed attempts).
- Moving a job out of DLQ represents human or administrative intervention (e.g., fixing an external dependency, updating configuration, or resolving a network failure).
- If `attempts` were NOT reset:
  1. The next single transient failure would immediately move the job right back to `dead` state without giving it the benefit of configured retry attempts.
  2. The exponential backoff formula (`delay = base ^ attempts`) would compute an excessively long delay for the first retry attempt after manual intervention.
- Resetting `attempts = 0` provides the job with a fresh lifecycle under the configured `max_retries` policy, ensuring predictable exponential backoff starting at $base^1$ seconds.

---

### Question 4: Considered & Rejected Designs for `worker stop`

To gracefully stop workers from another terminal, `queuectl worker stop` must communicate cross-process.

| Considered Design | Reason for Rejection |
| :--- | :--- |
| OS Signals (`SIGTERM` via PID file) | Rejected : Required maintaining a central `workers.pid` registry file. Prone to stale PID files after system reboots or hard worker crashes, causing `kill` signals to be sent to unrelated processes. Complex on Windows due to different signal semantics. |
| Unix Domain Sockets / IPC Control Server | Rejected : Required spawning and maintaining a dedicated daemon/server process to listen on a socket file. High complexity, OS portability issues (Windows named pipes vs Unix domain sockets), and single point of failure. |
| Shared Database Control Table (Chosen) | ACCEPTED : Uses a lightweight `worker_control` table in SQLite. Running `queuectl worker stop` executes `UPDATE worker_control SET value = '1' WHERE key = 'stop_signal'`. Active workers check this flag on each loop iteration. Cross-platform, zero extra processes, persistent, and crash-safe. |

---

### Question 5: Impact of Adding Job Priorities

If job priorities were added (e.g., `--priority high` where high-priority jobs jump the queue):

#### Parts of Design that Survive Unchanged:
1. **Concurrency & Atomic Claiming**: `BEGIN IMMEDIATE` transaction locking model in SQLite (`queuectl/db.py`) remains 100% unchanged.
2. **Worker Subprocess Execution & Heartbeats**: Background job execution, subprocess invocation, heartbeat loop, and exit-code evaluation survive intact.
3. **Crash Recovery & DLQ Logic**: Heartbeat monitoring, stale processing job recovery, and exponential backoff calculations operate identically regardless of priority.
4. **Configuration & Worker Signaling**: Worker stop signaling and configuration persistence require no modifications.

#### Parts of Design that Break / Must Change:
1. **Database Schema**: The `jobs` table requires a new `priority INTEGER NOT NULL DEFAULT 0` column.
2. **Database Indices**: The composite index `idx_jobs_state_run ON jobs(state, run_at)` breaks query performance and must be updated to `CREATE INDEX idx_jobs_priority ON jobs(priority DESC, run_at ASC, created_at ASC);`.
3. **Claim Query (`claim_job`)**: The `SELECT` query in `claim_job` must change from:
   ```sql
   ORDER BY created_at ASC
   ```
   to:
   ```sql
   ORDER BY priority DESC, created_at ASC
   ```
4. **CLI Contract**: `queuectl enqueue` must be expanded to accept `--priority` flags or JSON field parsing.
