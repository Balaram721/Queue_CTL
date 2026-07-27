"""
Database storage and atomic lock implementation using SQLite in WAL mode.
"""

import os
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any, Tuple
from queuectl.models import Job, JobState, current_iso_time, parse_iso_time

DEFAULT_DB_PATH = os.environ.get("QUEUECTL_DB", "queuectl.db")

def get_db_connection(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """
    Establishes a connection to SQLite database with Write-Ahead Logging (WAL)
    mode enabled, autocommit isolation level, and busy timeout set for robust cross-process concurrency.
    """
    conn = sqlite3.connect(db_path, timeout=10.0, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn

def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    """Initializes tables, indices, and default configurations."""
    conn = get_db_connection(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                state TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 3,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                run_at TEXT NOT NULL,
                worker_id TEXT,
                heartbeat_at TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_state_run ON jobs(state, run_at);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at);")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS worker_control (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

        # Set default configs if not already set
        conn.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('max-retries', '3')")
        conn.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('backoff-base', '2')")
        conn.execute("INSERT OR IGNORE INTO worker_control (key, value) VALUES ('stop_signal', '0')")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def row_to_job(row: Tuple) -> Job:
    """Converts SQLite database row tuple to Job instance."""
    return Job(
        id=row[0],
        command=row[1],
        state=row[2],
        attempts=row[3],
        max_retries=row[4],
        created_at=row[5],
        updated_at=row[6],
        run_at=row[7],
        worker_id=row[8],
        heartbeat_at=row[9]
    )


class Database:
    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        init_db(self.db_path)

    def get_connection(self) -> sqlite3.Connection:
        return get_db_connection(self.db_path)

    def enqueue_job(self, job: Job) -> Job:
        """Enqueues a new job into persistent storage."""
        conn = self.get_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, run_at, worker_id, heartbeat_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.id,
                    job.command,
                    job.state,
                    job.attempts,
                    job.max_retries,
                    job.created_at,
                    job.updated_at,
                    job.run_at,
                    job.worker_id,
                    job.heartbeat_at
                )
            )
            conn.execute("COMMIT")
            return job
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def claim_job(self, worker_id: str, stale_seconds: float = 10.0) -> Optional[Job]:
        """
        Atomically claims the next eligible pending/failed job for execution.
        
        ATOMIC LOCKING GUARANTEE:
        Uses 'BEGIN IMMEDIATE' SQLite transaction. This acquires a RESERVED database lock,
        preventing any other process/worker from entering write mode or claiming jobs concurrently.
        """
        conn = self.get_connection()
        try:
            now_iso = current_iso_time()
            now_dt = parse_iso_time(now_iso)
            stale_cutoff = (now_dt - timedelta(seconds=stale_seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")

            conn.execute("BEGIN IMMEDIATE")

            # Crash Recovery Check: Recover jobs stuck in 'processing' with outdated heartbeat
            cursor = conn.execute(
                """
                SELECT id, attempts, max_retries FROM jobs
                WHERE state = 'processing' AND (heartbeat_at IS NULL OR heartbeat_at < ?)
                """,
                (stale_cutoff,)
            )
            stale_rows = cursor.fetchall()
            for s_id, s_attempts, s_max in stale_rows:
                if s_attempts >= s_max:
                    conn.execute(
                        "UPDATE jobs SET state = 'dead', updated_at = ?, worker_id = NULL, heartbeat_at = NULL WHERE id = ?",
                        (now_iso, s_id)
                    )
                else:
                    conn.execute(
                        "UPDATE jobs SET state = 'pending', run_at = ?, updated_at = ?, worker_id = NULL, heartbeat_at = NULL WHERE id = ?",
                        (now_iso, now_iso, s_id)
                    )

            # Query for next available job
            cursor = conn.execute(
                """
                SELECT id, command, state, attempts, max_retries, created_at, updated_at, run_at, worker_id, heartbeat_at
                FROM jobs
                WHERE (state = 'pending' OR state = 'failed') AND run_at <= ?
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (now_iso,)
            )
            row = cursor.fetchone()
            if not row:
                conn.execute("COMMIT")
                return None

            candidate = row_to_job(row)
            new_attempts = candidate.attempts + 1

            # Atomic claim update
            conn.execute(
                """
                UPDATE jobs
                SET state = 'processing',
                    attempts = ?,
                    updated_at = ?,
                    worker_id = ?,
                    heartbeat_at = ?
                WHERE id = ?
                """,
                (new_attempts, now_iso, worker_id, now_iso, candidate.id)
            )

            conn.execute("COMMIT")

            candidate.state = JobState.PROCESSING.value
            candidate.attempts = new_attempts
            candidate.updated_at = now_iso
            candidate.worker_id = worker_id
            candidate.heartbeat_at = now_iso
            return candidate
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def update_heartbeat(self, job_id: str, worker_id: str) -> None:
        """Updates heartbeat timestamp for an in-flight processing job."""
        conn = self.get_connection()
        try:
            now_iso = current_iso_time()
            conn.execute(
                """
                UPDATE jobs
                SET heartbeat_at = ?, updated_at = ?
                WHERE id = ? AND worker_id = ? AND state = 'processing'
                """,
                (now_iso, now_iso, job_id, worker_id)
            )
        finally:
            conn.close()

    def complete_job(self, job_id: str) -> None:
        """Marks a job as successfully completed."""
        conn = self.get_connection()
        try:
            now_iso = current_iso_time()
            conn.execute(
                """
                UPDATE jobs
                SET state = 'completed', updated_at = ?, worker_id = NULL, heartbeat_at = NULL
                WHERE id = ?
                """,
                (now_iso, job_id)
            )
        finally:
            conn.close()

    def fail_job(self, job_id: str, backoff_base: float = 2.0) -> str:
        """
        Handles job failure. If attempts >= max_retries, transitions to 'dead' (DLQ).
        Otherwise transitions to 'failed' with exponential backoff delay.
        Returns the new state ('failed' or 'dead').
        """
        conn = self.get_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                "SELECT attempts, max_retries FROM jobs WHERE id = ?", (job_id,)
            )
            row = cursor.fetchone()
            if not row:
                conn.execute("COMMIT")
                return JobState.FAILED.value

            attempts, max_retries = row[0], row[1]
            now_dt = datetime.now(timezone.utc)
            now_iso = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            if attempts >= max_retries:
                new_state = JobState.DEAD.value
                conn.execute(
                    """
                    UPDATE jobs
                    SET state = ?, updated_at = ?, worker_id = NULL, heartbeat_at = NULL
                    WHERE id = ?
                    """,
                    (new_state, now_iso, job_id)
                )
            else:
                new_state = JobState.FAILED.value
                delay_seconds = float(backoff_base) ** attempts
                run_at_dt = now_dt + timedelta(seconds=delay_seconds)
                run_at_iso = run_at_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

                conn.execute(
                    """
                    UPDATE jobs
                    SET state = ?, run_at = ?, updated_at = ?, worker_id = NULL, heartbeat_at = NULL
                    WHERE id = ?
                    """,
                    (new_state, run_at_iso, now_iso, job_id)
                )

            conn.execute("COMMIT")
            return new_state
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def get_job(self, job_id: str) -> Optional[Job]:
        """Retrieves job by ID."""
        conn = self.get_connection()
        try:
            cursor = conn.execute(
                "SELECT id, command, state, attempts, max_retries, created_at, updated_at, run_at, worker_id, heartbeat_at FROM jobs WHERE id = ?",
                (job_id,)
            )
            row = cursor.fetchone()
            return row_to_job(row) if row else None
        finally:
            conn.close()

    def list_jobs(self, state: Optional[str] = None) -> List[Job]:
        """Lists jobs optionally filtered by state."""
        conn = self.get_connection()
        try:
            if state and state.lower() != "all":
                cursor = conn.execute(
                    "SELECT id, command, state, attempts, max_retries, created_at, updated_at, run_at, worker_id, heartbeat_at FROM jobs WHERE state = ? ORDER BY created_at ASC",
                    (state.lower(),)
                )
            else:
                cursor = conn.execute(
                    "SELECT id, command, state, attempts, max_retries, created_at, updated_at, run_at, worker_id, heartbeat_at FROM jobs ORDER BY created_at ASC"
                )
            rows = cursor.fetchall()
            return [row_to_job(r) for r in rows]
        finally:
            conn.close()

    def retry_dlq_job(self, job_id: str) -> bool:
        """
        Re-enqueues a dead job back to pending state.
        Resets attempts to 0 so it receives a fresh set of retry attempts.
        """
        conn = self.get_connection()
        try:
            now_iso = current_iso_time()
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute("SELECT state FROM jobs WHERE id = ?", (job_id,))
            row = cursor.fetchone()
            if not row or row[0] != JobState.DEAD.value:
                conn.execute("COMMIT")
                return False

            conn.execute(
                """
                UPDATE jobs
                SET state = 'pending', attempts = 0, run_at = ?, updated_at = ?, worker_id = NULL, heartbeat_at = NULL
                WHERE id = ?
                """,
                (now_iso, now_iso, job_id)
            )
            conn.execute("COMMIT")
            return True
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def set_config(self, key: str, value: str) -> None:
        """Sets a configuration value."""
        conn = self.get_connection()
        try:
            conn.execute(
                "INSERT INTO config (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = ?",
                (key, value, value)
            )
        finally:
            conn.close()

    def get_config(self, key: str, default: str = "") -> str:
        """Gets a configuration value."""
        conn = self.get_connection()
        try:
            cursor = conn.execute("SELECT value FROM config WHERE key = ?", (key,))
            row = cursor.fetchone()
            return row[0] if row else default
        finally:
            conn.close()

    def get_all_configs(self) -> Dict[str, str]:
        """Gets all configuration values."""
        conn = self.get_connection()
        try:
            cursor = conn.execute("SELECT key, value FROM config")
            return {r[0]: r[1] for r in cursor.fetchall()}
        finally:
            conn.close()

    def set_stop_signal(self, stop: bool) -> None:
        """Sets worker stop flag for cross-process worker shutdown."""
        conn = self.get_connection()
        try:
            val = "1" if stop else "0"
            conn.execute(
                "INSERT INTO worker_control (key, value) VALUES ('stop_signal', ?) ON CONFLICT(key) DO UPDATE SET value = ?",
                (val, val)
            )
        finally:
            conn.close()

    def get_stop_signal(self) -> bool:
        """Reads worker stop flag."""
        conn = self.get_connection()
        try:
            cursor = conn.execute("SELECT value FROM worker_control WHERE key = 'stop_signal'")
            row = cursor.fetchone()
            return row[0] == "1" if row else False
        finally:
            conn.close()

    def get_status_counts(self) -> Dict[str, int]:
        """Gets count of jobs in each state."""
        conn = self.get_connection()
        try:
            cursor = conn.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state")
            counts = {state.value: 0 for state in JobState}
            for state_val, cnt in cursor.fetchall():
                counts[state_val] = cnt
            return counts
        finally:
            conn.close()
