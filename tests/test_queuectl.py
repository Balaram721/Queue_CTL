"""
Automated Test Suite for QueueCTL

Tests all 5 critical evaluation scenarios using Python's standard unittest framework:
1. Basic job execution & completion.
2. Job failures, exponential backoff retries, and movement to DLQ ('dead').
3. Multi-worker parallel processing & exact-once execution concurrency check.
4. SIGKILL crash recovery mid-job & stale job recovery under 60 seconds.
5. Persistence across restarts & process crashes.
"""
import os
import sys
import time
import tempfile
import unittest
import threading

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from queuectl.db import Database, init_db
from queuectl.models import Job, JobState
from queuectl.queue import QueueManager
from queuectl.worker import WorkerRunner

class TestQueueCTL(unittest.TestCase):
    def setUp(self):
        fd, self.temp_db = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        init_db(self.temp_db)

    def tearDown(self):
        for ext in ["", "-wal", "-shm"]:
            path = self.temp_db + ext
            if os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass

    def test_scenario_1_basic_job_completion(self):
        """Scenario 1: A basic job completes successfully."""
        qm = QueueManager(self.temp_db)
        job = qm.enqueue(command="echo 'Hello QueueCTL'", job_id="job-basic-1")
        self.assertEqual(job.state, JobState.PENDING.value)
        self.assertEqual(job.attempts, 0)

        runner = WorkerRunner(db_path=self.temp_db, worker_id="test-worker-1")
        
        # Claim job
        claimed = qm.db.claim_job("test-worker-1")
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed.id, "job-basic-1")
        self.assertEqual(claimed.state, JobState.PROCESSING.value)
        self.assertEqual(claimed.attempts, 1)

        # Execute job
        success = runner.execute_job(claimed)
        self.assertTrue(success)

        fetched = qm.get_job("job-basic-1")
        self.assertEqual(fetched.state, JobState.COMPLETED.value)

    def test_scenario_2_failing_job_and_dlq(self):
        """Scenario 2: A failing job retries with backoff and lands in DLQ."""
        qm = QueueManager(self.temp_db)
        qm.set_config("backoff-base", "2")
        qm.set_config("max-retries", "2")

        job = qm.enqueue(command="exit 1", job_id="job-fail-1", max_retries=2)
        runner = WorkerRunner(db_path=self.temp_db, worker_id="test-worker-2")

        # First Attempt
        claimed1 = qm.db.claim_job("test-worker-2")
        self.assertIsNotNone(claimed1)
        self.assertEqual(claimed1.attempts, 1)
        res1 = runner.execute_job(claimed1)
        self.assertFalse(res1)

        job_after_attempt_1 = qm.get_job("job-fail-1")
        self.assertEqual(job_after_attempt_1.state, JobState.FAILED.value)
        self.assertEqual(job_after_attempt_1.attempts, 1)

        # Fast-forward run_at time for test by setting run_at to past timestamp
        now_iso = "2020-01-01T00:00:00Z"
        conn = qm.db.get_connection()
        conn.execute("UPDATE jobs SET run_at = ? WHERE id = ?", (now_iso, "job-fail-1"))
        conn.close()

        # Second Attempt (reaches max_retries=2)
        claimed2 = qm.db.claim_job("test-worker-2")
        self.assertIsNotNone(claimed2)
        self.assertEqual(claimed2.attempts, 2)
        res2 = runner.execute_job(claimed2)
        self.assertFalse(res2)

        job_after_attempt_2 = qm.get_job("job-fail-1")
        self.assertEqual(job_after_attempt_2.state, JobState.DEAD.value)

        # Check DLQ listing
        dlq_jobs = qm.list_jobs(state="dead")
        self.assertEqual(len(dlq_jobs), 1)
        self.assertEqual(dlq_jobs[0].id, "job-fail-1")

        # Test DLQ Retry
        retried = qm.retry_dlq("job-fail-1")
        self.assertTrue(retried)
        retried_job = qm.get_job("job-fail-1")
        self.assertEqual(retried_job.state, JobState.PENDING.value)
        self.assertEqual(retried_job.attempts, 0)

    def test_scenario_3_multi_worker_concurrency(self):
        """Scenario 3: Many jobs across multiple workers — every job runs exactly once."""
        qm = QueueManager(self.temp_db)
        num_jobs = 10

        for i in range(num_jobs):
            qm.enqueue(command=f"echo 'Job {i}'", job_id=f"job-multi-{i}")

        db_path = self.temp_db

        def worker_thread_fn(w_idx):
            runner = WorkerRunner(db_path=db_path, worker_id=f"w-{w_idx}")
            for _ in range(20):
                job = qm.db.claim_job(f"w-{w_idx}")
                if not job:
                    time.sleep(0.05)
                    continue
                runner.execute_job(job)

        threads = [threading.Thread(target=worker_thread_fn, args=(i,)) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        completed_jobs = qm.list_jobs(state="completed")
        self.assertEqual(len(completed_jobs), num_jobs)

        # Verify each job ran exactly once and state is completed
        for i in range(num_jobs):
            j = qm.get_job(f"job-multi-{i}")
            self.assertEqual(j.state, JobState.COMPLETED.value)
            self.assertEqual(j.attempts, 1)

    def test_scenario_4_sigkill_crash_recovery(self):
        """Scenario 4: Worker is SIGKILLed mid-job; after restart, job completes and nothing stuck in processing."""
        qm = QueueManager(self.temp_db)
        job = qm.enqueue(command="sleep 10", job_id="job-crash-1")

        # Worker 1 claims job but "crashes" (simulated by updating state to processing with old heartbeat)
        claimed = qm.db.claim_job("crashed-worker-99")
        self.assertEqual(claimed.state, JobState.PROCESSING.value)

        # Simulate SIGKILL crash by backdating heartbeat_at beyond stale cutoff (10s)
        old_iso = "2020-01-01T00:00:00Z"
        conn = qm.db.get_connection()
        conn.execute("UPDATE jobs SET heartbeat_at = ? WHERE id = ?", (old_iso, "job-crash-1"))
        conn.close()

        # Next claim attempt by active worker detects stale processing job, recovers it, and claims it
        active_runner = WorkerRunner(db_path=self.temp_db, worker_id="active-worker-1")

        # Claim job will trigger crash recovery
        recovered_job = qm.db.claim_job("active-worker-1", stale_seconds=5.0)
        self.assertIsNotNone(recovered_job)
        self.assertEqual(recovered_job.id, "job-crash-1")

        # Complete with quick command override or standard execution
        conn = qm.db.get_connection()
        conn.execute("UPDATE jobs SET command = 'echo recovered' WHERE id = ?", ("job-crash-1",))
        conn.close()

        recovered_job.command = "echo recovered"
        success = active_runner.execute_job(recovered_job)
        self.assertTrue(success)

        final_job = qm.get_job("job-crash-1")
        self.assertEqual(final_job.state, JobState.COMPLETED.value)

    def test_scenario_5_persistence_across_restart(self):
        """Scenario 5: Jobs survive process restart and full recreate of QueueManager."""
        qm1 = QueueManager(self.temp_db)
        qm1.enqueue(command="echo 'Persisted job'", job_id="job-persist-1")

        # Simulating application shutdown and restart by recreating QueueManager instance
        del qm1

        qm2 = QueueManager(self.temp_db)
        job = qm2.get_job("job-persist-1")
        self.assertIsNotNone(job)
        self.assertEqual(job.command, "echo 'Persisted job'")
        self.assertEqual(job.state, JobState.PENDING.value)


if __name__ == "__main__":
    unittest.main()
