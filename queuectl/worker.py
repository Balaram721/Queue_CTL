import os
import sys
import time
import signal
import threading
import subprocess
from typing import List
from Queue_CTL.queuectl.db import Database, DEFAULT_DB_PATH
from queuectl.models import Job, JobState

# Global shutdown flag across worker threads
shutdown_requested = False


def signal_handler(signum, frame):
    global shutdown_requested
    sys.stderr.write(f"\n[queuectl worker] Received signal {signum}. Initiating graceful shutdown...\n")
    sys.stderr.flush()
    shutdown_requested = True


class WorkerRunner:
    def __init__(self, db_path: str = DEFAULT_DB_PATH, worker_id: str = ""):
        self.db_path = db_path
        self.db = Database(db_path)
        self.worker_id = worker_id or f"worker-{os.getpid()}-{threading.get_ident()}"

    def run_loop(self):
        global shutdown_requested
        while not shutdown_requested:
            if self.db.get_stop_signal():
                break

            job = self.db.claim_job(self.worker_id, stale_seconds=10.0)
            if not job:
                time.sleep(0.5)
                continue

            self.execute_job(job)

    def execute_job(self, job: Job) -> bool:
        stop_heartbeat = threading.Event()

        def heartbeat_loop():
            while not stop_heartbeat.wait(2.0):
                try:
                    self.db.update_heartbeat(job.id, self.worker_id)
                except Exception:
                    pass

        hb_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        hb_thread.start()

        try:
            # Execute command in shell
            res = subprocess.run(job.command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            exit_code = res.returncode

            stop_heartbeat.set()
            hb_thread.join(timeout=2.0)

            if exit_code == 0:
                self.db.complete_job(job.id)
                return True
            else:
                backoff_str = self.db.get_config("backoff-base", "2")
                try:
                    backoff_base = float(backoff_str)
                except ValueError:
                    backoff_base = 2.0
                self.db.fail_job(job.id, backoff_base=backoff_base)
                return False
        except Exception as e:
            stop_heartbeat.set()
            hb_thread.join(timeout=2.0)
            backoff_str = self.db.get_config("backoff-base", "2")
            try:
                backoff_base = float(backoff_str)
            except ValueError:
                backoff_base = 2.0
            self.db.fail_job(job.id, backoff_base=backoff_base)
            return False


def start_workers(count: int = 1, db_path: str = DEFAULT_DB_PATH):
    """
    Starts `count` worker threads in the foreground.
    Registers SIGINT and SIGTERM signal handlers for graceful shutdown.
    """
    global shutdown_requested
    shutdown_requested = False

    db = Database(db_path)
    db.set_stop_signal(False)

    # Register signal handlers
    try:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    except (ValueError, AttributeError):
        pass  # Windows thread signal handling fallback if needed

    sys.stderr.write(f"[queuectl worker] Starting {count} worker(s) in foreground (PID {os.getpid()}). Press Ctrl+C to stop.\n")
    sys.stderr.flush()

    threads: List[threading.Thread] = []

    for i in range(count):
        worker_id = f"worker-{os.getpid()}-w{i+1}"
        runner = WorkerRunner(db_path=db_path, worker_id=worker_id)
        t = threading.Thread(target=runner.run_loop, name=worker_id)
        t.start()
        threads.append(t)

    # Main thread monitoring loop
    try:
        while any(t.is_alive() for t in threads):
            if shutdown_requested or db.get_stop_signal():
                break
            time.sleep(0.5)
    except KeyboardInterrupt:
        shutdown_requested = True

    sys.stderr.write("[queuectl worker] Waiting for active jobs to finish...\n")
    sys.stderr.flush()

    for t in threads:
        t.join()

    sys.stderr.write("[queuectl worker] All workers stopped cleanly.\n")
    sys.stderr.flush()
