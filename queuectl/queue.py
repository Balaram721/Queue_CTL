import uuid
from typing import List, Optional, Dict, Any
from queuectl.db import Database, DEFAULT_DB_PATH
from queuectl.models import Job, JobState, current_iso_time

class QueueManager:
    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db = Database(db_path)

    def enqueue(self, command: str, job_id: Optional[str] = None, max_retries: Optional[int] = None) -> Job:
        """Enqueues a new background job."""
        if not job_id:
            job_id = f"job-{uuid.uuid4().hex[:8]}"

        if max_retries is None:
            config_val = self.db.get_config("max-retries", "3")
            try:
                max_retries = int(config_val)
            except ValueError:
                max_retries = 3

        now_iso = current_iso_time()
        job = Job(
            id=job_id,
            command=command,
            state=JobState.PENDING.value,
            attempts=0,
            max_retries=max_retries,
            created_at=now_iso,
            updated_at=now_iso,
            run_at=now_iso
        )

        return self.db.enqueue_job(job)

    def get_job(self, job_id: str) -> Optional[Job]:
        return self.db.get_job(job_id)

    def list_jobs(self, state: Optional[str] = None) -> List[Job]:
        return self.db.list_jobs(state)

    def retry_dlq(self, job_id: str) -> bool:
        return self.db.retry_dlq_job(job_id)

    def get_status(self) -> Dict[str, Any]:
        counts = self.db.get_status_counts()
        return {
            "jobs": counts,
            "stop_signaled": self.db.get_stop_signal()
        }

    def set_config(self, key: str, value: str) -> None:
        self.db.set_config(key, value)

    def get_config(self, key: str) -> str:
        return self.db.get_config(key, "")

    def list_configs(self) -> Dict[str, str]:
        return self.db.get_all_configs()

    def set_worker_stop(self) -> None:
        self.db.set_stop_signal(True)

    def clear_worker_stop(self) -> None:
        self.db.set_stop_signal(False)