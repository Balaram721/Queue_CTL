from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, Any


class JobState(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"


def current_iso_time() -> str:
    """Returns current UTC timestamp in ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso_time(iso_str: str) -> datetime:
    """Parses ISO 8601 string to aware UTC datetime object."""
    if iso_str.endswith("Z"):
        iso_str = iso_str[:-1] + "+00:00"
    return datetime.fromisoformat(iso_str).astimezone(timezone.utc)

@dataclass
class Job:
    id: str
    command: str
    state: str = JobState.PENDING.value
    attempts: int = 0
    max_retries: int = 3
    created_at: str = ""
    updated_at: str = ""
    run_at: str = ""
    worker_id: Optional[str] = None
    heartbeat_at: Optional[str] = None

    def __post_init__(self):
        now = current_iso_time()
        if not self.created_at:
            self.created_at = now
        if not self.updated_at:
            self.updated_at = now
        if not self.run_at:
            self.run_at = self.created_at

    def to_dict(self, clean_for_output: bool = True) -> Dict[str, Any]:
        """Converts Job object to dictionary matching spec requirements."""
        data = {
            "id": self.id,
            "command": self.command,
            "state": self.state,
            "attempts": self.attempts,
            "max_retries": self.max_retries,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
        if not clean_for_output:
            data["run_at"] = self.run_at
            data["worker_id"] = self.worker_id
            data["heartbeat_at"] = self.heartbeat_at
        return data

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Job":
        return cls(
            id=d["id"],
            command=d["command"],
            state=d.get("state", JobState.PENDING.value),
            attempts=d.get("attempts", 0),
            max_retries=d.get("max_retries", 3),
            created_at=d.get("created_at", ""),
            updated_at=d.get("updated_at", ""),
            run_at=d.get("run_at", ""),
            worker_id=d.get("worker_id"),
            heartbeat_at=d.get("heartbeat_at"),
        )