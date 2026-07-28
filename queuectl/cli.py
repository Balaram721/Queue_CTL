"""
Command Line Interface (CLI) implementation for QueueCTL.
"""

import sys
import os
import re
import json
import argparse
from typing import List, Optional
from Queue_CTL.queuectl.queue import QueueManager
from Queue_CTL.queuectl.worker import start_workers


def parse_enqueue_arg(raw_arg: str) -> dict:
    """
    Parses raw JSON string argument for queuectl enqueue.
    Handles standard JSON and PowerShell-mangled unquoted JSON representations.
    """
    if not raw_arg:
        return {}

    # Attempt 1: Standard JSON parse
    try:
        data = json.loads(raw_arg)
        if isinstance(data, dict):
            return data
    except (json.JSONDecodeError, TypeError):
        pass

    # Attempt 2: Repair quote-stripped JSON from Windows PowerShell (e.g., {id:job1,command:echo hello})
    trimmed = raw_arg.strip()
    if trimmed.startswith("{") and trimmed.endswith("}"):
        content = trimmed[1:-1]
        result = {}
        # Match key:value pairs
        pattern = r'(?:\s*)([a-zA-Z0-9_-]+)\s*:\s*([^,}]+)'
        matches = re.findall(pattern, content)
        if matches:
            for key, val in matches:
                val = val.strip().strip("'\"")
                result[key.strip()] = val
            if "command" in result or "id" in result:
                return result

    # Attempt 3: Treat raw argument as shell command
    return {"command": raw_arg}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="queuectl",
        description="QueueCTL — Production-grade CLI background job queue system"
    )
    subparsers = parser.add_subparsers(dest="command_name", help="Command category")

    # --- ENQUEUE ---
    p_enqueue = subparsers.add_parser("enqueue", help="Add a new job to the queue")
    p_enqueue.add_argument("payload", nargs="?", help="JSON object string e.g. '{\"id\":\"job1\",\"command\":\"sleep 2\"}' or shell command string")
    p_enqueue.add_argument("--id", dest="job_id", help="Job ID")
    p_enqueue.add_argument("--command", dest="job_command", help="Command to execute")
    p_enqueue.add_argument("--max-retries", dest="max_retries", type=int, help="Max retry count")

    # --- WORKER ---
    p_worker = subparsers.add_parser("worker", help="Manage background worker processes")
    worker_sub = p_worker.add_subparsers(dest="worker_action", help="Worker subcommands")
    
    p_w_start = worker_sub.add_parser("start", help="Start workers in foreground")
    p_w_start.add_argument("--count", type=int, default=1, help="Number of worker processes/threads (default: 1)")

    p_w_stop = worker_sub.add_parser("stop", help="Gracefully stop running workers from another terminal")

    # --- STATUS ---
    subparsers.add_parser("status", help="Summary of job states and active workers")

    # --- LIST ---
    p_list = subparsers.add_parser("list", help="List jobs by state")
    p_list.add_argument("--state", choices=["pending", "processing", "completed", "failed", "dead", "all"], default="all", help="Job state filter")
    p_list.add_argument("--json", action="store_true", help="Output JSON array")

    # --- DLQ ---
    p_dlq = subparsers.add_parser("dlq", help="Dead Letter Queue management")
    dlq_sub = p_dlq.add_subparsers(dest="dlq_action", help="DLQ subcommands")
    
    p_dlq_list = dlq_sub.add_parser("list", help="List dead letter queue jobs")
    p_dlq_list.add_argument("--json", action="store_true", help="Output JSON array")

    p_dlq_retry = dlq_sub.add_parser("retry", help="Retry a job from DLQ")
    p_dlq_retry.add_argument("job_id", help="Job ID to retry")

    # --- CONFIG ---
    p_config = subparsers.add_parser("config", help="Manage queuectl configuration")
    config_sub = p_config.add_subparsers(dest="config_action", help="Config subcommands")
    
    p_cfg_set = config_sub.add_parser("set", help="Set configuration key-value")
    p_cfg_set.add_argument("key", help="Config key (e.g. max-retries, backoff-base)")
    p_cfg_set.add_argument("value", help="Config value")

    p_cfg_get = config_sub.add_parser("get", help="Get configuration value")
    p_cfg_get.add_argument("key", help="Config key")

    p_cfg_list = config_sub.add_parser("list", help="List all configurations")

    return parser


def main(cli_args: Optional[List[str]] = None):
    parser = build_parser()
    args = parser.parse_args(cli_args)
    qm = QueueManager()

    if args.command_name == "enqueue":
        cmd = None
        job_id = args.job_id
        max_retries = args.max_retries

        if args.payload:
            parsed = parse_enqueue_arg(args.payload)
            if "command" in parsed:
                cmd = parsed["command"]
            if "id" in parsed and not job_id:
                job_id = parsed["id"]
            if "max_retries" in parsed and max_retries is None:
                max_retries = int(parsed["max_retries"])
            if "max-retries" in parsed and max_retries is None:
                max_retries = int(parsed["max-retries"])

        if not cmd and args.job_command:
            cmd = args.job_command

        if not cmd:
            sys.stderr.write("Error: Command to execute must be specified.\n")
            sys.exit(1)

        job = qm.enqueue(command=cmd, job_id=job_id, max_retries=max_retries)
        sys.stdout.write(json.dumps(job.to_dict(), indent=2) + "\n")

    elif args.command_name == "worker":
        if args.worker_action == "start":
            start_workers(count=args.count, db_path=qm.db.db_path)
        elif args.worker_action == "stop":
            qm.set_worker_stop()
            sys.stdout.write("Worker stop signal recorded successfully.\n")
        else:
            parser.parse_args(["worker", "--help"])

    elif args.command_name == "status":
        st = qm.get_status()
        sys.stdout.write("QueueCTL Status:\n")
        sys.stdout.write("----------------\n")
        for state, count in st["jobs"].items():
            sys.stdout.write(f"  {state:<12}: {count}\n")
        sys.stdout.write(f"  Stop Signaled : {st['stop_signaled']}\n")

    elif args.command_name == "list":
        jobs = qm.list_jobs(state=args.state)
        if args.json:
            json_output = json.dumps([j.to_dict() for j in jobs])
            sys.stdout.write(json_output + "\n")
        else:
            if not jobs:
                sys.stdout.write("No jobs found.\n")
            else:
                for j in jobs:
                    sys.stdout.write(f"ID: {j.id:<15} State: {j.state:<10} Attempts: {j.attempts}/{j.max_retries} Command: '{j.command}'\n")

    elif args.command_name == "dlq":
        if args.dlq_action == "list":
            jobs = qm.list_jobs(state="dead")
            if args.json:
                json_output = json.dumps([j.to_dict() for j in jobs])
                sys.stdout.write(json_output + "\n")
            else:
                if not jobs:
                    sys.stdout.write("No dead letter queue jobs.\n")
                else:
                    for j in jobs:
                        sys.stdout.write(f"ID: {j.id:<15} Attempts: {j.attempts}/{j.max_retries} Command: '{j.command}'\n")
        elif args.dlq_action == "retry":
            success = qm.retry_dlq(args.job_id)
            if success:
                sys.stdout.write(f"Job '{args.job_id}' re-enqueued successfully from DLQ.\n")
            else:
                sys.stderr.write(f"Error: Job '{args.job_id}' not found in DLQ.\n")
                sys.exit(1)
        else:
            parser.parse_args(["dlq", "--help"])

    elif args.command_name == "config":
        if args.config_action == "set":
            qm.set_config(args.key, args.value)
            sys.stdout.write(f"Configuration '{args.key}' set to '{args.value}'.\n")
        elif args.config_action == "get":
            val = qm.get_config(args.key)
            sys.stdout.write(f"{args.key} = {val}\n")
        elif args.config_action == "list":
            cfgs = qm.list_configs()
            for k, v in cfgs.items():
                sys.stdout.write(f"{k} = {v}\n")
        else:
            parser.parse_args(["config", "--help"])

    else:
        parser.print_help()

if __name__ == "__main__":
    main()