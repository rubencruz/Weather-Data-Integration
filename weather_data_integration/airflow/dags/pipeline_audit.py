"""Small structured audit helper for the local Airflow demonstration.

In a managed production environment this event stream can be redirected to a
central observability platform or an operational database without changing the DAG contract.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

AUDIT_FILE = Path(os.getenv("WEATHER_AUDIT_FILE", "/opt/airflow/logs/weather_pipeline_audit.jsonl"))


def write_audit_event(event: str, context: dict) -> None:
    AUDIT_FILE.parent.mkdir(parents=True, exist_ok=True)
    ti = context.get("task_instance")
    dag = context.get("dag")
    payload = {
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "event": event,
        "dag_id": getattr(dag, "dag_id", None),
        "task_id": getattr(ti, "task_id", None),
        "run_id": getattr(ti, "run_id", None),
        "try_number": getattr(ti, "try_number", None),
    }
    with AUDIT_FILE.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")
