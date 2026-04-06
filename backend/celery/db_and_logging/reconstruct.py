#!/usr/bin/env python3
"""
Exports ALL runs into reconstruct.json in this shape:
 
[
  {
    "run_id": "...",
    "car": "MH49BB1307" | null,
    "run_summary": {
      "start_ts": "...",
      "end_ts": "...",
      "total_duration_ms": ...,
      "status": "FAILED|SUCCESS|running|..."
    },
    "steps": [...],
    "akamai_events": [...]
  },
  ...
]
"""
 
import json
import psycopg2
import psycopg2.extras
 
# -----------------------------
# DB CONFIG
# -----------------------------
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "insurance_v2"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
 
OUT_FILE = "reconstruct.json"
 
 
# -----------------------------
# DB HELPERS
# -----------------------------
def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
 
 
def fetch_one(cur, query, params=None):
    cur.execute(query, params or ())
    return cur.fetchone()
 
 
def fetch_all(cur, query, params=None):
    cur.execute(query, params or ())
    return cur.fetchall()
 
 
def table_has_column(cur, table_name, column_name, table_schema="public"):
    row = fetch_one(
        cur,
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s AND column_name=%s
        LIMIT 1
        """,
        (table_schema, table_name, column_name),
    )
    return row is not None
 
 
def find_first_existing_column(cur, table_name, candidates, table_schema="public"):
    for c in candidates:
        if table_has_column(cur, table_name, c, table_schema=table_schema):
            return c
    return None
 
 
# -----------------------------
# LOADERS
# -----------------------------
def load_run_row(cur, run_id):
    return fetch_one(
        cur,
        """
        SELECT run_id, status, started_at, ended_at, total_duration_ms
        FROM scrape_runs
        WHERE run_id = %s
        """,
        (run_id,),
    )
 
 
def load_car_from_inputs(cur, run_id):
    # You showed: scrape_run_inputs(run_id UNIQUE, car_number NOT NULL, ...)
    if not table_has_column(cur, "scrape_run_inputs", "car_number"):
        return None
 
    row = fetch_one(
        cur,
        """
        SELECT car_number
        FROM scrape_run_inputs
        WHERE run_id = %s
        LIMIT 1
        """,
        (run_id,),
    )
    return row["car_number"] if row else None
 
 
def load_steps(cur, run_id):
    data_col = find_first_existing_column(cur, "run_logs", ["data_json", "data", "payload", "meta"]) or "data_json"
 
    rows = fetch_all(
        cur,
        f"""
        SELECT step_number, step_key, status, section,
               start_ts, end_ts, duration_ms,
               {data_col} AS data
        FROM run_logs
        WHERE run_id = %s
        ORDER BY step_number
        """,
        (run_id,),
    )
 
    steps = []
    for r in rows:
        steps.append(
            {
                "step_number": r.get("step_number"),
                "step_key": r.get("step_key"),
                "status": r.get("status"),
                "start_ts": r["start_ts"].isoformat() if r.get("start_ts") else None,
                "section": r.get("section"),
                "data": r.get("data"),
                "end_ts": r["end_ts"].isoformat() if r.get("end_ts") else None,
                "duration_ms": r.get("duration_ms"),
            }
        )
    return steps
 
 
def load_akamai_events(cur, run_id):
    ts_col = find_first_existing_column(cur, "akamai_events", ["event_timestamp", "timestamp", "ts", "created_at"]) or "event_timestamp"
    data_col = find_first_existing_column(cur, "akamai_events", ["data_json", "data", "payload", "meta"]) or "data_json"
 
    rows = fetch_all(
        cur,
        f"""
        SELECT step_after, step_key_after,
               {ts_col} AS ts,
               {data_col} AS data
        FROM akamai_events
        WHERE run_id = %s
        ORDER BY id
        """,
        (run_id,),
    )
 
    events = []
    for r in rows:
        events.append(
            {
                "step_after": r.get("step_after"),
                "step_key_after": r.get("step_key_after"),
                "timestamp": r["ts"].isoformat() if r.get("ts") else None,
                "data": r.get("data"),
            }
        )
    return events
 
 
def build_run_object(cur, run_id):
    run_row = load_run_row(cur, run_id)
    if not run_row:
        return None
 
    run_summary = {
        "start_ts": run_row["started_at"].isoformat() if run_row.get("started_at") else None,
        "end_ts": run_row["ended_at"].isoformat() if run_row.get("ended_at") else None,
        "total_duration_ms": run_row.get("total_duration_ms"),
        "status": run_row.get("status"),
    }
 
    return {
        "run_id": run_row["run_id"],
        "car": load_car_from_inputs(cur, run_id),  # <-- main fix
        "run_summary": run_summary,
        "steps": load_steps(cur, run_id),
        "akamai_events": load_akamai_events(cur, run_id),
    }
 
 
# -----------------------------
# MAIN
# -----------------------------
def dump_all(output_path=OUT_FILE):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            run_id_rows = fetch_all(cur, "SELECT DISTINCT run_id FROM scrape_runs ORDER BY run_id")
            run_ids = [r["run_id"] for r in run_id_rows]
 
            runs = []
            for run_id in run_ids:
                obj = build_run_object(cur, run_id)
                if obj:
                    runs.append(obj)
 
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(runs, f, indent=2, ensure_ascii=False, default=str)
 
        print(f"[RECONSTRUCT] wrote {len(runs)} runs -> {output_path}")
    finally:
        conn.close()
 
 
if __name__ == "__main__":
    dump_all("reconstruct.json")
 
 