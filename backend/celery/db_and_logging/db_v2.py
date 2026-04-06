import psycopg2
import psycopg2.extras
import json
from datetime import datetime
import uuid
import os
# ============================================
# DB CONNECTION
# ============================================
from dotenv import load_dotenv
load_dotenv()

DB_HOST     = os.environ.get("DB_HOST")
DB_PORT     = os.environ.get("DB_PORT")
DB_NAME     = os.environ.get("DB_NAME")
DB_USER     = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_SSL      = os.environ.get("DB_SSL", "false").lower() == "true"
DB_SSL_MODE = os.environ.get("DB_SSL_MODE", "require")


def get_connection():
    conn_params = {
        "host":     DB_HOST,
        "port":     DB_PORT,
        "dbname":   DB_NAME,
        "user":     DB_USER,
        "password": DB_PASSWORD,
    }
    if DB_SSL:
        conn_params["sslmode"] = DB_SSL_MODE
    return psycopg2.connect(**conn_params)


# ============================================
# HELPERS
# ============================================
def dict_cursor_execute(query, params=None, fetchone=False, fetchall=False):
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(query, params or ())
    result = None
    if fetchone:
        result = cur.fetchone()
    elif fetchall:
        result = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    return result


def safe_update(table, id_field, id_value, data):
    if not data:
        print(f"No data to update for {table} {id_field}={id_value}")
        return
    fields = []
    values = []
    for key, value in data.items():
        if isinstance(value, dict):
            value = json.dumps(value)
        fields.append(f"{key}=%s")
        values.append(value)
    values.append(datetime.now())
    values.append(id_value)
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute(f"""
        UPDATE {table}
        SET {', '.join(fields)}, updated_at=%s
        WHERE {id_field}=%s
    """, values)
    print(f"Rows updated in {table}: {cur.rowcount}")
    conn.commit()
    cur.close()
    conn.close()


# ============================================
# SCRAPE RUNS
# ============================================
def create_scrape_run(
    conn,
    run_id=None,
    status="pending",
    started_at=None,
    ended_at=None,
    notes=None,
    total_duration_ms=None,
):
    started_at = started_at or datetime.now()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO scrape_runs
        (run_id, status, started_at, ended_at, total_duration_ms, notes, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,NOW(),NOW())
        ON CONFLICT (run_id)
        DO UPDATE SET
            status            = EXCLUDED.status,
            ended_at          = EXCLUDED.ended_at,
            total_duration_ms = EXCLUDED.total_duration_ms,
            notes             = EXCLUDED.notes,
            updated_at        = NOW();
    """, (run_id, status, started_at, ended_at, total_duration_ms, notes))
    conn.commit()
    cur.close()
    return run_id


def get_scrape_run(run_id):
    return dict_cursor_execute("SELECT * FROM scrape_runs WHERE run_id=%s", (run_id.strip(),), fetchone=True)


def update_scrape_run_status(run_id, status):
    safe_update("scrape_runs", "run_id", run_id.strip(), {"status": status})


def delete_scrape_run(run_id):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("DELETE FROM scrape_runs WHERE run_id=%s", (run_id.strip(),))
    conn.commit()
    cur.close()
    conn.close()


# ============================================
# CAR INFO
# ============================================
def insert_car_info(run_id, data):
    run_id = run_id.strip()
    conn   = get_connection()
    cur    = conn.cursor()
    cur.execute("""
        INSERT INTO car_info
        (run_id, registration_number, make_name, model_name,
         vehicle_variant, fuel_type, cubic_capacity,
         state_code, city_tier, car_age, registration_date, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        run_id,
        data.get("registration_number") or data.get("car_number"),
        data.get("make_name"),
        data.get("model_name"),
        data.get("vehicle_variant"),
        data.get("fuel_type"),
        data.get("cubic_capacity"),
        data.get("state_code"),
        data.get("city_tier"),
        data.get("car_age"),
        data.get("registration_date"),
        datetime.now(),
        datetime.now(),
    ))
    conn.commit()
    cur.close()


def get_car_info(run_id):
    return dict_cursor_execute("SELECT * FROM car_info WHERE run_id=%s", (run_id.strip(),), fetchone=True)


def update_car_info(run_id, **kwargs):
    safe_update("car_info", "run_id", run_id.strip(), kwargs)


def delete_car_info(run_id):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("DELETE FROM car_info WHERE run_id=%s", (run_id.strip(),))
    conn.commit()
    cur.close()
    conn.close()


# ============================================
# SCRAPE RUN INPUTS
# ============================================
def insert_scrape_input(conn, run_id, data):
    run_id = run_id.strip()
    cur    = conn.cursor()
    cur.execute("""
        INSERT INTO scrape_run_inputs
        (run_id, car_number, policy_expiry, claim_status, phone, customer_name, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,NOW(),NOW())
        ON CONFLICT (run_id) DO NOTHING
    """, (
        run_id,
        data.get("car_number"),
        data.get("policy_expiry"),
        data.get("claim_status"),
        data.get("phone"),
        data.get("customer_name"),
    ))
    conn.commit()
    cur.close()



def get_scrape_input(run_id):
    return dict_cursor_execute("SELECT * FROM scrape_run_inputs WHERE run_id=%s", (run_id.strip(),), fetchone=True)


def update_scrape_input(run_id, **kwargs):
    safe_update("scrape_run_inputs", "run_id", run_id.strip(), kwargs)


def delete_scrape_input(run_id):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("DELETE FROM scrape_run_inputs WHERE run_id=%s", (run_id.strip(),))
    conn.commit()
    cur.close()
    conn.close()


# ============================================
# SCRAPE DATA USAGE
# ============================================
def insert_data_usage(conn, run_id, usage_summary: dict):
    run_id = run_id.strip()
    cur    = conn.cursor()

    # ── Accumulators for cross-phase summary ──────────────────────────────────
    cat_agg: dict[str, dict] = {}
    grand = {"call_count": 0, "req_bytes": 0, "resp_bytes": 0, "total_bytes": 0}

    def _upsert(phase, category, call_count, req_bytes, resp_bytes, total_bytes,
                req_human, resp_human, total_human, top_urls):
        cur.execute("""
            INSERT INTO scrape_data_usage
            (run_id, phase, category,
             call_count, request_bytes, response_bytes, total_bytes,
             request_size, response_size, total_size,
             top_urls, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (run_id, phase, category)
            DO UPDATE SET
                call_count     = EXCLUDED.call_count,
                request_bytes  = EXCLUDED.request_bytes,
                response_bytes = EXCLUDED.response_bytes,
                total_bytes    = EXCLUDED.total_bytes,
                request_size   = EXCLUDED.request_size,
                response_size  = EXCLUDED.response_size,
                total_size     = EXCLUDED.total_size,
                top_urls       = EXCLUDED.top_urls;
        """, (
            run_id, phase, category,
            call_count, req_bytes, resp_bytes, total_bytes,
            req_human, resp_human, total_human,
            top_urls,
        ))

    def _fmt_bytes(b: int) -> str:
        if b < 1024:         return f"{b} B"
        elif b < 1024 ** 2:  return f"{b / 1024:.2f} KB"
        elif b < 1024 ** 3:  return f"{b / (1024 ** 2):.2f} MB"
        return f"{b / (1024 ** 3):.2f} GB"

    for phase, phase_data in usage_summary.items():
        if not isinstance(phase_data, dict):
            continue

        # ── Per-category rows ─────────────────────────────────────────────────
        categories = phase_data.get("categories", {})
        for category, stats in categories.items():
            if not isinstance(stats, dict):
                continue

            req_b   = stats.get("req_bytes",    0)
            resp_b  = stats.get("resp_bytes",   0)
            total_b = stats.get("total_bytes",  0)
            calls   = stats.get("call_count",   0)
            top_urls = json.dumps(stats["top_urls"]) if stats.get("top_urls") else None

            _upsert(
                phase, category,
                calls, req_b, resp_b, total_b,
                stats.get("req_human",   "0 B"),
                stats.get("resp_human",  "0 B"),
                stats.get("total_human", "0 B"),
                top_urls,
            )

            # ── Accumulate into cross-phase category totals ───────────────────
            agg = cat_agg.setdefault(category, {
                "call_count": 0, "req_bytes": 0,
                "resp_bytes": 0, "total_bytes": 0,
                "top_urls": [],
            })
            agg["call_count"]  += calls
            agg["req_bytes"]   += req_b
            agg["resp_bytes"]  += resp_b
            agg["total_bytes"] += total_b
            if stats.get("top_urls"):
                agg["top_urls"].extend(stats["top_urls"])

            # ── Accumulate into grand total ───────────────────────────────────
            grand["call_count"]  += calls
            grand["req_bytes"]   += req_b
            grand["resp_bytes"]  += resp_b
            grand["total_bytes"] += total_b

        # ── Phase total row ───────────────────────────────────────────────────
        total = phase_data.get("phase_total", {})
        if total:
            _upsert(
                phase, "phase_total",
                total.get("call_count",    0),
                total.get("req_bytes",     0),
                total.get("resp_bytes",    0),
                total.get("total_bytes",   0),
                total.get("req_human",   "0 B"),
                total.get("resp_human",  "0 B"),
                total.get("total_human", "0 B"),
                None,
            )

    # ── Cross-phase category summary rows (phase = 'summary') ─────────────────
    for category, agg in cat_agg.items():
        _upsert(
            "summary", category,
            agg["call_count"],
            agg["req_bytes"],
            agg["resp_bytes"],
            agg["total_bytes"],
            _fmt_bytes(agg["req_bytes"]),
            _fmt_bytes(agg["resp_bytes"]),
            _fmt_bytes(agg["total_bytes"]),
            json.dumps(agg["top_urls"]) if agg["top_urls"] else None,
        )

    # ── Grand total row ───────────────────────────────────────────────────────
    _upsert(
        "summary", "grand_total",
        grand["call_count"],
        grand["req_bytes"],
        grand["resp_bytes"],
        grand["total_bytes"],
        _fmt_bytes(grand["req_bytes"]),
        _fmt_bytes(grand["resp_bytes"]),
        _fmt_bytes(grand["total_bytes"]),
        None,
    )

    conn.commit()
    cur.close()



def get_data_usage(run_id):
    return dict_cursor_execute(
        "SELECT * FROM scrape_data_usage WHERE run_id=%s ORDER BY phase, category",
        (run_id.strip(),),
        fetchall=True,
    )


def delete_data_usage(run_id):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("DELETE FROM scrape_data_usage WHERE run_id=%s", (run_id.strip(),))
    conn.commit()
    cur.close()
    conn.close()


# ============================================
# QUOTES DETAILS
# ============================================
def insert_quotes_plandetails(conn, run_id, insurer_name, plan_id, plan_json, addon_combo_id, idv_selected, idv_type):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO quotes_details
        (run_id, insurer_name, plan_id, plan_json, addon_combo_id, idv_selected, idv_type, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (run_id.strip(), insurer_name, plan_id, json.dumps(plan_json), addon_combo_id, idv_selected, idv_type, datetime.now(), datetime.now()))
    cur.close()


def get_quotes_details(run_id):
    return dict_cursor_execute("SELECT * FROM quotes_details WHERE run_id=%s", (run_id.strip(),), fetchall=True)


def update_quotes_detail(id, new_json):
    safe_update("quotes_details", "id", id, {"plan_json": new_json})


def delete_quotes_details(run_id):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("DELETE FROM quotes_details WHERE run_id=%s", (run_id.strip(),))
    conn.commit()
    cur.close()
    conn.close()


# ============================================
# QUOTES RESPONSES
# ============================================
def insert_quotes_response(conn, run_id, api_name, api_url, idv_selected, idv_type, response_json):
    run_id = run_id.strip()
    cur    = conn.cursor()
    cur.execute("""
        INSERT INTO quotes_responses
        (run_id, api_name, api_url, idv_selected, idv_type, response_json, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, (run_id, api_name, api_url, idv_selected, idv_type, json.dumps(response_json), datetime.now(), datetime.now()))
    conn.commit()
    cur.close()


def get_quotes_responses(run_id):
    return dict_cursor_execute("SELECT * FROM quotes_responses WHERE run_id=%s", (run_id.strip(),), fetchall=True)


def update_quotes_response(id, new_json):
    safe_update("quotes_responses", "id", id, {"response_json": new_json})


def delete_quotes_responses(run_id):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("DELETE FROM quotes_responses WHERE run_id=%s", (run_id.strip(),))
    conn.commit()
    cur.close()
    conn.close()


# ============================================
# FINAL DATA
# ============================================
def insert_final_data(run_id, final_json):
    run_id = run_id.strip()
    conn   = get_connection()
    cur    = conn.cursor()
    cur.execute("""
        INSERT INTO final_data
        (run_id, final_data, created_at, updated_at)
        VALUES (%s,%s,%s,%s)
    """, (run_id, json.dumps(final_json), datetime.now(), datetime.now()))
    conn.commit()
    cur.close()
    conn.close()


def get_final_data(run_id):
    return dict_cursor_execute("SELECT * FROM final_data WHERE run_id=%s", (run_id.strip(),), fetchone=True)


def update_final_data(run_id, final_json):
    safe_update("final_data", "run_id", run_id.strip(), {"final_data": final_json})


def delete_final_data(run_id):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("DELETE FROM final_data WHERE run_id=%s", (run_id.strip(),))
    conn.commit()
    cur.close()
    conn.close()


# ============================================
# FINAL FLAT OUTPUT
# ============================================
def insert_flat_output(run_id, flat_json):
    run_id = run_id.strip()
    conn   = get_connection()
    cur    = conn.cursor()
    cur.execute("""
        INSERT INTO final_flat_output
        (run_id, flat_output, created_at, updated_at)
        VALUES (%s,%s,%s,%s)
    """, (run_id, json.dumps(flat_json), datetime.now(), datetime.now()))
    conn.commit()
    cur.close()
    conn.close()


def get_flat_output(run_id):
    return dict_cursor_execute("SELECT * FROM final_flat_output WHERE run_id=%s", (run_id.strip(),), fetchone=True)


def update_flat_output(run_id, flat_json):
    safe_update("final_flat_output", "run_id", run_id.strip(), {"flat_output": flat_json})


def delete_flat_output(run_id):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("DELETE FROM final_flat_output WHERE run_id=%s", (run_id.strip(),))
    conn.commit()
    cur.close()
    conn.close()


# ============================================
# RUN LOGS
# ============================================
def insert_run_log(conn, run_id, step_number, step_key,
                   status=None, start_ts=None, end_ts=None,
                   duration_ms=None, data=None):
    run_id = run_id.strip()
    cur    = conn.cursor()
    cur.execute("""
        INSERT INTO run_logs
        (run_id, step_number, step_key, status,
         start_ts, end_ts, duration_ms, data, created_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
    """, (
        run_id, step_number, step_key, status,
        start_ts, end_ts, duration_ms,
        json.dumps(data) if data else None,
        datetime.now(),
    ))
    log_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    return log_id


def get_run_logs(run_id):
    return dict_cursor_execute(
        "SELECT * FROM run_logs WHERE run_id=%s ORDER BY step_number ASC",
        (run_id.strip(),), fetchall=True,
    )


def update_run_log(log_id, **kwargs):
    safe_update("run_logs", "id", log_id, kwargs)


def delete_run_log(log_id):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("DELETE FROM run_logs WHERE id=%s", (log_id,))
    conn.commit()
    cur.close()
    conn.close()


# ============================================
# AKAMAI EVENTS
# ============================================
def insert_akamai_event(conn, run_id, log_id=None, step_after=None,
                        step_key_after=None, event_timestamp=None, data=None):
    run_id = run_id.strip()
    cur    = conn.cursor()
    cur.execute("""
        INSERT INTO akamai_events
        (run_id, log_id, step_after, step_key_after, event_timestamp, data, created_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
    """, (
        run_id, log_id, step_after, step_key_after,
        event_timestamp or datetime.now(),
        json.dumps(data) if data else None,
        datetime.now(),
    ))
    event_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    return event_id


def get_akamai_events(run_id):
    return dict_cursor_execute(
        "SELECT * FROM akamai_events WHERE run_id=%s ORDER BY event_timestamp ASC",
        (run_id.strip(),), fetchall=True,
    )


def update_akamai_event(event_id, **kwargs):
    safe_update("akamai_events", "id", event_id, kwargs)


def delete_akamai_event(event_id):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("DELETE FROM akamai_events WHERE id=%s", (event_id,))
    conn.commit()
    cur.close()
    conn.close()
 