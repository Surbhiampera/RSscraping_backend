import json
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime


DB_CONFIG = {
    "host": "localhost",
    "database": "insurance_v2",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}


def ingest_run(json_file_path):
    conn = None
    cursor = None

    try:
        with open(json_file_path, "r") as f:
            raw_data = json.load(f)

        # 🔥 FIX: Support both list and single object
        if isinstance(raw_data, list):
            runs = raw_data
        else:
            runs = [raw_data]

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        conn.autocommit = False

        for data in runs:

            run_id = data["run_id"]

            # ==============================
            # scrape_runs
            # ==============================
            # ==============================
            # scrape_runs
            # ==============================
            run_summary = data.get("run_summary", {})

            cursor.execute("""
                INSERT INTO scrape_runs (
                    run_id,
                    status,
                    started_at,
                    ended_at,
                    total_duration_ms,
                    notes
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    started_at = EXCLUDED.started_at,
                    ended_at = EXCLUDED.ended_at,
                    total_duration_ms = EXCLUDED.total_duration_ms
            """, (
                run_id,
                run_summary.get("status"),
                run_summary.get("start_ts"),
                run_summary.get("end_ts"),
                run_summary.get("total_duration_ms"),
                None
            ))


            # ==============================
            # car_info
            # ==============================
            car = data.get("car_info", {})
            if isinstance(car, dict) and car:
                cursor.execute("""
                    INSERT INTO car_info (
                        run_id,
                        registration_number,
                        make_name,
                        model_name,
                        vehicle_variant,
                        fuel_type,
                        cubic_capacity,
                        state_code,
                        city_tier,
                        car_age,
                        registration_date
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (run_id) DO NOTHING
                """, (
                    run_id,
                    car.get("registration_number"),
                    car.get("make_name"),
                    car.get("model_name"),
                    car.get("vehicle_variant"),
                    car.get("fuel_type"),
                    car.get("cubic_capacity"),
                    car.get("state_code"),
                    car.get("city_tier"),
                    car.get("car_age"),
                    car.get("registration_date")
                ))

            # ==============================
            # run_logs + akamai
            # ==============================
            for step in data.get("steps", []):

                if not isinstance(step, dict):
                    continue

                cursor.execute("""
                    INSERT INTO run_logs (
                        run_id,
                        step_number,
                        step_key,
                        status,
                        section,
                        start_ts,
                        end_ts,
                        duration_ms,
                        data
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id
                """, (
                    run_id,
                    step.get("step_number"),
                    step.get("step_key"),
                    step.get("status"),
                    step.get("section"),
                    step.get("start_ts"),
                    step.get("end_ts"),
                    step.get("duration_ms"),
                    json.dumps(step.get("data"))
                ))

                log_id = cursor.fetchone()[0]

                for akamai in step.get("akamai_events", []):
                    if not isinstance(akamai, dict):
                        continue

                    cursor.execute("""
                        INSERT INTO akamai_events (
                            run_id,
                            log_id,
                            step_after,
                            step_key_after,
                            event_timestamp,
                            data
                        )
                        VALUES (%s,%s,%s,%s,%s,%s)
                    """, (
                        run_id,
                        log_id,
                        akamai.get("step_after"),
                        akamai.get("step_key_after"),
                        akamai.get("event_timestamp"),
                        json.dumps(akamai.get("data"))
                    ))

            # ==============================
            # quotes_details
            # ==============================
            for plan in data.get("quotes_details", []):
                if not isinstance(plan, dict):
                    continue

                cursor.execute("""
                    INSERT INTO quotes_details (
                        run_id,
                        insurer_name,
                        plan_id,
                        plan_json
                    )
                    VALUES (%s,%s,%s,%s)
                """, (
                    run_id,
                    plan.get("insurer_name"),
                    plan.get("plan_id"),
                    json.dumps(plan)
                ))

            # ==============================
            # quotes_responses
            # ==============================
            for response in data.get("quotes_responses", []):
                if not isinstance(response, dict):
                    continue

                cursor.execute("""
                    INSERT INTO quotes_responses (
                        run_id,
                        api_name,
                        api_url,
                        response_json
                    )
                    VALUES (%s,%s,%s,%s)
                """, (
                    run_id,
                    response.get("api_name"),
                    response.get("api_url"),
                    json.dumps(response.get("response_json"))
                ))

            # ==============================
            # final_data
            # ==============================
            if data.get("final_data"):
                cursor.execute("""
                    INSERT INTO final_data (
                        run_id,
                        final_data
                    )
                    VALUES (%s,%s)
                    ON CONFLICT (run_id) DO NOTHING
                """, (
                    run_id,
                    json.dumps(data.get("final_data"))
                ))

            # ==============================
            # final_flat_output
            # ==============================
            if data.get("final_flat_output"):
                cursor.execute("""
                    INSERT INTO final_flat_output (
                        run_id,
                        flat_output
                    )
                    VALUES (%s,%s)
                """, (
                    run_id,
                    json.dumps(data.get("final_flat_output"))
                ))

        conn.commit()
        print("✅ All runs ingested successfully.")

    except Exception as e:
        if conn:
            conn.rollback()
        print("❌ Error during ingestion:", str(e))

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    ingest_run("testing_log_data.json")
