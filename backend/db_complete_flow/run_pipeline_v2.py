import sys
from pathlib import Path
import psycopg2.extras


# ------------------------------------------------
# Add db_complete_flow folder to python path
# ------------------------------------------------
_this_dir = str(Path(__file__).resolve().parent)
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

# Ensure backend root is importable
_backend_root = str(Path(__file__).resolve().parent.parent)
if _backend_root not in sys.path:
    sys.path.insert(0, _backend_root)


# ------------------------------------------------
# Import your scripts
# ------------------------------------------------
import responses_final_v2
import finaldb_flatdb
import flatdb_excel


# ------------------------------------------------
# Fetch all SUCCESS run_ids from scrape_runs
# ------------------------------------------------
def fetch_success_run_ids(conn) -> list:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT run_id FROM scrape_runs
            WHERE status = 'SUCCESS'
            ORDER BY started_at ASC
        """)
        return [str(row["run_id"]) for row in cur.fetchall()]


# ------------------------------------------------
# Fetch run_ids already present in final_flat_output
# ------------------------------------------------
def fetch_already_processed_run_ids(conn) -> set:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT DISTINCT run_id FROM final_flat_output")
        return {str(row["run_id"]) for row in cur.fetchall()}


# ------------------------------------------------
# Pipeline for a single run_id
# ------------------------------------------------
def run_pipeline(run_id: str):

    print("\n======================================")
    print("🚀 QUOTES PIPELINE STARTED")
    print("RUN_ID:", run_id)
    print("======================================")

    # ── STEP 1: quotes_responses → car_info, quotes_details, final_data ──────
    print("\n🔹 STEP 1: Processing quotes responses")

    conn = responses_final_v2.get_conn()
    try:
        plans_count = responses_final_v2.process_run(conn, run_id, force=True)
        print(f"✅ Step 1 Completed → {plans_count} plans processed")
    finally:
        conn.close()

    if plans_count == 0:
        print(f"⚠️  Skipping Steps 2 & 3 — no plans extracted for run {run_id}")
        return False

    # ── STEP 2: final_data → final_flat_output ────────────────────────────────
    print("\n🔹 STEP 2: Flattening final data")

    rows = finaldb_flatdb.fetch_final_data()
    step2_done = False

    for r_id, final_data in rows:
        if str(r_id) != str(run_id):
            continue

        eligible_ncb = finaldb_flatdb.fetch_all_ncb(run_id)
        flat_rows    = finaldb_flatdb.flatten_final_data(run_id, final_data, eligible_ncb)
        finaldb_flatdb.save_flat_output(run_id, flat_rows)

        print(f"✅ Step 2 Completed → {len(flat_rows)} rows flattened")
        step2_done = True
        break

    if not step2_done:
        print(f"⚠️  Step 2 skipped — no final_data found for run {run_id}")
        return False

    # ── STEP 3: final_flat_output → Excel ─────────────────────────────────────
    print("\n🔹 STEP 3: Exporting Excel")

    flatdb_excel.RUN_ID = run_id
    flatdb_excel.main()

    print("\n======================================")
    print("🎉 PIPELINE COMPLETED SUCCESSFULLY")
    print("======================================\n")

    return True


# ------------------------------------------------
# MAIN — batch mode: all SUCCESS runs not yet flat
# ------------------------------------------------
def main():
    conn = responses_final_v2.get_conn()
    try:
        success_run_ids   = fetch_success_run_ids(conn)
        already_processed = fetch_already_processed_run_ids(conn)
    finally:
        conn.close()

    pending = [rid for rid in success_run_ids if rid not in already_processed]

    # ── Show summary upfront ──────────────────────────────────────────────────
    print("=" * 60)
    print(f"✅ SUCCESS runs total      : {len(success_run_ids)}")
    print(f"⏭️  Already in flat table  : {len(already_processed)}")
    print(f"🔄 Pending to process      : {len(pending)}")
    print("=" * 60)

    if already_processed:
        print(f"\n⏭️  Already processed run_ids ({len(already_processed)}) — skipping these:")
        for rid in sorted(already_processed):
            print(f"      {rid}")
        print()

    if not pending:
        print("🎉 Nothing to process — all SUCCESS runs are already flattened.")
        return

    summary_ok   = []
    summary_skip = []
    summary_fail = []

    for i, run_id in enumerate(pending, 1):
        print(f"\n[{i}/{len(pending)}] ▶  run_id = {run_id}")
        try:
            success = run_pipeline(run_id)
            if success:
                summary_ok.append(run_id)
            else:
                summary_skip.append(run_id)
        except Exception as e:
            print(f"❌ run {run_id} FAILED with error: {e}")
            summary_fail.append((run_id, str(e)))

    # ── Final summary ─────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("📊 BATCH SUMMARY")
    print(f"  ✅ Completed : {len(summary_ok)}")
    print(f"  ⚠️  Skipped   : {len(summary_skip)}")
    print(f"  ❌ Failed    : {len(summary_fail)}")

    if summary_skip:
        print("\n  ⚠️  Skipped run_ids (0 plans extracted):")
        for rid in summary_skip:
            print(f"      {rid}")

    if summary_fail:
        print("\n  ❌ Failed run_ids:")
        for rid, err in summary_fail:
            print(f"      {rid}  →  {err}")

    print("=" * 60)


if __name__ == "__main__":
    main()
