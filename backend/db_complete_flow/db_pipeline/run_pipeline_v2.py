import json
import sys
from pathlib import Path
import psycopg2.extras
from datetime import datetime


# ------------------------------------------------
# Add db_complete_flow folder to python path
# ------------------------------------------------
project_root = Path(__file__).parent
db_path = str(project_root / "db_complete_flow")

if db_path not in sys.path:
    sys.path.insert(0, db_path)


# ------------------------------------------------
# Import your scripts
# ------------------------------------------------
import RSscraping_backend.backend.db_complete_flow.db_pipeline.responses_final_v2 as responses_final_v2
import RSscraping_backend.backend.db_complete_flow.db_pipeline.finaldb_flatdb as finaldb_flatdb
import RSscraping_backend.backend.db_complete_flow.db_pipeline.flatdb_excel as flatdb_excel

REPROCESS_ALL = True  # set False normally

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

    for r_id, final_data, created_at in rows:
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

    flatdb_excel.main(run_id)

    print("\n======================================")
    print("🎉 PIPELINE COMPLETED SUCCESSFULLY")
    print("======================================\n")

    return True

def reprocess_single_run(runid: str) -> bool:
    """
    Re-run the full pipeline (Steps 1–3) for a single runid,
    regardless of whether it is already present in finalflatoutput.
    """
    print()
    print("QUOTES PIPELINE REPROCESS STARTED")
    print("RUNID:", runid)
    print("-" * 60)

    success = run_pipeline(runid)
    if success:
        print()
        print("REPROCESS COMPLETED SUCCESSFULLY")
    else:
        print()
        print("REPROCESS FAILED")

    return success
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


    if REPROCESS_ALL:
        pending = success_run_ids
    else:
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
    if len(sys.argv) == 2:
        # Usage: python run_pipeline_v2.py <runid>
        reprocess_single_run(sys.argv[1])
    else:
        # Original batch behavior
        conn = responses_final_v2.get_conn()
        try:
            success_run_ids = fetch_success_run_ids(conn)
            already_processed = fetch_already_processed_run_ids(conn)
        finally:
            conn.close()

        ids_to_skip = [
            "2b118c2a-888c-4293-b83d-c21055be2917",
            "28cf1e75-dd0d-4340-abaa-d2ab52ba29f2",
            "88c191e0-46e1-4718-a900-d3f9f5744491",
            "8d0cbeb5-f08d-4a4e-a3d8-0fda419d8c06",
            "37f00046-065f-494d-8d71-4c4fe5d7bd8e",
            "e589250a-9c29-4e7a-abd6-04235c05f40e",
            "2d7045db-2ed4-42fd-b470-15098ba04d3f",
            "d27ce50f-7e2c-4c96-ae10-eb3d2cfd1b37",
            "f6880291-51c3-48ed-8c45-a006dafa222f",
            "a75a7d89-8e3f-4f94-93a7-1037aad24831",
            "1a94e0f9-0ddb-4f73-bdd8-5a6cc5be370c",
            "43c83c4b-8483-45ed-ae0c-7373358b01b0",
            "6c3821e0-564b-4792-a896-185c93e8b5e7",
            "0bd1f460-70ba-4d2a-aaa6-a822c23945e6",
            "133bce40-6f08-4c43-92cc-21ee0657c052",
            "2683a2ef-a60d-400a-968c-2525f54971fe",
            "3f4019a4-8c97-4383-a644-945d28221ef3",
            "ecfd5b58-e6d1-4ed8-8212-cbcffc700ec4",
            "df72383d-0ecf-4a44-a7eb-ddf7bd0994a9",
            "08fa1b4c-d520-4b60-8ab1-171c2371f773",
            "307e799f-ba42-4e88-af3b-68c81c71f823",
            "053510f3-0520-4d2d-bd4d-0776aa552210",
            "4e3778f7-c2e0-4326-aa08-6e9e1aee8358",
            "fc3871d9-ff23-4334-86a2-5bc6d8aa59f9",
            "f57c5d3d-bb7d-4744-936d-4ce5bff3c02f",
            "f983e011-0d7d-4fc6-b17f-4a5e82bad6a0",
            "0364cb50-531d-4e8d-85ee-c4bc382fbc35",
            "fe8f5ff7-a8a1-497e-a2c6-2d5dfb4d740a",
            "3473a8c7-80d9-44b6-8c0c-5f0cac5b8103",
            "a8dbee4d-08fd-48d0-83dc-228e161fafeb",
            "352e86ca-4263-444c-acff-0f2f6e850fb5",
            "23a19111-bbb8-4e36-8345-4115bec31668",
            "30e5aa28-74b4-4d37-866d-e970749c47a0",
            "da0dd62f-a762-4dc9-9928-8719a047241e",
            "a6fdc656-acab-46a8-8fc5-c1a573013705",
            "72ad876c-af73-4e9d-a80d-d1233cc1c90d",
            "cae6b2e6-98e6-4248-9b92-f5829b0e691d",
            "53c5ae65-49d6-42b2-90ae-1a2d9d7077ff",
            "695b6721-a801-49bb-aafa-7e5cec03ddfc",
            "d1b11f4f-f330-458d-b02a-d689240b0cd7",
            "ad4127b4-32b3-4b9f-9891-13e334a67cbb",
            "2ae0691b-a518-4dcf-89f4-ef859d15c824",
            "a551cf7f-11a9-47fc-b290-82c4d6cdd322",
            "b4e329ec-c7db-41b0-893b-e35ddde23a46",
            "44875200-93c6-4733-9106-940681c0d11a",
            "42dc5d3d-9df1-456b-80f4-79ff2b920b41",
            "d3021b60-19b3-4c7b-98e2-0ab4a7990ac8",
            "678b9df0-bfab-4c89-8bcc-36386e0eeeb6",
            "b49f5f15-58bb-4d97-8f97-933eb149e3a1",
            "84c9f23f-f781-44be-8d54-0270db80ec03",
            "7758b059-8bd2-47c8-bfb6-e0e8d117dca2",
            "8f85027e-7835-4355-8331-f85078c6e77d",
            "c5341a83-224a-4063-b7f9-9d6db5d7623e",
            "fb74b2d9-f382-44dc-8c32-eebdcdd911d2",
            "6a2509d5-544d-4b4b-97a6-ebc9d928c89f",
            "237de508-9ee2-4ebc-95d4-4cb5c97b1cdc",
            "ae01a531-11b6-4519-8e47-23de3895c190",
            "38d85ac0-8c05-4fb7-9ee6-97138156731c",
            "a79dbd45-b53f-4671-b564-d704d9be9f98",
            "5fdf0a4b-f8ab-4f0a-844c-9a57457725fd",
            "0cfc5aa4-605a-4ed9-80e4-1b4eb7b4a185"
            ]
        

        # with open("ids_to_skip.json", 'r') as f:
        #     # Parse the string
        #     ids = json.load(f)
        
        # if not ids_to_skip:
        #     print("No ids to skip loaded from ids_to_skip.json")
        #     raise ValueError("ids_to_skip.json is empty or missing 'ids' key")
            

        pending = [rid for rid in success_run_ids if rid not in already_processed]

        pending = [rid for rid in pending if rid not in ids_to_skip]

        print("-" * 60)
        print(f"SUCCESS runs total: {len(success_run_ids)}")
        print(f"Already in flat table: {len(already_processed)}")
        print(f"Pending to process: {len(pending)}")
        print("-" * 60)

        if already_processed:
            print(f"Already processed runids ({len(already_processed)}) — skipping these:")
            for rid in sorted(already_processed):
                print(rid)

        if not pending:
            print("Nothing to process: all SUCCESS runs are already flattened.")
            sys.exit(0)

        summary_ok = []
        summary_skip = []
        summary_fail = []

        for i, runid in enumerate(pending, 1):
            print()
            print(f"[{i}/{len(pending)}] RUNID {runid}")
            try:
                success = run_pipeline(runid)
                if success:
                    summary_ok.append(runid)
                else:
                    summary_skip.append(runid)
            except Exception as e:
                print(f"run {runid} FAILED with error: {e}")
                summary_fail.append((runid, str(e)))

        print("-" * 60)
        print("BATCH SUMMARY")
        print(f"Completed: {len(summary_ok)}")
        print(f"Skipped:   {len(summary_skip)}")
        print(f"Failed:    {len(summary_fail)}")
        if summary_skip:
            print("Skipped runids (0 plans extracted):")
            for rid in summary_skip:
                print(rid)
        if summary_fail:
            print("Failed runids:")
            for rid, err in summary_fail:
                print(rid, err)
        print("-" * 60)