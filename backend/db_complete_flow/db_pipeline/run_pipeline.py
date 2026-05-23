import sys
from pathlib import Path

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
import responses_final_v2
import finaldb_flatdb
import flatdb_excel


# ------------------------------------------------
# Pipeline Controller
# ------------------------------------------------
def run_pipeline(run_id):

    print("\n======================================")
    print("🚀 QUOTES PIPELINE STARTED")
    print("RUN_ID:", run_id)
    print("======================================")

    # ------------------------------------------------
    # STEP 1
    # quotes_responses → car_info, quotes_details, final_data
    # ------------------------------------------------
    print("\n🔹 STEP 1: Processing quotes responses")

    conn = responses_final_v2.get_conn()

    try:
        plans_count = responses_final_v2.process_run(conn, run_id, force=True)
        print(f"✅ Step 1 Completed → {plans_count} plans processed")

    finally:
        conn.close()

    # ------------------------------------------------
    # STEP 2
    # final_data → final_flat_output
    # ------------------------------------------------
    print("\n🔹 STEP 2: Flattening final data")

    rows = finaldb_flatdb.fetch_final_data()

    for r_id, final_data, created_at in rows:

        if str(r_id) != str(run_id):
            continue

        eligible_ncb = finaldb_flatdb.fetch_all_ncb(run_id)

        flat_rows = finaldb_flatdb.flatten_final_data(
            run_id,
            final_data,
            eligible_ncb,
            created_at,
        )

        finaldb_flatdb.save_flat_output(run_id, flat_rows)

        print(f"✅ Step 2 Completed → {len(flat_rows)} rows flattened")

    # ------------------------------------------------
    # STEP 3
    # final_flat_output → Excel
    # ------------------------------------------------
    print("\n🔹 STEP 3: Exporting Excel")

    flatdb_excel.RUN_ID = run_id
    flatdb_excel.main()

    print("\n======================================")
    print("🎉 PIPELINE COMPLETED SUCCESSFULLY")
    print("======================================\n")


# ------------------------------------------------
# MAIN
# ------------------------------------------------
def main():

    run_id = input("🔹 Please enter RUN_ID: ").strip()

    if not run_id:
        print("❌ RUN_ID cannot be empty")
        return

    run_pipeline(run_id)


if __name__ == "__main__":
    main()