import psycopg2
import pandas as pd

from backend.core.config import settings

# -------------------------------
# DB CONFIG
# -------------------------------
DB_HOST     = settings.DB_HOST
DB_PORT     = settings.DB_PORT
DB_NAME     = settings.DB_NAME
DB_USER     = settings.DB_USER
DB_PASSWORD = settings.DB_PASSWORD


RUN_ID      = "11111111-1111-1111-1111-111111111111"
OUTPUT_DIR  = "excel_outputs"   # folder to store all per-run Excels


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def fetch_flat_output(run_id):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        SELECT flat_output
        FROM final_flat_output
        WHERE run_id = %s
    """, (run_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return []
    return row[0]


def fetch_idv_types(run_id):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        SELECT insurer_name, plan_id, idv_type
        FROM quotes_details
        WHERE run_id = %s
    """, (run_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=["insurer_name", "plan_id", "idv_type"])


COLUMN_ORDER = [
    "Sr No", "Rto Location", "Make", "Model", "Variant", "CC", "CC Range", "Fuel Type",
    "Insurer", "NCB %", "Selected IDV", "Nil Dep", "Tariff Rate", "YOM", "IDV", "IDV Type",
    "Basic OD Premium", "OD Discount", "Nil Dep Premium", "EP Premium", "RTI Premium",
    "RSA Premium", "Consumables", "Key & Lock Replacement", "Tyre Protector",
    "Loss of Personal Belongings", "Emergency Transport and Hotel Allowance",
    "No-Claim Bonus", "Voluntary Deductible Discount", "Other Discounts",
    "Daily Allowance", "NCB Protector", "Total TP Premium", "Final Premium visible to customer"
]


def build_excel_df(flat_rows):
    """Build a formatted DataFrame from flat_output rows.

    Applies column renames, ordering, sorting, and insurer grouping
    consistent with the canonical Excel format.
    """
    df = pd.DataFrame(flat_rows)
    if "run_id" in df.columns:
        df.drop(columns=["run_id"], inplace=True)
    if "plan_hidden" in df.columns:
        df.drop(columns=["plan_hidden"], inplace=True)
    if "IDV Type" not in df.columns:
        df["IDV Type"] = None

    if "sr_no" in df.columns:
        df.rename(columns={"sr_no": "Sr No"}, inplace=True)
    if "Company" in df.columns:
        df.rename(columns={"Company": "Insurer"}, inplace=True)

    ordered_cols   = [c for c in COLUMN_ORDER if c in df.columns]
    remaining_cols = [c for c in df.columns if c not in ordered_cols]
    df = df[ordered_cols + remaining_cols].fillna("Not Included")
    df = df.sort_values(by="Insurer", key=lambda col: col.str.lower())

    # Group by Insurer with empty row between groups
    df_list = []
    for insurer, group in df.groupby("Insurer", sort=False):
        df_list.append(group)
        empty_row = pd.DataFrame([[""] * len(df.columns)], columns=df.columns)
        df_list.append(empty_row)

    return pd.concat(df_list, ignore_index=True)


def main():
    flat_rows = fetch_flat_output(RUN_ID)

    if not flat_rows:
        print("❌ No data found for this run_id")
        return

    final_df = build_excel_df(flat_rows)

    # ── Save to per-run Excel file ────────────────────────────────────────────
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, f"{RUN_ID}.xlsx")
    final_df.to_excel(output_path, index=False)

    print("✅ Excel created successfully")
    print("📄 Path:", output_path)
    print("📊 Total insurer rows (including gaps):", len(final_df))


if __name__ == "__main__":
    main()
