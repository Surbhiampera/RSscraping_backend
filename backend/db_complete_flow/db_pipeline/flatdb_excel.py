import os
import re
from datetime import datetime

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# -------------------------------
# DB CONFIG
# -------------------------------
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

OUTPUT_DIR = "batch_three_33_records"
_RTO_CITY_MAP = None

# ---------------------------------------------------------------------------
# Tariff rate table
# ---------------------------------------------------------------------------
RATES = [
    {"zone": "Zone B", "age": "0-5", "cc": "upto_1000", "rate": 3.039},
    {"zone": "Zone B", "age": "0-5", "cc": "1001_1500", "rate": 3.191},
    {"zone": "Zone B", "age": "0-5", "cc": "abv_1500", "rate": 3.343},
    {"zone": "Zone B", "age": "5-10", "cc": "upto_1000", "rate": 3.191},
    {"zone": "Zone B", "age": "5-10", "cc": "1001_1500", "rate": 3.351},
    {"zone": "Zone B", "age": "5-10", "cc": "abv_1500", "rate": 3.510},
    {"zone": "Zone B", "age": "10+", "cc": "upto_1000", "rate": 3.267},
    {"zone": "Zone B", "age": "10+", "cc": "1001_1500", "rate": 3.430},
    {"zone": "Zone B", "age": "10+", "cc": "abv_1500", "rate": 3.594},
    {"zone": "Zone A", "age": "0-5", "cc": "upto_1000", "rate": 3.127},
    {"zone": "Zone A", "age": "0-5", "cc": "1001_1500", "rate": 3.283},
    {"zone": "Zone A", "age": "0-5", "cc": "abv_1500", "rate": 3.440},
    {"zone": "Zone A", "age": "5-10", "cc": "upto_1000", "rate": 3.283},
    {"zone": "Zone A", "age": "5-10", "cc": "1001_1500", "rate": 3.447},
    {"zone": "Zone A", "age": "5-10", "cc": "abv_1500", "rate": 3.612},
    {"zone": "Zone A", "age": "10+", "cc": "upto_1000", "rate": 3.362},
    {"zone": "Zone A", "age": "10+", "cc": "1001_1500", "rate": 3.529},
    {"zone": "Zone A", "age": "10+", "cc": "abv_1500", "rate": 3.698},
]

RATE_LOOKUP = {(r["zone"], r["age"], r["cc"]): r["rate"] for r in RATES}

ZONE_A_CITIES = {
    "ahmedabad", "bangalore", "bengaluru", "chennai",
    "delhi", "new delhi", "delhi ncr",
    "hyderabad", "kolkata", "calcutta", "mumbai", "bombay", "pune",
}

STATE_CODE_TO_CITY = {
    "DL": "delhi",
    "MH01": "mumbai", "MH02": "mumbai", "MH03": "mumbai", "MH04": "mumbai",
    "MH12": "pune", "MH14": "pune",
    "KA01": "bangalore", "KA02": "bangalore", "KA03": "bangalore",
    "KA04": "bangalore", "KA05": "bangalore", "KA50": "bangalore",
    "KA51": "bangalore", "KA53": "bangalore",
    "TN01": "chennai", "TN02": "chennai", "TN03": "chennai",
    "TN04": "chennai", "TN05": "chennai", "TN06": "chennai",
    "TN07": "chennai", "TN09": "chennai", "TN10": "chennai",
    "TN11": "chennai", "TN12": "chennai", "TN13": "chennai",
    "TN14": "chennai", "TN18": "chennai", "TN22": "chennai",
    "TS07": "hyderabad", "TS08": "hyderabad", "TS09": "hyderabad",
    "TS10": "hyderabad", "TS11": "hyderabad", "TS12": "hyderabad",
    "TS13": "hyderabad", "AP09": "hyderabad", "AP10": "hyderabad",
    "AP11": "hyderabad", "AP12": "hyderabad", "AP13": "hyderabad",
    "AP14": "hyderabad", "AP15": "hyderabad", "AP16": "hyderabad",
    "AP28": "hyderabad", "AP29": "hyderabad",
    "WB01": "kolkata", "WB02": "kolkata", "WB03": "kolkata",
    "WB04": "kolkata", "WB05": "kolkata", "WB06": "kolkata",
    "WB07": "kolkata", "WB08": "kolkata", "WB09": "kolkata",
    "WB10": "kolkata",
    "GJ01": "ahmedabad", "GJ27": "ahmedabad",
}

NOT_INCLUDED_TOKENS = {"not included", "n/a", "na", "none", "null", "-", ""}


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def fetch_flat_output(run_id: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT flat_output
        FROM final_flat_output
        WHERE run_id = %s
        """,
        (run_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return []
    return row[0]


def fetch_car_number_for_run(run_id: str) -> str | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT final_data -> 'car_info' ->> 'regNo'
            FROM final_data
            WHERE run_id = %s
            LIMIT 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return clean_text(row[0])
    finally:
        conn.close()


def normalize_code(value) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def to_num(value) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return 0.0
        return float(value)
    text = str(value).strip()
    if not text or text.lower() in NOT_INCLUDED_TOKENS:
        return 0.0
    text = text.replace(",", "").replace("₹", "").replace("%", "").strip()
    try:
        return float(text)
    except ValueError:
        return 0.0


def clean_text(value):
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() in NOT_INCLUDED_TOKENS | {"0", "0.0", "nan"}:
        return None
    return text


def load_rto_city_map() -> dict:
    global _RTO_CITY_MAP
    if _RTO_CITY_MAP is not None:
        return _RTO_CITY_MAP

    csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "rto_locator", "RTO_vehicle_codes.csv")
    rto_df = pd.read_csv(csv_path, usecols=["RegNo", "Place", "State"])
    _RTO_CITY_MAP = {
        normalize_code(row["RegNo"]): {
            "city": str(row["Place"]).strip(),
            "state": str(row["State"]).strip(),
        }
        for _, row in rto_df.iterrows()
        if clean_text(row["RegNo"]) and clean_text(row["Place"])
    }
    return _RTO_CITY_MAP


def resolve_rto_location(rto_location=None, rto_number=None, city=None):
    rto_location_text = clean_text(rto_location)
    if rto_location_text:
        return rto_location_text

    rto_number_text = normalize_code(rto_number)
    rto_info = load_rto_city_map().get(rto_number_text, {}) if rto_number_text else {}
    location_parts = [rto_info.get("city") or clean_text(city), rto_info.get("state")]
    resolved = ", ".join(part for part in location_parts if part)
    return resolved or None


def resolve_city(city, rto_location=None, rto_number=None):
    city_text = clean_text(city)
    if city_text:
        return city_text

    rto_location_text = clean_text(rto_location)
    if rto_location_text:
        first_part = rto_location_text.split(",")[0].strip()
        if clean_text(first_part):
            return first_part

    rto_number_text = normalize_code(rto_number)
    if rto_number_text:
        rto_info = load_rto_city_map().get(rto_number_text, {})
        return rto_info.get("city")

    return None


def normalize_car_number(car_number, rto_number=None) -> str:
    car_number_text = normalize_code(car_number)
    rto_number_text = normalize_code(rto_number)
    if not car_number_text:
        return "0"
    if rto_number_text and car_number_text == rto_number_text:
        return "0"
    return car_number_text


def classify_zone(rto_location, car_number) -> str:
    candidates = []
    if isinstance(rto_location, str) and rto_location.strip():
        candidates.extend(part.strip().lower() for part in rto_location.split(","))
    if car_number:
        cn = car_number.upper().replace(" ", "").replace("-", "")
        for key_len in (4, 2):
            key = cn[:key_len]
            if key in STATE_CODE_TO_CITY:
                candidates.append(STATE_CODE_TO_CITY[key])
                break
    for candidate in candidates:
        if any(metro in candidate for metro in ZONE_A_CITIES):
            return "Zone A"
    return "Zone B"


def age_bucket(yom):
    yom_n = to_num(yom)
    if yom_n <= 0:
        return None
    age = datetime.now().year - int(yom_n)
    if age < 0:
        age = 0
    if age <= 5:
        return "0-5"
    if age <= 10:
        return "5-10"
    return "10+"


def format_age_bucket(age):
    if age == "0-5":
        return "03-05"
    if age == "5-10":
        return "05-10"
    return age or ""


def cc_bucket(cc):
    cc_n = to_num(cc)
    if cc_n <= 0:
        return None
    if cc_n <= 1000:
        return "upto_1000"
    if cc_n <= 1500:
        return "1001_1500"
    return "abv_1500"


def lookup_tariff(zone, age, cc):
    if not age or not cc:
        return 0.0
    return RATE_LOOKUP.get((zone, age, cc), 0.0)


def main(run_id: str):
    flat_rows = fetch_flat_output(run_id)
    if not flat_rows:
        print("❌ No data found for this run_id")
        return

    df = pd.DataFrame(flat_rows)
    df.drop(columns=[c for c in ["run_id", "plan_hidden", "Sr No", "Selected IDV", "OD Discount"] if c in df.columns], inplace=True)

    if "IDV Type" not in df.columns:
        df["IDV Type"] = None
    if "sr_no" in df.columns:
        df.rename(columns={"sr_no": "Sr No"}, inplace=True)
    if "Company" in df.columns:
        df.rename(columns={"Company": "Insurer"}, inplace=True)
    if "City" not in df.columns:
        df["City"] = None
    if "Rto Number" not in df.columns:
        df["Rto Number"] = None
    if "Car Number" not in df.columns:
        df["Car Number"] = None
    if "Rto Location" not in df.columns:
        df["Rto Location"] = None
    if "Extracted Date" not in df.columns:
        df["Extracted Date"] = datetime.now().strftime("%Y-%m-%d")
    if "Bi Fuel Kit Premium" not in df.columns:
        df["Bi Fuel Kit Premium"] = None
    for col in [
        "Age Bucket",
        "Tariff Rate",
        "Tariff Rate Premium",
        "OD Discount Rate",
        "Total discount",
        "Net OD Rate",
        "Nil Dep Rate",
    ]:
        if col not in df.columns:
            df[col] = 0

    fallback_car_number = fetch_car_number_for_run(run_id)
    df["Car Number"] = [
        normalize_car_number(car_number if clean_text(car_number) else fallback_car_number, rto_number)
        for car_number, rto_number in zip(df["Car Number"], df["Rto Number"])
    ]
    df["Rto Location"] = [
        resolve_rto_location(rto_location, rto_number, city)
        for rto_location, rto_number, city in zip(df["Rto Location"], df["Rto Number"], df["City"])
    ]
    df["City"] = [
        resolve_city(city, rto_location, rto_number)
        for city, rto_location, rto_number in zip(df["City"], df["Rto Location"], df["Rto Number"])
    ]

    ages = []
    tariff_rates = []
    tariff_premiums = []
    od_discount_rates = []
    total_discounts = []
    net_od_rates = []
    nil_dep_rates = []

    for _, row in df.iterrows():
        zone = classify_zone(row.get("Rto Location"), row.get("Car Number"))
        age_lookup = age_bucket(row.get("YOM"))
        cc_lookup = cc_bucket(row.get("CC"))
        tariff_rate = lookup_tariff(zone, age_lookup, cc_lookup)

        idv = to_num(row.get("IDV"))
        basic_od = to_num(row.get("Basic OD Premium"))
        other_discount = to_num(row.get("Other Discounts"))
        ncb = to_num(row.get("No-Claim Bonus"))
        nil_dep = to_num(row.get("Nil Dep Premium"))

        tariff_premium = (tariff_rate / 100.0) * idv
        total_discount = other_discount + ncb

        if tariff_premium > 0:
            od_discount_rate = 1 - ((basic_od - other_discount) / tariff_premium)
        else:
            od_discount_rate = 0.0

        if idv > 0:
            net_od_rate = (basic_od - total_discount) / idv
            nil_dep_rate = nil_dep / idv
        else:
            net_od_rate = 0.0
            nil_dep_rate = 0.0

        ages.append(format_age_bucket(age_lookup))
        tariff_rates.append(tariff_rate)
        tariff_premiums.append(tariff_premium)
        od_discount_rates.append(od_discount_rate)
        total_discounts.append(total_discount)
        net_od_rates.append(net_od_rate)
        nil_dep_rates.append(nil_dep_rate)

    df["Age Bucket"] = ages
    df["Tariff Rate"] = tariff_rates
    df["Tariff Rate Premium"] = tariff_premiums
    df["OD Discount Rate"] = od_discount_rates
    df["Total discount"] = total_discounts
    df["Net OD Rate"] = net_od_rates
    df["Nil Dep Rate"] = nil_dep_rates

    column_order = [
        "Car Number",
        "Rto Number",
        "Rto Location",
        "City",
        "Make",
        "Model",
        "Variant",
        "CC",
        "Fuel Type",
        "Insurer",
        "Tariff Rate",
        "NCB %",
        "Nil Dep",
        "YOM",
        "Age Bucket",
        "IDV",
        "IDV Type",
        "Basic OD Premium",
        "Other Discounts",
        "No-Claim Bonus",
        "Tariff Rate Premium",
        "OD Discount Rate",
        "Total discount",
        "Net OD Rate",
        "Nil Dep Premium",
        "Nil Dep Rate",
        "EP Premium",
        "RTI Premium",
        "Consumables",
        "Key & Lock Replacement",
        "Tyre Protector",
        "NCB Protector",
        "RSA Premium",
        "Loss of Personal Belongings",
        "Emergency Transport and Hotel Allowance",
        "Voluntary Deductible Discount",
        "Daily Allowance",
        "Bi Fuel Kit Premium",
        "Total TP Premium",
        "Final Premium visible to customer",
        "Extracted Date",
    ]

    ordered_cols = [c for c in column_order if c in df.columns]
    df = df[ordered_cols]

    def normalize_cell(value):
        if isinstance(value, str):
            val = value.strip().lower()
            if val in {"", "not included", "nan", "none", "null", "n/a", "-"}:
                return 0
        if pd.isna(value):
            return 0
        return value

    for col in df.columns:
        df[col] = df[col].map(normalize_cell)

    if "Basic OD Premium" in df.columns:
        df = df[df["Basic OD Premium"].map(to_num) > 0].reset_index(drop=True)

    df = df.sort_values(by="Insurer", key=lambda col: col.astype(str).str.lower())

    # Remove fully duplicate rows
    before_count = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    after_count = len(df)
    print(f"🧹 Removed {before_count - after_count} duplicate rows")

    # -------------------------------
    # Prepare output naming
    # -------------------------------
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    basename = f"{run_id}_{timestamp}"

    # -------------------------------
    # Split by IDV Type
    # -------------------------------
    default_df = df[
        df["IDV Type"].astype(str).str.strip().str.lower() == "default"
    ].copy()

    median_df = df[
        df["IDV Type"].astype(str).str.strip().str.lower() == "median"
    ].copy()

    # -------------------------------
    # Export Default Excel
    # -------------------------------
    if not default_df.empty:
        default_output_path = os.path.join(
            OUTPUT_DIR,
            f"{basename}_default.xlsx"
        )
        default_df.to_excel(default_output_path, index=False)
        print("✅ Default Excel created")
        print("📄 Path:", default_output_path)
        print("📊 Rows:", len(default_df))

    # -------------------------------
    # Export Median Excel
    # -------------------------------
    if not median_df.empty:
        median_output_path = os.path.join(
            OUTPUT_DIR,
            f"{basename}_median.xlsx"
        )
        median_df.to_excel(median_output_path, index=False)
        print("✅ Median Excel created")
        print("📄 Path:", median_output_path)
        print("📊 Rows:", len(median_df))


def build_excel_df(rows: list) -> "pd.DataFrame":
    """
    Take a list of flat-row dicts (already in final_flat_output format) and
    return a fully-processed DataFrame with computed columns (tariff rates,
    OD discount rates, etc.) and proper column ordering — matching the logic
    used by main() but working with in-memory data instead of a DB fetch.
    """
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    drop_cols = ["run_id", "plan_hidden", "Sr No", "Selected IDV", "OD Discount"]
    df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True)

    if "sr_no" in df.columns:
        df.rename(columns={"sr_no": "Sr No"}, inplace=True)
    if "Company" in df.columns:
        df.rename(columns={"Company": "Insurer"}, inplace=True)

    for col, default in [
        ("IDV Type", None),
        ("City", None),
        ("Rto Number", None),
        ("Car Number", None),
        ("Rto Location", None),
        ("Bi Fuel Kit Premium", None),
    ]:
        if col not in df.columns:
            df[col] = default

    if "Extracted Date" not in df.columns:
        df["Extracted Date"] = datetime.now().strftime("%Y-%m-%d")

    for col in ["Age Bucket", "Tariff Rate", "Tariff Rate Premium",
                "OD Discount Rate", "Total discount", "Net OD Rate", "Nil Dep Rate"]:
        if col not in df.columns:
            df[col] = 0

    df["Car Number"] = [
        normalize_car_number(cn, rn)
        for cn, rn in zip(df["Car Number"], df["Rto Number"])
    ]
    df["Rto Location"] = [
        resolve_rto_location(loc, rn, city)
        for loc, rn, city in zip(df["Rto Location"], df["Rto Number"], df["City"])
    ]
    df["City"] = [
        resolve_city(city, loc, rn)
        for city, loc, rn in zip(df["City"], df["Rto Location"], df["Rto Number"])
    ]

    ages, tariff_rates, tariff_premiums = [], [], []
    od_discount_rates, total_discounts, net_od_rates, nil_dep_rates = [], [], [], []

    for _, row in df.iterrows():
        zone = classify_zone(row.get("Rto Location"), row.get("Car Number"))
        age_lkp = age_bucket(row.get("YOM"))
        cc_lkp = cc_bucket(row.get("CC"))
        tariff_rate = lookup_tariff(zone, age_lkp, cc_lkp)

        idv = to_num(row.get("IDV"))
        basic_od = to_num(row.get("Basic OD Premium"))
        other_discount = to_num(row.get("Other Discounts"))
        ncb_val = to_num(row.get("No-Claim Bonus"))
        nil_dep = to_num(row.get("Nil Dep Premium"))

        tariff_premium = (tariff_rate / 100.0) * idv
        total_discount = other_discount + ncb_val

        od_discount_rate = (
            1 - ((basic_od - other_discount) / tariff_premium)
            if tariff_premium > 0 else 0.0
        )
        net_od_rate = (basic_od - total_discount) / idv if idv > 0 else 0.0
        nil_dep_rate = nil_dep / idv if idv > 0 else 0.0

        ages.append(format_age_bucket(age_lkp))
        tariff_rates.append(tariff_rate)
        tariff_premiums.append(tariff_premium)
        od_discount_rates.append(od_discount_rate)
        total_discounts.append(total_discount)
        net_od_rates.append(net_od_rate)
        nil_dep_rates.append(nil_dep_rate)

    df["Age Bucket"] = ages
    df["Tariff Rate"] = tariff_rates
    df["Tariff Rate Premium"] = tariff_premiums
    df["OD Discount Rate"] = od_discount_rates
    df["Total discount"] = total_discounts
    df["Net OD Rate"] = net_od_rates
    df["Nil Dep Rate"] = nil_dep_rates

    # Use Car Number if any row has a real value; otherwise use Rto Number
    _invalid = {"", "0", "none", "null", "nan", "n/a"}
    has_car_number = df["Car Number"].apply(
        lambda x: bool(x) and str(x).strip().lower() not in _invalid
    ).any()

    if has_car_number:
        leading_id_cols = ["Car Number", "Rto Location", "City"]
    else:
        leading_id_cols = ["Rto Number", "Rto Location", "City"]

    column_order = leading_id_cols + [
        "Make", "Model", "Variant", "CC", "Fuel Type", "Insurer",
        "Tariff Rate", "NCB %", "Nil Dep", "YOM", "Age Bucket", "IDV", "IDV Type",
        "Basic OD Premium", "Other Discounts", "No-Claim Bonus",
        "Tariff Rate Premium", "OD Discount Rate", "Total discount", "Net OD Rate",
        "Nil Dep Premium", "Nil Dep Rate", "EP Premium", "RTI Premium",
        "Consumables", "Key & Lock Replacement", "Tyre Protector", "NCB Protector",
        "RSA Premium", "Loss of Personal Belongings",
        "Emergency Transport and Hotel Allowance", "Voluntary Deductible Discount",
        "Daily Allowance", "Bi Fuel Kit Premium", "Total TP Premium",
        "Final Premium visible to customer", "Extracted Date",
    ]
    ordered_cols = [c for c in column_order if c in df.columns]
    df = df[ordered_cols]

    def _normalize_cell(value):
        if isinstance(value, str):
            if value.strip().lower() in {"", "not included", "nan", "none", "null", "n/a", "-"}:
                return 0
        try:
            if pd.isna(value):
                return 0
        except (TypeError, ValueError):
            pass
        return value

    for col in df.columns:
        df[col] = df[col].map(_normalize_cell)

    if "Basic OD Premium" in df.columns:
        df = df[df["Basic OD Premium"].map(to_num) > 0].reset_index(drop=True)

    # Sort: group by car identity first, then by insurer within each car
    car_id_sort = [c for c in ["Make", "Model", "Variant", "Rto Location", "YOM", "CC"] if c in df.columns]
    sort_cols = car_id_sort + (["Insurer"] if "Insurer" in df.columns else [])
    if sort_cols:
        df = df.sort_values(by=sort_cols, key=lambda s: s.astype(str).str.lower())

    df = df.drop_duplicates().reset_index(drop=True)
    return df


def build_excel_bytes(rows: list) -> bytes:
    """
    Return Excel bytes for the given flat rows.
    Produces two sheets — 'Default' and 'Median' — split by IDV Type,
    matching the pipeline-generated file structure.
    """
    import io as _io

    df = build_excel_df(rows)
    output = _io.BytesIO()

    if df.empty:
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            pd.DataFrame().to_excel(writer, sheet_name="Default", index=False)
        output.seek(0)
        return output.read()

    default_df = df[df["IDV Type"].astype(str).str.strip().str.lower() == "default"].copy()
    median_df = df[df["IDV Type"].astype(str).str.strip().str.lower() == "median"].copy()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        if not default_df.empty:
            default_df.to_excel(writer, sheet_name="Default", index=False)
        if not median_df.empty:
            median_df.to_excel(writer, sheet_name="Median", index=False)
        if default_df.empty and median_df.empty:
            df.to_excel(writer, sheet_name="All", index=False)

    output.seek(0)
    return output.read()


def build_excel_bytes_for_type(rows: list, idv_type: str) -> bytes:
    """
    Return Excel bytes filtered to a single IDV type ('default' or 'median').
    Single sheet named after the IDV type.
    """
    import io as _io

    df = build_excel_df(rows)
    output = _io.BytesIO()
    sheet_name = idv_type.capitalize()

    if not df.empty and "IDV Type" in df.columns:
        df = df[df["IDV Type"].astype(str).str.strip().str.lower() == idv_type.lower()].copy()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        if not df.empty:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            pd.DataFrame().to_excel(writer, sheet_name=sheet_name, index=False)

    output.seek(0)
    return output.read()


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python flatdb_excel.py <run_id>")
    else:
        main(sys.argv[1])