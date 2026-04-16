import psycopg2
import json
from copy import deepcopy

from backend.core.config import settings


# -------------------------------
# DB CONFIG
# -------------------------------
DB_HOST     = settings.DB_HOST
DB_PORT     = settings.DB_PORT
DB_NAME     = settings.DB_NAME
DB_USER     = settings.DB_USER
DB_PASSWORD = settings.DB_PASSWORD
DB_SSL      = settings.DB_SSL
DB_SSL_MODE = settings.DB_SSL_MODE


# -------------------------------
# DB TABLES
# -------------------------------
FINAL_TABLE = "final_data"
FLAT_TABLE  = "final_flat_output"


# -------------------------------
# Connect to DB
# -------------------------------
def get_connection():
    conn_params = {
        "host":               DB_HOST,
        "port":               DB_PORT,
        "dbname":             DB_NAME,
        "user":               DB_USER,
        "password":           DB_PASSWORD,
        "connect_timeout":    10,
        "keepalives":         1,
        "keepalives_idle":    30,
        "keepalives_interval":10,
        "keepalives_count":   5,
    }
    if DB_SSL:
        conn_params["sslmode"] = DB_SSL_MODE
    return psycopg2.connect(**conn_params)


# -------------------------------
# Fetch final_data
# -------------------------------
def fetch_final_data():
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute(f"SELECT run_id, final_data FROM {FINAL_TABLE}")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows  # list of tuples: (run_id, final_data JSONB)


def fetch_final_data_for_run(run_id):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute(f"SELECT run_id, final_data FROM {FINAL_TABLE} WHERE run_id = %s", (str(run_id),))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row  # tuple: (run_id, final_data JSONB) or None


# -------------------------------
# Fetch eligible NCB from quotes_responses
# Confirmed JSON path: response_json -> data -> eligibleNCB
# api_name variants:
#   Old scraper  → 'carapi/Quote/NcbData'
#   New ingester → 'NcbData'
# -------------------------------
def fetch_all_ncb(run_id) -> dict:
    """
    Returns a dict mapping idv_type → eligible NCB % (int)

    Tries both JSON paths in priority order:
      Path 1 (new ingester) : response_json -> data -> eligibleNCB
      Path 2 (old scraper)  : response_json -> data -> data -> eligibleNCB
    """
    conn = get_connection()
    cur  = conn.cursor()

    cur.execute("""
        SELECT
            idv_type,
            COALESCE(
                response_json -> 'data'         ->> 'eligibleNCB',
                response_json -> 'data' -> 'data' ->> 'eligibleNCB'
            ) AS eligible_ncb,
            COALESCE(
                response_json -> 'data'         ->> 'existingNCB',
                response_json -> 'data' -> 'data' ->> 'existingNCB'
            ) AS existing_ncb
        FROM quotes_responses
        WHERE run_id  = %s
          AND api_name IN ('carapi/Quote/NcbData', 'NcbData')
    """, (str(run_id),))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    ncb_dict = {}
    for idv_type, eligible_ncb, existing_ncb in rows:
        if eligible_ncb is not None:
            ncb_dict[idv_type] = int(eligible_ncb)
            print(f"   💰 NCB [{idv_type}]: eligibleNCB={eligible_ncb}%  existingNCB={existing_ncb}%")

    if not ncb_dict:
        print(f"   ⚠️  NCB fetch: no eligibleNCB found for run {run_id}")

    return ncb_dict


# -------------------------------
# CC Range helper
# -------------------------------
def get_cc_range(cc: int | None) -> str | None:
    if cc is None:
        return None
    if cc <= 1000:
        return "Up to 1000"
    elif cc <= 1500:
        return "1000 - 1500"
    elif cc <= 2000:
        return "1500 - 2000"
    else:
        return "Above 2000"


# -------------------------------
# Flatten logic
# -------------------------------
def flatten_final_data(run_id, final_data, eligible_ncb_dict: dict) -> list:
    car_info  = final_data.get("car_info", {})
    flat_rows = []
    sr_no     = 1

    # ── Collect all dynamic addon names across all plans ─────────────────────
    all_addons = set()
    for bucket in final_data.get("bucketList", []):
        if bucket.get("planName") in ["Limited Third Party", "Third Party"]:
            continue
        for plan in bucket.get("plans", []):
            addons = plan.get("premium_breakdown", {}).get("Addon & Accessories", {})
            for addon in addons:
                if addon in ["Engine Protector", "Engine Protection Cover"]:
                    all_addons.add("Engine Protector")
                else:
                    all_addons.add(addon)

    # ── Flatten each plan ─────────────────────────────────────────────────────
    for bucket in final_data.get("bucketList", []):
        if bucket.get("planName") in ["Limited Third Party", "Third Party"]:
            continue

        for plan in bucket.get("plans", []):
            pb          = plan.get("premium_breakdown", {})
            addons      = pb.get("Addon & Accessories", {})
            basic_od    = pb.get("Basic Own Damage Premium")
            idv         = plan.get("idv")
            idv_type    = plan.get("idv_type")

            # ── NCB: lookup by idv_type, fallback to first available ──────────
            eligible_ncb = (
                eligible_ncb_dict.get(idv_type)
                or next(iter(eligible_ncb_dict.values()), None)
            )

            # ── CC & CC Range ─────────────────────────────────────────────────
            cc_int   = car_info.get("cubicCapacity")
            cc_range = get_cc_range(cc_int)

            # ── RTO location ──────────────────────────────────────────────────
            rto_location = car_info.get("rto_location")

            # ── Tariff Rate ───────────────────────────────────────────────────
            try:
                basic_od_f  = float(basic_od) if basic_od is not None else None
                idv_f       = float(idv)       if idv       is not None else None
                tariff_rate = round((basic_od_f / idv_f) * 100, 2) if basic_od_f and idv_f else None
            except (ValueError, TypeError):
                tariff_rate = None

            # ── Nil Dep ───────────────────────────────────────────────────────
            nil_dep_premium = addons.get("Zero Depreciation")
            nil_dep         = "Yes" if nil_dep_premium not in [None, 0] else "No"

            # ── YOM from registration date ────────────────────────────────────
            reg_date = car_info.get("registrationDate")
            yom      = str(reg_date)[:4] if reg_date else None

            row = {
                "run_id":   str(run_id),
                "sr_no":    sr_no,
                "plan_hidden": plan.get("plan_hidden", False),

                # ── Car info ──────────────────────────────────────────────────
                "Rto Location": rto_location,
                "Make":         car_info.get("makeName"),
                "Model":        car_info.get("modelName"),
                "Variant":      car_info.get("vehicle_variant"),
                "CC":           cc_int,
                "CC Range":     cc_range,
                "Fuel Type":    car_info.get("fuelType"),
                "YOM":          yom,

                # ── Plan info ─────────────────────────────────────────────────
                "Company":  plan.get("insurerName"),
                "NCB %":    f"{eligible_ncb}%" if eligible_ncb is not None else None,
                "Nil Dep":  nil_dep,
                "IDV":      idv_f,
                "IDV Type": idv_type,

                # ── Premium fields ────────────────────────────────────────────
                "Tariff Rate":     tariff_rate,
                "Basic OD Premium": basic_od_f,

                "No-Claim Bonus":              pb.get("No-Claim Bonus"),
                "Voluntary Deductible Discount": (
                    pb.get("Voluntary Deductible Discount")
                    or pb.get("Voluntary Deductible")
                    or pb.get("Voluntary Deductible Discount Amount")
                ),
                "Other Discounts": pb.get("Other Discounts"),
                "OD Discount":     pb.get("Other Discounts"),

                # ── Addon premiums ────────────────────────────────────────────
                "Nil Dep Premium":       nil_dep_premium,
                "EP Premium":            addons.get("Engine Protector") or addons.get("Engine Protection Cover"),
                "RTI Premium":           addons.get("Invoice Price"),
                "RSA Premium":           addons.get("24x7 Roadside Assistance"),
                "Consumables":           addons.get("Consumables"),
                "Key & Lock Replacement":addons.get("Key & Lock Replacement"),
                "Tyre Protector":        addons.get("Tyre Protector"),

                # ── Totals ────────────────────────────────────────────────────
                "Total TP Premium":                 pb.get("Third Party Cover Premium"),
                "Final Premium visible to customer": pb.get("finalPremium"),
            }

            # ── Remaining dynamic addons ──────────────────────────────────────
            skip_addons = {
                "Zero Depreciation", "Engine Protector", "Engine Protection Cover",
                "Invoice Price", "24x7 Roadside Assistance", "Consumables",
                "Key & Lock Replacement", "Tyre Protector",
            }
            for addon in sorted(all_addons):
                if addon in skip_addons:
                    continue
                row.setdefault(addon, addons.get(addon))

            flat_rows.append(deepcopy(row))
            sr_no += 1

    return flat_rows


# -------------------------------
# Insert flattened rows into DB
# -------------------------------
def save_flat_output(run_id, flat_rows):
    if not flat_rows:
        return

    conn = get_connection()
    cur  = conn.cursor()

    json_array = json.dumps(flat_rows)

    cur.execute(
        f"""
        INSERT INTO {FLAT_TABLE} (run_id, flat_output)
        VALUES (%s, %s)
        """,
        (str(run_id), json_array)
    )

    conn.commit()
    cur.close()
    conn.close()


# -------------------------------
# MAIN
# -------------------------------
def main():
    final_rows = fetch_final_data()
    print(f"Found {len(final_rows)} run(s) in final_data table")

    total_flat_rows = 0
    for run_id, final_data in final_rows:
        eligible_ncb_dict = fetch_all_ncb(run_id)
        flat_rows         = flatten_final_data(run_id, final_data, eligible_ncb_dict)
        save_flat_output(run_id, flat_rows)
        total_flat_rows  += len(flat_rows)
        print(f"✅ Run {run_id}: {len(flat_rows)} flat rows saved")

    print(f"\n✅ Total flat rows saved: {total_flat_rows}")


if __name__ == "__main__":
    main()
