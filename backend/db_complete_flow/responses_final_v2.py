import os
import re
import json
import html
import psycopg2
import psycopg2.extras
import pandas as pd
from pathlib import Path
from psycopg2.extras import Json

from backend.core.config import settings

_BACKEND_ROOT = Path(__file__).resolve().parent.parent
_RTO_CSV = str(_BACKEND_ROOT / "rto_locator" / "RTO_vehicle_codes.csv")


# ─── RTO LOCATOR ─────────────────────────────────────────────────────────────
class RTOLocator:
    def __init__(self, csv_path: str = _RTO_CSV):
        df = pd.read_csv(csv_path)
        df["RegNo"] = df["RegNo"].str.upper().str.strip()
        self._map = {
            row["RegNo"]: {
                "city":       row["Place"],
                "state":      row["State"],
                "state_code": row["StateCode"],
            }
            for _, row in df.iterrows()
        }
        print(f"✅ RTOLocator loaded: {len(self._map)} RTO codes from '{csv_path}'")

    def lookup(self, reg_no: str) -> dict:
        if not reg_no or len(reg_no) < 4:
            return {}
        rto_code = ''.join(filter(str.isalnum, reg_no))[:4].upper()
        return self._map.get(rto_code, {})

    def get_rto_code(self, reg_no: str) -> str | None:
        if not reg_no or len(reg_no) < 4:
            return None
        return ''.join(filter(str.isalnum, reg_no))[:4].upper()


# ─── SINGLETON ───────────────────────────────────────────────────────────────
rto_locator = RTOLocator()


# ─── DB CONFIG ────────────────────────────────────────────────────────────────
DB_HOST     = settings.DB_HOST
DB_PORT     = settings.DB_PORT
DB_NAME     = settings.DB_NAME
DB_USER     = settings.DB_USER
DB_PASSWORD = settings.DB_PASSWORD
DB_SSL      = settings.DB_SSL
DB_SSL_MODE = settings.DB_SSL_MODE

PLANS_DEFAULT_DIR = "plans_json_validation"
PLANS_MEDIAN_DIR  = "plans_median"

# ─── MANUAL OVERRIDE ─────────────────────────────────────────────────────────
RUN_ID_REG_NO_OVERRIDE = {
    "11111111-1111-1111-1111-111111111111": "MH04KW1827",
}

# ─── PIPELINE CONFIG TOGGLES ─────────────────────────────────────────────────
FORCE_REPROCESS = True
USE_LOCAL_FILES = False   # True  → load plan JSONs from local disk
                           # False → fetch plan JSONs from DB


def get_conn():
    conn_params = {
        "host":     DB_HOST,
        "port":     int(DB_PORT),
        "dbname":   DB_NAME,
        "user":     DB_USER,
        "password": DB_PASSWORD,
    }
    if DB_SSL:
        conn_params["sslmode"] = DB_SSL_MODE
    return psycopg2.connect(**conn_params)


# ─── HELPERS ─────────────────────────────────────────────────────────────────
def safe_get(dct, *keys, default=None):
    for key in keys:
        if not isinstance(dct, dict):
            return default
        dct = dct.get(key)
        if dct is None:
            return default
    return dct if dct is not None else default


def clean_amount(value):
    if not isinstance(value, str):
        return None
    if value == "included":
        return "Included"
    value = html.unescape(value).replace("₹", "").replace(",", "").strip()
    return int(value) if value.isdigit() else None


def parse_idv(raw_idv) -> int | None:
    if raw_idv is None:
        return None
    try:
        return int(str(raw_idv).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


# ─── NORMALIZE BUCKET LIST ────────────────────────────────────────────────────
# Format A (old scraper) : response_json -> data -> bucketList
# Format B (new ingester): response_json -> original_response -> data -> data -> bucketList
def extract_bucket_list(response_json: dict) -> list:
    return (
        safe_get(response_json, "data", "bucketList", default=None)
        or safe_get(response_json, "original_response", "data", "data", "bucketList", default=[])
    )


# ─── NORMALIZE CAR DETAILS JSON ───────────────────────────────────────────────
def extract_car_details_api_data(response_json: dict) -> dict:
    candidates = [
        safe_get(response_json, "data"),                               # Format A ← new ingester
        safe_get(response_json, "data", "data"),                       # Format B (old scraper)
        safe_get(response_json, "original_response", "data", "data"),  # Format C (wrapped nested)
        safe_get(response_json, "original_response", "data"),          # Format D (wrapped flat)
    ]

    for api_data in candidates:
        if isinstance(api_data, dict) and api_data.get("registrationNumber"):
            return api_data

    for api_data in candidates:
        if isinstance(api_data, dict) and api_data:
            return api_data

    return {}


# ─── NORMALIZE PLAN DETAIL JSON ───────────────────────────────────────────────
def extract_plan_id_and_combo(payload: dict) -> tuple[int | None, int | None]:
    def _to_int(val) -> int | None:
        try:
            return int(val) if val is not None else None
        except (ValueError, TypeError):
            return None

    # Path 1: top-level snake_case (old scraper)
    plan_id     = _to_int(payload.get("plan_id"))
    addon_combo = _to_int(payload.get("addon_combo_id"))
    if plan_id is not None:
        return plan_id, addon_combo

    # Path 2: top-level camelCase
    plan_id     = _to_int(payload.get("planId"))
    addon_combo = _to_int(payload.get("addonComboId")) if plan_id else addon_combo
    if plan_id is not None:
        return plan_id, addon_combo

    # Path 3: response_json -> data -> planId  ← NEW INGESTER (confirmed)
    data        = payload.get("data") or {}
    plan_id     = _to_int(data.get("planId") or data.get("plan_id"))
    addon_combo = _to_int(data.get("addonComboId") or data.get("addon_combo_id"))
    if plan_id is not None:
        return plan_id, addon_combo

    # Path 4: api_response camelCase (old scraper alt)
    plan_id     = _to_int(safe_get(payload, "api_response", "planId"))
    addon_combo = _to_int(safe_get(payload, "api_response", "addonComboId"))
    if plan_id is not None:
        return plan_id, addon_combo

    # Path 5: api_response -> data
    plan_id     = _to_int(safe_get(payload, "api_response", "data", "planId"))
    addon_combo = _to_int(safe_get(payload, "api_response", "data", "addonComboId"))

    return plan_id, addon_combo


# ─── LOCAL FILE LOADER ───────────────────────────────────────────────────────
def load_local_plan_maps(reg_no: str) -> tuple[dict, dict]:
    def _load_dir(base_dir: str, key_by_combo: bool) -> dict:
        plan_map = {}
        plan_dir = Path(base_dir) / reg_no
        if not plan_dir.exists():
            return plan_map
        for f in sorted(plan_dir.glob("plan_*.json")):
            parts = f.stem.split("_")
            if len(parts) < 3:
                continue
            try:
                plan_id     = int(parts[1])
                addon_combo = int(parts[2])
            except ValueError:
                continue
            try:
                with open(f, "r", encoding="utf-8") as fp:
                    data = json.load(fp)
            except Exception as e:
                print(f"   ⚠️  Failed to load {f.name}: {e}")
                continue
            if key_by_combo:
                plan_map[(plan_id, addon_combo)] = data
            else:
                plan_map.setdefault(plan_id, data)
        return plan_map

    default_map = _load_dir(PLANS_DEFAULT_DIR, key_by_combo=False)
    median_map  = _load_dir(PLANS_MEDIAN_DIR,  key_by_combo=True)

    print(f"   📂 Local plan files — "
          f"default={len(default_map)} (by plan_id)  "
          f"median={len(median_map)} (by plan_id+combo)  "
          f"reg_no={reg_no}")

    return default_map, median_map


# ─── DB PLAN MAP FETCHER ──────────────────────────────────────────────────────
# ✅ reads from quotes_details (raw PlanDetail responses written by ingester/scraper)
# ✅ uses direct columns plan_id + addon_combo_id — no JSON parsing needed
# ✅ this pipeline NEVER writes to quotes_details
def fetch_plan_maps_from_db(conn, run_id: str) -> tuple[dict, dict]:
    default_map = {}
    median_map  = {}
    null_count  = 0

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT DISTINCT ON (idv_type, plan_id, addon_combo_id)
                idv_type,
                plan_id,
                addon_combo_id,
                plan_json AS payload
            FROM quotes_details
            WHERE run_id = %s
            ORDER BY
                idv_type,
                plan_id,
                addon_combo_id,
                created_at DESC
        """, (str(run_id),))
        rows = cur.fetchall()

    print(f"   🗄️  PlanDetail rows fetched from quotes_details: {len(rows)}")

    if rows:
        sample     = dict(rows[0]["payload"])
        data_block = sample.get("data") or {}
        print(f"   🔍 top-level keys        : {list(sample.keys())[:10]}")
        print(f"   🔍 data keys             : {list(data_block.keys())[:10]}")
        print(f"   🔍 data.planId           : {data_block.get('planId') or data_block.get('plan_id')}")
        print(f"   🔍 data.addonComboId     : {data_block.get('addonComboId') or data_block.get('addon_combo_id')}")
        print(f"   🔍 data.premiumBreakup   : {bool(data_block.get('premiumBreakup'))}")
        print(f"   🔍 data.planDetails      : {bool(data_block.get('planDetails'))}")

    for row in rows:
        payload     = row["payload"]
        plan_id     = row["plan_id"]        # direct column — no parsing needed
        addon_combo = row["addon_combo_id"] # direct column

        if plan_id is None:
            null_count += 1
            continue

        if row["idv_type"] == "median":
            median_map[(plan_id, addon_combo)] = payload
        else:
            default_map[plan_id] = payload  # ✅ direct assign — always latest via DISTINCT ON

    if null_count:
        print(f"   ⚠️  Skipped {null_count} rows — plan_id is NULL")

    print(f"   🗄️  DB plan maps — "
          f"default={len(default_map)} (by plan_id)  "
          f"median={len(median_map)} (by plan_id+combo)")

    return default_map, median_map

# ─── UNIFIED PLAN MAP LOADER ──────────────────────────────────────────────────
def load_plan_maps(conn, run_id: str, reg_no: str) -> tuple[dict, dict]:
    if USE_LOCAL_FILES:
        print(f"   📂 [toggle] USE_LOCAL_FILES=True — loading from disk (reg_no={reg_no})")
        if reg_no:
            return load_local_plan_maps(reg_no)
        else:
            print("   ⚠️  USE_LOCAL_FILES=True but reg_no unknown — returning empty maps")
            return {}, {}
    else:
        print(f"   🗄️  [toggle] USE_LOCAL_FILES=False — loading from DB (run_id={run_id})")
        return fetch_plan_maps_from_db(conn, run_id)


# ─── DB FETCHERS ─────────────────────────────────────────────────────────────
def fetch_all_quotes_responses(conn, run_id):
    """
    Fetches best Quotes row per idv_type.
    Prefers: isStopPolling=true → largest bucketList → most recent.
    Handles api_name: 'carapi/Quote/Quotes' OR 'quotes'
    Handles bucketList path: response_json -> data -> bucketList
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT DISTINCT ON (idv_type)
                idv_type,
                idv_selected,
                response_json
            FROM quotes_responses
            WHERE run_id = %s
              AND api_name IN ('carapi/Quote/Quotes', 'quotes')
              AND response_json -> 'data' -> 'bucketList' IS NOT NULL
              AND jsonb_array_length(response_json -> 'data' -> 'bucketList') > 0
            ORDER BY
                idv_type,
                ((response_json -> 'data' ->> 'isStopPolling')::boolean) DESC NULLS LAST,
                jsonb_array_length(response_json -> 'data' -> 'bucketList') DESC,
                created_at DESC;
        """, (str(run_id),))

        rows = cur.fetchall()

        print(f"\n🔎 fetch_all_quotes_responses RESULT")
        print(f"Total rows returned: {len(rows)}")
        for i, r in enumerate(rows, 1):
            bucket_list = extract_bucket_list(r.get("response_json") or {})
            print(f"  Row {i} → idv_type={r['idv_type']} | "
                  f"bucketList length={len(bucket_list)} | "
                  f"isStopPolling={r['response_json'].get('data', {}).get('isStopPolling')}")
        print("-" * 50)
        return rows


def fetch_car_info_response(conn, run_id):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT response_json
            FROM quotes_responses
            WHERE run_id = %s
              AND api_name IN (
                  'carapi/Quote/CarDetails',
                  'CarDetails',
                  'cardetails'
              )
            ORDER BY created_at DESC
            LIMIT 1
        """, (str(run_id),))
        return cur.fetchone()


def fetch_reg_no_for_run(conn, run_id: str) -> str | None:
    override = RUN_ID_REG_NO_OVERRIDE.get(str(run_id))
    if override:
        print(f"   🔧 reg_no from manual override: {override}")
        return override.upper()

    # ── Attempt 1: scrape_runs.registration_number column ────────────────────
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        try:
            cur.execute("""
                SELECT registration_number FROM scrape_runs
                WHERE run_id = %s AND registration_number IS NOT NULL LIMIT 1
            """, (str(run_id),))
            row = cur.fetchone()
            if row and row["registration_number"]:
                return row["registration_number"].upper()
        except psycopg2.Error:
            conn.rollback()

    # ── Attempt 2: scrape_runs JSON columns ──────────────────────────────────
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        try:
            cur.execute("""
                SELECT COALESCE(
                    run_data   ->> 'regNo', run_data   ->> 'registration_number',
                    metadata   ->> 'regNo', metadata   ->> 'registration_number',
                    input_data ->> 'regNo', input_data ->> 'registration_number'
                ) AS reg_no FROM scrape_runs WHERE run_id = %s LIMIT 1
            """, (str(run_id),))
            row = cur.fetchone()
            if row and row["reg_no"]:
                return row["reg_no"].upper()
        except psycopg2.Error:
            conn.rollback()

    # ── Attempt 3: quotes_responses car_info / regNo ──────────────────────────
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT COALESCE(
                response_json -> 'car_info' ->> 'regNo',
                response_json ->> 'regNo'
            ) AS reg_no FROM quotes_responses
            WHERE run_id = %s
              AND (response_json -> 'car_info' ->> 'regNo' IS NOT NULL
                OR response_json ->> 'regNo' IS NOT NULL)
            LIMIT 1
        """, (str(run_id),))
        row = cur.fetchone()
        if row and row["reg_no"]:
            return row["reg_no"].upper()

    # ── Attempt 4: Quotes response original_response / request paths ──────────
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT COALESCE(
                response_json -> 'original_response' -> 'data' -> 'data' ->> 'vehicleNumber',
                response_json -> 'original_response' -> 'data' -> 'data' ->> 'regNo',
                response_json -> 'original_response' -> 'data' ->> 'vehicleNumber',
                response_json -> 'original_response' -> 'data' ->> 'regNo',
                response_json -> 'input_data'  ->> 'regNo',
                response_json -> 'input_data'  ->> 'vehicleNumber',
                response_json -> 'request'     ->> 'regNo',
                response_json -> 'request'     ->> 'vehicleNumber'
            ) AS reg_no FROM quotes_responses
            WHERE run_id = %s
              AND api_name IN ('carapi/Quote/Quotes', 'Quotes', 'quotes')
            LIMIT 1
        """, (str(run_id),))
        row = cur.fetchone()
        if row and row["reg_no"]:
            return row["reg_no"].upper()

    # ── Attempt 5: CarDetails response ───────────────────────────────────────
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT COALESCE(
                response_json -> 'data' ->> 'registrationNumber',
                response_json -> 'data' -> 'gaEventObj' ->> 'regNo'
            ) AS reg_no FROM quotes_responses
            WHERE run_id = %s
              AND api_name IN ('carapi/Quote/CarDetails', 'CarDetails', 'cardetails')
            LIMIT 1
        """, (str(run_id),))
        row = cur.fetchone()
        if row and row["reg_no"]:
            return row["reg_no"].upper()

    return None


# ─── EXTRACTION ──────────────────────────────────────────────────────────────
def extract_car_info(response_json: dict) -> dict:
    api_data = extract_car_details_api_data(response_json)
    ga_obj   = api_data.get("gaEventObj") or {}

    registration_no = api_data.get("registrationNumber") or ga_obj.get("regNo")

    rto_info   = rto_locator.lookup(registration_no or "")
    rto_code   = rto_locator.get_rto_code(registration_no or "")
    state_code = rto_info.get("state_code")
    rto_city   = rto_info.get("city")
    rto_state  = rto_info.get("state")

    if rto_info:
        print(f"   🗺️  RTO: {rto_code} → {rto_city}, {rto_state} (state_code={state_code})")
    else:
        print(f"   ⚠️  RTO lookup: no match for '{registration_no}' (rto_code={rto_code})")

    raw_cc = ga_obj.get("cubicCapacity") or api_data.get("cubicCapacity")
    cubic_capacity = None
    if raw_cc:
        match = re.search(r"\d+", str(raw_cc))
        if match:
            cubic_capacity = int(match.group())

    return {
        "makeName":         ga_obj.get("makeName"),
        "modelName":        ga_obj.get("modelName"),
        "vehicle_variant":  api_data.get("variant"),
        "regNo":            registration_no,
        "fuelType":         api_data.get("fuelType"),
        "cubicCapacity":    cubic_capacity,
        "stateCode":        state_code,
        "rto_location":     f"{rto_city or ''},{rto_state or ''}".strip(","),
        "cityTier":         ga_obj.get("cityTier"),
        "coverType":        "Comprehensive",
        "carAge":           ga_obj.get("carAge"),
        "registrationDate": api_data.get("registrationDate"),
        "policyExpiryDate": api_data.get("policyExpiryDate") or ga_obj.get("policyExpiryDate") or "",
    }


def parse_premium_breakdown(premium_breakup: dict) -> dict:
    result = {}

    for item in safe_get(premium_breakup, "baseCover", "listBreakups", default=[]):
        amt = clean_amount(item.get("value"))
        if amt is not None:
            result[item["key"]] = amt

    for item in safe_get(premium_breakup, "discounts", "listBreakups", default=[]):
        amt = clean_amount(item.get("value"))
        if amt is not None:
            result[item["key"]] = amt

    addons = {
        item["key"]: clean_amount(item.get("value"))
        for item in safe_get(premium_breakup, "addonsAndAccessories", "listBreakups", default=[])
        if clean_amount(item.get("value")) is not None
    }
    if addons:
        result["Addon & Accessories"] = addons

    prem_details = {
        item["key"]: clean_amount(item.get("value"))
        for item in safe_get(premium_breakup, "premiumDetails", "listBreakups", default=[])
        if clean_amount(item.get("value")) is not None
    }
    if prem_details:
        result["Premium Details"] = prem_details

    final_amt = clean_amount(safe_get(premium_breakup, "finalPremium", "premium"))
    if final_amt is not None:
        result["finalPremium"] = final_amt

    return {"premium_breakdown": result}


def parse_insurer_sights(plan_details: dict) -> dict:
    key_points = safe_get(
        plan_details, "insurerSights", "sectionDetails", "keyPoints", default=[]
    )

    points = []
    for item in key_points:
        if not item:
            continue
        raw_title   = item.get("title") or ""
        clean_title = raw_title.replace("<b>", "").replace("</b>", "").strip()
        raw_desc    = item.get("infoDescription") or ""

        detail_section = item.get("detailSection") or {}
        statistics = [
            {
                "title": s.get("title", "").strip(),
                "value": s.get("value"),
            }
            for s in detail_section.get("statistics") or []
            if s and s.get("title")
        ]

        if clean_title or raw_desc:
            points.append({
                "title":           clean_title,
                "infoDescription": raw_desc,
                "statistics":      statistics,
            })

    return {"insurerSights": {"count": len(points), "points": points}}


def get_coverage_for_plan(plan_response_json: dict) -> dict:
    empty = {
        "whatsCovered":        {"count": 0, "text": []},
        "whatsNotCovered":     {"count": 0, "text": []},
        "addonBenifits":       {"count": 0, "text": []},
        "insurerSights":       {"count": 0, "points": []},
        "premium_breakdown":   {},
        "idv_from_detail":     None,
        "insurer_from_detail": None,
    }
    if not plan_response_json:
        return empty

    try:
        api_data = (
            safe_get(plan_response_json, "api_response", "data")
            or plan_response_json.get("data")
            or {}
        )

        plan_details    = api_data.get("planDetails") or {}
        premium_breakup = api_data.get("premiumBreakup") or {}

        covered_text = [
            item.get("headText", "")
            for item in safe_get(plan_details, "whatsCovered", "sectionDetails", "keyPoints", default=[])
            if item and item.get("headText")
        ]
        not_covered_text = [
            item.get("headText", "")
            for item in safe_get(plan_details, "whatsNotCovered", "sectionDetails", "keyPoints", default=[])
            if item and item.get("headText")
        ]
        addon_text = [
            item.get("title", "")
            for item in safe_get(plan_details, "addonBenifits", "sectionDetails", "keyPoints", default=[])
            if item and item.get("title")
        ]

        insurer_sights      = parse_insurer_sights(plan_details)
        idv_from_detail     = parse_idv(api_data.get("idv"))
        insurer_from_detail = api_data.get("insurer")

        return {
            "whatsCovered":        {"count": len(covered_text),     "text": covered_text},
            "whatsNotCovered":     {"count": len(not_covered_text), "text": not_covered_text},
            "addonBenifits":       {"count": len(addon_text),       "text": addon_text},
            "idv_from_detail":     idv_from_detail,
            "insurer_from_detail": insurer_from_detail,
            **insurer_sights,
            **parse_premium_breakdown(premium_breakup),
        }
    except Exception as e:
        print(f"⚠️  Coverage parse error: {e}")
        return empty


# ─── BUILD PLAN ROWS ─────────────────────────────────────────────────────────
def build_plan_rows(run_id, quotes_rows: list,
                    default_map: dict, median_map: dict) -> tuple[list, list]:
    plans_rows = []
    all_plans  = []
    seen       = set()

    for qrow in quotes_rows:
        idv_type      = qrow.get("idv_type")
        idv_selected  = qrow.get("idv_selected")
        response_json = qrow["response_json"]

        bucket_list = extract_bucket_list(response_json)

        for bucket in (bucket_list or []):
            for plan in (bucket.get("plans") or []):
                idv          = plan.get("idv")
                insurer_name = plan.get("insurerName")

                for pd in (plan.get("planDetails") or []):
                    plan_id     = pd.get("planId")
                    addon_combo = int(pd["addonComboId"]) if pd.get("addonComboId") is not None else None
                    plan_hidden = pd.get("isHidePlan", False)
                    plan_name   = pd.get("planName") or plan.get("planName")

                    if not plan_id or not insurer_name:
                        continue

                    dedup_key = (plan_id, addon_combo, idv_type)
                    if dedup_key in seen:
                        continue
                    seen.add(dedup_key)

                    if idv_type == "median":
                        local_plan_json = median_map.get((plan_id, addon_combo))
                    else:
                        local_plan_json = default_map.get(plan_id)

                    if local_plan_json:
                        coverage = get_coverage_for_plan(local_plan_json)
                    else:
                        claim_highlights = pd.get("claimHighlights") or {}
                        covered_text = [
                            item.get("title", "")
                            for item in (claim_highlights.get("points") or [])
                            if item and item.get("title")
                        ]
                        addon_hl   = pd.get("addonHighlightsSection") or {}
                        addon_text = [
                            item.get("title", "")
                            for item in (addon_hl.get("points") or [])
                            if item and item.get("title")
                        ]
                        coverage = {
                            "whatsCovered":        {"count": len(covered_text), "text": covered_text},
                            "whatsNotCovered":     {"count": 0,                 "text": []},
                            "addonBenifits":       {"count": len(addon_text),   "text": addon_text},
                            "insurerSights":       {"count": 0,                 "points": []},
                            "idv_from_detail":     None,
                            "insurer_from_detail": None,
                            "premium_breakdown":   {},
                        }

                    plan_json = {
                        "planId":              plan_id,
                        "addonComboId":        addon_combo,
                        "planName":            plan_name,
                        "planHidden":          plan_hidden,
                        "idv_type":            idv_type,
                        "basePremium":         pd.get("basePremium"),
                        "premium":             pd.get("premium"),
                        "idv":                 idv,
                        "insurerName":         insurer_name,
                        "otherAddons_count":   coverage["addonBenifits"]["count"],
                        "whatsCovered":        coverage["whatsCovered"],
                        "whatsNotCovered":     coverage["whatsNotCovered"],
                        "addonBenifits":       coverage["addonBenifits"],
                        "insurerSights":       coverage["insurerSights"],
                        "claimSettlementPerc": plan.get("claimSettlementPerc"),
                        "claimHighlights":     pd.get("claimHighlights"),
                        "keyHighLights":       pd.get("keyHighLights"),
                        **coverage.get("premium_breakdown", {}),
                    }

                    plans_rows.append((
                        str(run_id),
                        insurer_name,
                        plan_id,
                        Json(plan_json),
                        addon_combo,
                        idv_type,
                        idv_selected,
                    ))

                    all_plans.append({
                        "planName":            plan_name,
                        "planId":              plan_id,
                        "addonComboId":        addon_combo,
                        "planHidden":          plan_hidden,
                        "idv_type":            idv_type,
                        "basePremium":         plan_json["basePremium"],
                        "premium":             plan_json["premium"],
                        "idv":                 plan_json["idv"],
                        "insurerName":         insurer_name,
                        "otherAddons_count":   plan_json["otherAddons_count"],
                        "whatsCovered":        plan_json["whatsCovered"],
                        "whatsNotCovered":     plan_json["whatsNotCovered"],
                        "addonBenifits":       plan_json["addonBenifits"],
                        "insurerSights":       plan_json["insurerSights"],
                        "claimSettlementPerc": plan_json["claimSettlementPerc"],
                        "claimHighlights":     plan_json["claimHighlights"],
                        "keyHighLights":       plan_json["keyHighLights"],
                        "premium_breakdown":   coverage.get("premium_breakdown", {}),
                    })

    return plans_rows, all_plans


# ─── DB WRITERS ──────────────────────────────────────────────────────────────
def insert_car_info(conn, run_id, car_info: dict):
    reg_no = car_info.get("regNo")
    if not reg_no:
        print(f"   ⚠️  Skipping insert_car_info — registration_number is NULL for run {run_id}")
        return

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO car_info (
                run_id, registration_number, make_name, model_name,
                vehicle_variant, fuel_type, cubic_capacity,
                state_code, city_tier, car_age, registration_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            str(run_id),
            reg_no,
            car_info.get("makeName"),
            car_info.get("modelName"),
            car_info.get("vehicle_variant"),
            car_info.get("fuelType"),
            car_info.get("cubicCapacity"),
            car_info.get("stateCode"),
            car_info.get("cityTier"),
            car_info.get("carAge"),
            car_info.get("registrationDate"),
        ))
    conn.commit()


def upsert_final_data(conn, run_id, car_info: dict, all_plans: list):
    """Groups all_plans by planName → bucketList."""
    plans_by_name = {}

    for p in all_plans:
        name = p.get("planName")
        if not name:
            print(f"  ⚠️  Skipping plan with no planName: planId={p.get('planId')} insurer={p.get('insurerName')}")
            continue
        plan_copy = dict(p)
        plan_copy.pop("planName", None)
        plans_by_name.setdefault(name, []).append(plan_copy)

    bucket_list = [
        {"planName": plan_name, "plans": plans_list}
        for plan_name, plans_list in sorted(plans_by_name.items(), key=lambda x: x[0])
    ]

    final = {"car_info": car_info, "bucketList": bucket_list}

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO final_data (run_id, final_data)
            VALUES (%s, %s)
            ON CONFLICT (run_id) DO UPDATE
                SET final_data = EXCLUDED.final_data,
                    updated_at = CURRENT_TIMESTAMP
        """, (str(run_id), Json(final)))
    conn.commit()

    print(f"   📊 planName groups ({len(bucket_list)}):")
    for group in bucket_list:
        insurers = [p["insurerName"] for p in group["plans"]]
        print(f"      {group['planName']:45s} → {len(group['plans'])} plans  "
              f"[{', '.join(insurers[:4])}{'...' if len(insurers) > 4 else ''}]")


# ─── RESET ───────────────────────────────────────────────────────────────────
# ✅ quotes_details is NOT touched — it's a raw source table owned by the ingester
def reset_run(conn, run_id):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM final_data WHERE run_id = %s", (str(run_id),))
        cur.execute("DELETE FROM car_info   WHERE run_id = %s", (str(run_id),))
    conn.commit()
    print(f"🗑️  Cleared final_data / car_info for run {run_id}")


# ─── DEBUG ───────────────────────────────────────────────────────────────────
def debug_run(conn, run_id):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

        # ── 1. quotes_details — source row count (READ-ONLY, owned by ingester) ──
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM quotes_details WHERE run_id = %s",
            (str(run_id),)
        )
        qd_count = cur.fetchone()["cnt"]
        print(f"  quotes_details rows (source) : {qd_count}")

        if qd_count:
            cur.execute("""
                SELECT insurer_name,
                       plan_id,
                       addon_combo_id,
                       idv_type,
                       plan_json -> 'data' ? 'premiumBreakup' AS has_premium_breakup,
                       plan_json -> 'data' ? 'planDetails'    AS has_plan_details
                FROM quotes_details
                WHERE run_id = %s
                LIMIT 5
            """, (str(run_id),))
            rows = cur.fetchall()
            for row in rows:
                print(f"    {str(row['insurer_name'] or '?'):30s}  "
                      f"plan={row['plan_id']}  "
                      f"combo={row['addon_combo_id']}  "
                      f"idv_type={row['idv_type']}  "
                      f"has_premiumBreakup={row['has_premium_breakup']}  "
                      f"has_planDetails={row['has_plan_details']}")

        # ── 2. final_data — pipeline output ──────────────────────────────────
        cur.execute("""
            SELECT
                final_data ? 'bucketList'                      AS has_bucket,
                jsonb_array_length(final_data -> 'bucketList') AS bucket_count
            FROM final_data
            WHERE run_id = %s
        """, (str(run_id),))
        row = cur.fetchone()
        if row:
            print(f"  final_data                   : exists=True  "
                  f"has_bucketList={row['has_bucket']}  "
                  f"bucket_groups={row['bucket_count']}")

            cur.execute("""
                SELECT
                    grp ->> 'planName'                                       AS plan_name,
                    jsonb_array_length(grp -> 'plans')                       AS plan_count,
                    (grp -> 'plans' -> 0 -> 'whatsCovered'    ->> 'count')  AS covered_count,
                    (grp -> 'plans' -> 0 -> 'whatsNotCovered' ->> 'count')  AS not_covered_count,
                    (grp -> 'plans' -> 0 -> 'addonBenifits'   ->> 'count')  AS addon_count,
                    (grp -> 'plans' -> 0 -> 'insurerSights'   ->> 'count')  AS sights_count
                FROM final_data,
                     jsonb_array_elements(final_data -> 'bucketList') AS grp
                WHERE run_id = %s
                LIMIT 5
            """, (str(run_id),))
            bucket_rows = cur.fetchall()
            for br in bucket_rows:
                print(f"    {str(br['plan_name'] or '?'):45s}  "
                      f"plans={br['plan_count']}  "
                      f"covered={br['covered_count']}  "
                      f"not_covered={br['not_covered_count']}  "
                      f"addons={br['addon_count']}  "
                      f"sights={br['sights_count']}")
        else:
            print(f"  final_data                   : exists=False")

        # ── 3. car_info ───────────────────────────────────────────────────────
        cur.execute("""
            SELECT make_name, model_name, registration_number, state_code
            FROM car_info WHERE run_id = %s
        """, (str(run_id),))
        row = cur.fetchone()
        if row:
            rto_info = rto_locator.lookup(row["registration_number"] or "")
            print(f"  car_info                     : {row['make_name']} {row['model_name']} "
                  f"— {row['registration_number']}  "
                  f"state_code={row['state_code']}  "
                  f"city={rto_info.get('city', '?')}  "
                  f"state={rto_info.get('state', '?')}")
        else:
            print(f"  car_info                     : not found")


# ─── PROCESS ONE RUN ─────────────────────────────────────────────────────────
def process_run(conn, run_id, force: bool = False) -> int:
    # ✅ existence check against final_data (pipeline output), not quotes_details
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM final_data WHERE run_id = %s", (str(run_id),))
        already_exists = cur.fetchone()[0] > 0

    if already_exists:
        if force:
            reset_run(conn, run_id)
        else:
            print(f"⏭️  run {run_id}: already processed (set force=True to reprocess)")
            return 0

    quotes_rows = fetch_all_quotes_responses(conn, run_id)
    if not quotes_rows:
        print(f"⚠️  run {run_id}: No Quote/Quotes response with bucketList found")
        return 0

    print(f"   📥 Quote/Quotes rows: {len(quotes_rows)} ({[r['idv_type'] for r in quotes_rows]})")

    ci_row   = fetch_car_info_response(conn, run_id)
    car_info = extract_car_info(ci_row["response_json"]) if ci_row else {}

    # ── Patch reg_no into car_info if extract_car_info missed it ─────────────
    reg_no = (car_info.get("regNo") or "").upper()
    if not reg_no:
        reg_no = fetch_reg_no_for_run(conn, run_id) or ""
        if reg_no:
            car_info["regNo"] = reg_no
            print(f"   🔍 reg_no patched into car_info via fallback: {reg_no}")
        else:
            print(f"   ⚠️  reg_no unknown — car_info insert will be skipped, plan maps may be empty")

    if car_info:
        insert_car_info(conn, run_id, car_info)
        print(f"   🚗 Car : {car_info.get('makeName')} {car_info.get('modelName')} "
              f"— {car_info.get('regNo')}  "
              f"state_code={car_info.get('stateCode')}")

    default_map, median_map = load_plan_maps(conn, run_id, reg_no)

    plans_rows, all_plans = build_plan_rows(
        run_id, quotes_rows, default_map, median_map
    )

    if not plans_rows:
        print(f"⚠️  run {run_id}: no visible plans extracted")
        return 0

    print(f"   📦 Plans extracted: {len(plans_rows)}")

    # ✅ ONLY final_data is written — quotes_details is never touched by this pipeline
    upsert_final_data(conn, run_id, car_info, all_plans)

    return len(plans_rows)


# ─── MAIN ────────────────────────────────────────────────────────────────────
def main():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT run_id, status FROM scrape_runs ORDER BY created_at DESC")
            rows = cur.fetchall()

        print("=" * 60)
        print(f"Processing {len(rows)} run(s)...  "
              f"[force={FORCE_REPROCESS}]  "
              f"[use_local_files={USE_LOCAL_FILES}]")
        print("=" * 60)

        summary = []
        for row in rows:
            run_id = row["run_id"]
            print(f"\n▶  run_id={run_id}  status='{row['status']}'")
            count = process_run(conn, run_id, force=FORCE_REPROCESS)
            if count:
                summary.append((run_id, count))
                print(f"✅ run {run_id}: {count} plans → final_data")
                debug_run(conn, run_id)

        print("\n" + "=" * 60)
        print(f"Done. {len(summary)}/{len(rows)} runs produced plans.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()