import asyncio
import json
import os
import pathlib
import random
import sys
from datetime import datetime
from typing import Optional
from camoufox.async_api import AsyncCamoufox
import uuid

sys.path.append(pathlib.Path(__file__).parent.parent.as_posix())

from db_and_logging import (
    ScrapeLogger,
    LiveDBSync,
    get_connection,
    create_scrape_run,
    insert_scrape_input,
    insert_data_usage,
)
from policy_bazaar_utils.pb_utils import (
    get_rotated_proxy,
    human_delay,
)
from policy_bazaar_utils.pb_flow_utils import (
    get_rotated_name_and_mobile,
    setup_all_popup_handlers,
    detect_quotes_page_indicators,
    set_idv_to_median,
    step_1_navigate_to_motor_insurance,
    step_10_select_policy_expiry,
    step_11_select_claim_status,
    step_12_handle_car_question_popup,
    wait_for_page_ready,
    setup_addons,
    navigate_to_quotes,
)
from policy_bazaar_utils.registration_number_less_utils import apply_ncb
# ── API-FIRST IMPORTS ─────────────────────────────────────────────────────────
from policy_bazaar_utils.pb_parallel_coverage_utils import (
    extract_plan_cards_from_quotes,
    fetch_all_coverages_parallel,
    get_latest_quotes_from_db,
)
from policy_bazaar_utils.pb_data_tracker import DataUsageTracker
from policy_bazaar_utils.profile_manager import ProfileManager


# ─── Constants ────────────────────────────────────────────────────────────────
CAR_NAME                = "NEXON_RETRY"
SEEN_RESPONSES          = set()
POLICY_EXPIRY           = "Policy not expired yet"
CLAIM_STATUS            = "Not Sure"
BASE_MOTOR_URL          = "https://www.policybazaar.com/motor-insurance/"
MAX_RETRIES             = int(os.getenv("PB_MAX_RETRIES", 4))
PHASE3_MAX_RETRIES      = int(os.getenv("PB_PHASE3_MAX_RETRIES", 3))
NCB_PERCENTAGE          = int(os.getenv("PB_NCB_PERCENTAGE", 20))
IDV_MAX_RETRIES         = int(os.getenv("PB_IDV_MAX_RETRIES", 3))
MOTOR_INSURANCE_PAGE_MAX_WAIT_MS = int(os.getenv("PB_MOTOR_INSURANCE_PAGE_MAX_WAIT_MS", 20000))
QUOTES_NAV_TIMEOUT_MS   = int(os.getenv("PB_QUOTES_NAV_TIMEOUT_MS", 40000))
POLICY_EXPIRY_POPUP_TIMEOUT_MS = int(os.getenv("PB_POLICY_EXPIRY_POPUP_TIMEOUT_MS", 60000))
# ── Smart final wait tunables ─────────────────────────────────────────────────
FINAL_MAX_WAIT_MS = int(os.getenv("PB_FINAL_MAX_WAIT_MS", 15000))
FINAL_IDLE_MS     = int(os.getenv("PB_FINAL_IDLE_MS", 8000))
FINAL_POLL_MS     = int(os.getenv("PB_FINAL_POLL_MS", 1000))

USE_PROXY = os.getenv("PB_USE_PROXY", "True").strip().lower() in ("1", "true", "yes", "y")
HEADLESS_TYPE = os.getenv("PB_HEADLESS_TYPE", False)
TARGET_INSURER = "Royal Sundaram"

# ── Parallel API concurrency ──────────────────────────────────────────────────
PARALLEL_REQUESTS = int(os.getenv("PB_PARALLEL_REQUESTS", 5))


# ─── Async-safe response handler factory ──────────────────────────────────────


def _make_live_handler(dbsync: LiveDBSync, idv_state: dict):
    def handler(response):
        asyncio.ensure_future(
            dbsync.live_handle_response(
                response,
                idv_state["type"],
                idv_state["value"],
            )
        )
    return handler


def extract_insurer_from_coverage(coverage_data: list, target_insurer: str = TARGET_INSURER) -> list:
    """Filter coverage data to plans matching the target insurer."""
    if not coverage_data:
        return []
    target = (target_insurer or "").strip().casefold()
    filtered = []
    for plan in coverage_data:
        if not plan:
            continue
        insurer = str(plan.get("insurer") or "").strip().casefold()
        if insurer == target:
            filtered.append(plan)
    return filtered


def get_plan_count(coverage_data: Optional[list]) -> int:
    """Return count of successful coverage results."""
    if not coverage_data:
        return 0
    return sum(1 for d in coverage_data if isinstance(d, dict) and d.get("success"))


# ─── Orchestrator-only helpers ────────────────────────────────────────────────


async def _set_idv_with_retry(
    page,
    action: str,
    label: str,
    fatal: bool = True,
) -> tuple[int, int]:
    last_err = None
    for idv_attempt in range(1, IDV_MAX_RETRIES + 1):
        try:
            default_idv, median_idv = await set_idv_to_median(page, action=action)
            print(
                f"[{label}] IDV '{action}' succeeded "
                f"(attempt {idv_attempt}/{IDV_MAX_RETRIES}) → "
                f"default={default_idv} | median={median_idv}"
            )
            return default_idv, median_idv
        except Exception as e:
            last_err = e
            print(
                f"[{label}] IDV '{action}' attempt "
                f"{idv_attempt}/{IDV_MAX_RETRIES} failed: {str(e)[:120]}"
            )
            if idv_attempt < IDV_MAX_RETRIES:
                print(f"[{label}] Retrying IDV selection...")
                await human_delay(2000, 3000)

    if fatal:
        print(f"[{label}] IDV selection failed after {IDV_MAX_RETRIES} attempts — raising.")
        raise last_err
    else:
        print(f"[{label}] IDV selection failed after {IDV_MAX_RETRIES} attempts — continuing (non-fatal).")
        return None, None


async def _apply_ncb_with_retry(
    page,
    ncb_percentage: int,
    previous_claim: str,
    label: str,
) -> None:
    last_err = None
    for ncb_attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[{label}] Applying NCB={ncb_percentage}% (attempt {ncb_attempt}/{MAX_RETRIES})")
            await apply_ncb(page, ncb_percentage=ncb_percentage, previous_claim=previous_claim)
            print(f"[{label}] NCB={ncb_percentage}% applied successfully")
            return
        except Exception as e:
            last_err = e
            print(f"[{label}] Attempt {ncb_attempt}/{MAX_RETRIES} failed: {str(e)[:150]}")
            if ncb_attempt < MAX_RETRIES:
                await human_delay(2000, 3000)

    raise last_err


async def _smart_final_wait(data_tracker: DataUsageTracker) -> None:
    print(
        f"[FINAL WAIT] Waiting for delayed responses "
        f"(max {FINAL_MAX_WAIT_MS // 1000}s, "
        f"exits after {FINAL_IDLE_MS // 1000}s idle)..."
    )

    elapsed_ms = 0
    idle_ms    = 0
    last_count = data_tracker._total_count

    while elapsed_ms < FINAL_MAX_WAIT_MS:
        await asyncio.sleep(FINAL_POLL_MS / 1000)
        elapsed_ms += FINAL_POLL_MS

        current_count = data_tracker._total_count
        if current_count != last_count:
            print(
                f"[FINAL WAIT] New response(s) detected "
                f"(+{current_count - last_count}) at {elapsed_ms // 1000}s — resetting idle timer."
            )
            last_count = current_count
            idle_ms    = 0
        else:
            idle_ms += FINAL_POLL_MS
            if idle_ms >= FINAL_IDLE_MS:
                print(
                    f"[FINAL WAIT] No new responses for {FINAL_IDLE_MS // 1000}s "
                    f"— exiting early at {elapsed_ms // 1000}s."
                )
                return

    print(f"[FINAL WAIT] Hard cap of {FINAL_MAX_WAIT_MS // 1000}s reached — proceeding.")


# ─── Phase 2: Policy / claim / addons selection ───────────────────────────────


async def phase2_quotes_setup(
    page,
    policy_expiry: str,
    claim_status: str,
    quotes_url: Optional[str],
    log,
    dbsync,
) -> tuple[str, int, int]:
    captured_url = quotes_url or page.url
    max_policy_claim_attempts = MAX_RETRIES

    default_idv, median_idv = None, None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"\n[PHASE 2] Attempt {attempt}/{MAX_RETRIES} – Policy/claim/addons setup")

            popups_need_handling = True
            for attempt_policy in range(1, max_policy_claim_attempts + 1):
                try:
                    if not popups_need_handling:
                        break
                    print(f"[PHASE 2] Policy/claim attempt {attempt_policy}/{max_policy_claim_attempts}")
                    try:
                        await page.wait_for_load_state("domcontentloaded", timeout=20000)
                    except Exception:
                        pass
                    await human_delay(3000, 5000)

                    log.step_start("STEP_10_POLICY_EXPIRY", "Select policy expiry")
                    dbsync.push_step_start(log)
                    expiry_answered = await step_10_select_policy_expiry(page, policy_expiry)
                    if expiry_answered:
                        log.step_success("STEP_10_POLICY_EXPIRY")
                    else:
                        log.step_skipped("STEP_10_POLICY_EXPIRY", reason="popup not present on page")
                    dbsync.push_latest_step(log)

                    await human_delay(3000, 5000)

                    log.step_start("STEP_11_CLAIM_STATUS", "Select claim status")
                    dbsync.push_step_start(log)
                    claim_answered = await step_11_select_claim_status(page, claim_status)
                    if claim_answered:
                        log.step_success("STEP_11_CLAIM_STATUS")
                    else:
                        log.step_skipped("STEP_11_CLAIM_STATUS", reason="popup not present on page")
                    dbsync.push_latest_step(log)

                    await step_12_handle_car_question_popup(page)
                    popups_need_handling = False

                except Exception as e:
                    print(f"[PHASE 2] Policy/claim attempt {attempt_policy} failed: {e}")
                    if attempt_policy >= max_policy_claim_attempts:
                        print("[PHASE 2] Policy/claim retries exhausted – escalating to outer retry.")
                        raise

            live_url = page.url
            if not captured_url or not detect_quotes_page_indicators(captured_url):
                print("[PHASE 2] Quotes page indicators not detected in URL.")
                await human_delay(2000, 4000)
                captured_url = live_url
            print(f"[PHASE 2] Quotes URL captured: {captured_url[:80]}...")

            await human_delay(6000, 7000)

            print(f"[PHASE 2] ✓ Completed on attempt {attempt}")
            return captured_url, default_idv, median_idv

        except Exception as e:
            print(f"[PHASE 2] Attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES and captured_url:
                print("[PHASE 2] Re-navigating to quotes URL and retrying...")
                await navigate_to_quotes(page, captured_url, "PHASE 2")
            elif attempt < MAX_RETRIES:
                print("[PHASE 2] No quotes URL captured; retrying on current page...")
                await human_delay(3000, 5000)
            else:
                print("[PHASE 2] All retries exhausted – aborting run.")
                raise


# ─── Phase 3A: Default IDV scrape ─────────────────────────────────────────────


async def phase3a_scrape_default(page, quotes_response: dict, log, dbsync):
    """
    API-first default scrape:
    - Extracts plan cards (planId / addonComboId) from the captured quotes response
    - Fires parallel coverage APIs via fetch_all_coverages_parallel (no View Coverage clicks)
    The in-page fetches still fire page.on('response'), so LiveDBSync persists them to DB.
    """
    for attempt in range(1, PHASE3_MAX_RETRIES + 1):
        try:
            print(f"\n[PHASE 3A – DEFAULT] Attempt {attempt}/{PHASE3_MAX_RETRIES}")

            plan_cards = extract_plan_cards_from_quotes(quotes_response)
            plan_count = len(plan_cards or [])
            print(f"[PHASE 3A] Extracted {plan_count} plan card(s) from quotes.")

            if not plan_cards:
                raise Exception("No plan cards found in quotes response.")

            log.step_start("STEP_14_EXTRACT_COVERAGE_API", "Extract coverage via parallel APIs")
            dbsync.push_step_start(log)
            coverage_data = await fetch_all_coverages_parallel(
                page=page,
                plans=plan_cards,
                concurrency=PARALLEL_REQUESTS,
            )
            log.step_success("STEP_14_EXTRACT_COVERAGE_API")
            dbsync.push_latest_step(log)

            extracted_count = get_plan_count(coverage_data)
            print(f"[PHASE 3A] ✓ Extracted {extracted_count}/{plan_count} coverage plan(s) via API.")

            if not coverage_data or extracted_count == 0:
                raise Exception("No successful coverage data returned from parallel API calls.")

            return coverage_data

        except Exception as e:
            print(f"[PHASE 3A] Attempt {attempt} failed: {str(e)[:150]}")
            if attempt >= PHASE3_MAX_RETRIES:
                print("[PHASE 3A] All retries exhausted – default data unavailable.")
                return None
            await human_delay(2000, 3000)


# ─── Phase 3B: Median IDV scrape ──────────────────────────────────────────────


async def phase3b_scrape_median(page, quotes_response_median: dict, log, dbsync):
    """
    API-first median scrape:
    - Assumes median IDV has already been set by the caller and the median
      quotes response pulled from DB
    - Fires parallel coverage APIs (no View Coverage clicks)
    """
    for attempt in range(1, PHASE3_MAX_RETRIES + 1):
        try:
            print(f"\n[PHASE 3B – MEDIAN] Attempt {attempt}/{PHASE3_MAX_RETRIES}")

            plan_cards = extract_plan_cards_from_quotes(quotes_response_median)
            plan_count = len(plan_cards or [])
            print(f"[PHASE 3B] Extracted {plan_count} median plan card(s) from quotes.")

            if not plan_cards:
                raise Exception("No median plan cards found in quotes response.")

            log.step_start("STEP_19_EXTRACT_MEDIAN_COVERAGE_API", "Extract median coverage via parallel APIs")
            dbsync.push_step_start(log)
            coverage_data_median = await fetch_all_coverages_parallel(
                page=page,
                plans=plan_cards,
                concurrency=PARALLEL_REQUESTS,
            )
            log.step_success("STEP_19_EXTRACT_MEDIAN_COVERAGE_API")
            dbsync.push_latest_step(log)

            extracted_count = get_plan_count(coverage_data_median)
            print(f"[PHASE 3B] ✓ Extracted {extracted_count}/{plan_count} median coverage plan(s) via API.")

            if not coverage_data_median or extracted_count == 0:
                raise Exception("No successful median coverage data returned from parallel API calls.")

            return coverage_data_median

        except Exception as e:
            print(f"[PHASE 3B] Attempt {attempt} failed: {str(e)[:150]}")
            if attempt >= PHASE3_MAX_RETRIES:
                print("[PHASE 3B] All retries exhausted – median data unavailable.")
                return None
            await human_delay(2000, 3000)


# ─── Main orchestrator ────────────────────────────────────────────────────────


async def run(
    run_id,
    car_brand: Optional[str] = None,
    car_model: Optional[str] = None,
    fuel_type: Optional[str] = None,
    variant: Optional[str] = None,
    year: Optional[str] = None,
    rto_code: Optional[str] = None,
    car_name: Optional[str] = None,
    cust_name: Optional[str] = None,
    phone: Optional[str] = None,
    policy_expiry: Optional[str] = POLICY_EXPIRY,
    claim_status: Optional[str] = CLAIM_STATUS,
    quotes_url: Optional[str] = None,
    ncb_percent: Optional[str] = None,
    profile_unique_key: Optional[str] = None,
    profile_identifier_key: Optional[str] = None,
):
    if not run_id:
        run_id = str(uuid.uuid4())

    if not quotes_url:
        raise ValueError("quotes_url is required – Phase 1 has been removed from this locator.")

    ncb_percentage = int(ncb_percent) if ncb_percent is not None else NCB_PERCENTAGE
    if ncb_percentage == 0:
        claim_status = "Yes"

    log    = ScrapeLogger(None, run_id, car_brand)
    conn   = get_connection()
    dbsync = LiveDBSync(run_id, conn)

    create_scrape_run(
        conn,
        run_id=run_id,
        status="RUNNING",
        started_at=datetime.now(),
    )
    if cust_name is None or phone is None:
        customer  = await get_rotated_name_and_mobile()
        cust_name = cust_name or customer["name"]
        phone     = phone     or customer["phone"]
        print(f"[ROTATOR] Using – {cust_name} / {phone}")

    insert_scrape_input(conn, run_id, {
        "car_number":     car_brand or car_name or f"UNKNOWN-{run_id[:8]}",
        "policy_expiry":  policy_expiry,
        "claim_status":   claim_status,
        "phone":          phone,
        "customer_name":  cust_name,
    })

    log.step_start("TEST_STEP", "Logger test")
    dbsync.push_step_start(log)
    log.step_success("TEST_STEP")
    dbsync.push_latest_step(log)

    start_ts = datetime.now()

    datadir = f"policy_bazaar_responses/{car_name}_{car_brand}"
    os.makedirs(datadir, exist_ok=True)

    data_tracker = DataUsageTracker(label=f"{car_name}_{car_brand}")

    _proxy = get_rotated_proxy() if USE_PROXY else None
    _geoip = USE_PROXY
    print(f"[CONFIG] Proxy: {'enabled' if USE_PROXY else 'disabled (proxyless)'}")

    profile_dir = ProfileManager.acquire()
    print(f"[CONFIG] Browser profile: {profile_dir}")
    print(f"[CONFIG] profile_unique_key={profile_unique_key} | profile_identifier_key={profile_identifier_key}")

    _idv_state = {"type": "default", "value": None}
    _scrape_succeeded = False
    _result = None

    try:
        async with AsyncCamoufox(
            persistent_context=True,
            user_data_dir=str(profile_dir),
            os=random.choice(["windows", "macos"]),
            block_images=True,
            geoip=_geoip,
            proxy=_proxy,
            headless=HEADLESS_TYPE,
            humanize=True,
        ) as context:

            context.set_default_timeout(90_000)
            context.set_default_navigation_timeout(90_000)

            await asyncio.sleep(3)

            page = context.pages[0] if context.pages else await context.new_page()

            # ── Response listeners ────────────────────────────────────────────
            page.on(
                "response",
                lambda response: data_tracker.track(response),
            )
            page.on(
                "response",
                _make_live_handler(dbsync, _idv_state),
            )

            await setup_all_popup_handlers(page)

            # ── Navigate directly to quotes URL (Phase 1 removed) ─────────────
            data_tracker.set_phase("registration")
            print(f"\n[PHASE 1] Skipped – navigating directly to quotes URL.")
            log.step_start("STEP_1_NAVIGATE", "Navigate to quotes URL")
            dbsync.push_step_start(log)
            await step_1_navigate_to_motor_insurance(page, url=quotes_url)
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=10000)
            except Exception:
                pass
            await human_delay(3000, 5000)
            log.step_success("STEP_1_NAVIGATE")
            dbsync.push_latest_step(log)

            # ── PHASE 2: Policy / claim / addons ──────────────────────────────
            data_tracker.set_phase("setup")
            quotes_url, default_idv, _ = await phase2_quotes_setup(
                page, policy_expiry, claim_status, quotes_url, log, dbsync
            )

            # ── NCB SETUP ───────────────────────────────────────────────────
            data_tracker.set_phase("ncb")
            log.step_start("STEP_13_NCB_SETUP", f"Apply NCB={ncb_percentage}%")
            dbsync.push_step_start(log)
            if ncb_percentage == 0:
                print(f"\n[NCB] NCB=0% – using page default, skipping NCB changer")
                log.step_skipped("STEP_13_NCB_SETUP", reason="NCB=0% uses page default")
            else:
                try:
                    await _apply_ncb_with_retry(
                        page,
                        ncb_percentage=ncb_percentage,
                        previous_claim=claim_status,
                        label="NCB SETUP",
                    )
                    log.step_success("STEP_13_NCB_SETUP")
                except Exception as e:
                    log.step_fail("STEP_13_NCB_SETUP", error=e)
                    dbsync.push_latest_step(log)
                    raise
            dbsync.push_latest_step(log)
            await wait_for_page_ready(page, label="NCB SETUP")

            # ── ADDONS SETUP ──────────────────────────────────────────────────
            await setup_addons(page)
            print("[ADDONS] Addons applied")

            await wait_for_page_ready(page, label="POST-ADDONS")

            # PB sometimes does not fire the quotes API again after addons are
            # applied, so wait for delayed APIs to settle and pull the latest
            # default quotes response straight from DB (tagged idv_type='default').
            print("[DEFAULT SETUP] Waiting for quotes stabilization...")
            await human_delay(15000, 20000)
            quotes_response = get_latest_quotes_from_db(
                conn=conn,
                run_id=run_id,
                idv_type="default",
            )
            print("[DEFAULT SETUP] Latest default quotes response pulled from DB.")

            # ── PHASE 3A: Scrape with default IDV (API-first) ────────────────
            data_tracker.set_phase("default")
            coverage_data_default = await phase3a_scrape_default(page, quotes_response, log, dbsync)
            coverage_data_median = None
            actual_median = None

            if coverage_data_default:
                filtered_default = extract_insurer_from_coverage(coverage_data_default, TARGET_INSURER)
                if filtered_default:
                    print("Royal Sundaram found in quotes section, scraping all quotes.")
                else:
                    print("Could not find Royal Sundaram in quotes section, moving to Median Value.")
                print(
                    f"\n[RESULT – DEFAULT] {len(coverage_data_default)} plan(s) extracted via API "
                    f"({len(filtered_default)} match '{TARGET_INSURER}')."
                )
            else:
                print("\n[RESULT – DEFAULT] No coverage data extracted after retries.")

            # ── MEDIAN IDV SETUP ──────────────────────────────────────────────
            data_tracker.set_phase("median")
            print("\n[MEDIAN SETUP] Navigating and setting median IDV...")
            await navigate_to_quotes(page, quotes_url, "PHASE 3B – MEDIAN SETUP")

            log.step_start("STEP_18_SET_MEDIAN_IDV", "Set IDV to median")
            dbsync.push_step_start(log)
            default_idv, actual_median = await _set_idv_with_retry(
                page, action="set_median", label="PHASE 3B – MEDIAN", fatal=True
            )
            log.step_success("STEP_18_SET_MEDIAN_IDV")
            dbsync.push_latest_step(log)

            _idv_state["type"]  = "median"
            _idv_state["value"] = actual_median

            # Give LiveDBSync time to tag median responses, then re-apply addons.
            await human_delay(3000, 5000)
            await setup_addons(page)

            print("[MEDIAN SETUP] Waiting for median quotes stabilization...")
            await human_delay(15000, 20000)
            quotes_response_median = get_latest_quotes_from_db(
                conn=conn,
                run_id=run_id,
                idv_type="median",
            )
            print("[MEDIAN SETUP] Median quotes response pulled from DB.")

            # ── PHASE 3B: Scrape with median IDV (API-first) ─────────────────
            coverage_data_median = await phase3b_scrape_median(
                page, quotes_response_median, log, dbsync
            )

            if coverage_data_median:
                filtered_median = extract_insurer_from_coverage(coverage_data_median, TARGET_INSURER)
                if filtered_median:
                    print("Royal Sundaram found in Median Value, scraping data.")
                else:
                    print("Royal Sundaram insurer not found in Median Value as well.")
                print(
                    f"\n[RESULT – MEDIAN (IDV={actual_median})] {len(coverage_data_median)} plan(s) "
                    f"extracted via API ({len(filtered_median)} match '{TARGET_INSURER}')."
                )
            else:
                print("\n[RESULT – MEDIAN] No median coverage data extracted after retries.")

            # ── Smart final wait ──────────────────────────────────────────────
            data_tracker.set_phase("final")
            await _smart_final_wait(data_tracker)

            # ── Report + save ─────────────────────────────────────────────────
            data_tracker.report()
            usage_summary = data_tracker.summary()
            usage_path = pathlib.Path(datadir) / "data_usage.json"
            usage_path.write_text(json.dumps(usage_summary, indent=2))
            print(f"[TRACKER] Usage saved – {usage_path}")

            try:
                insert_data_usage(conn, run_id, usage_summary)
                print("[TRACKER] Usage pushed to DB ✓")
            except Exception as e:
                print(f"[TRACKER] Usage DB push failed (non-fatal): {e}")

            total_ms = int((datetime.now() - start_ts).total_seconds() * 1000)
            dbsync.finalize_run(run_id=run_id, status="SUCCESS", log=log, total_duration_ms=total_ms)
            _result = {
                "default": coverage_data_default,
                "median":  coverage_data_median,
                "idv": {
                    "default": default_idv,
                    "median":  actual_median,
                },
            }
            _scrape_succeeded = True

        conn.close()
        return _result

    except Exception as e:
        total_ms = int((datetime.now() - start_ts).total_seconds() * 1000)
        if _scrape_succeeded:
            print(f"[WARN] Browser cleanup error (non-fatal, run already SUCCESS): {e}")
            conn.close()
            return _result
        dbsync.finalize_run(run_id=run_id, status="FAILED", log=log, total_duration_ms=total_ms, notes=str(e))
        conn.close()
        raise


if __name__ == "__main__":
    _quotes_url = sys.argv[1] if len(sys.argv) > 1 else None
    if not _quotes_url:
        print("Usage: python cmf_locator_v6.py <quotes_url>")
        sys.exit(1)
    print(f"Starting CMF locator with quotes URL: {_quotes_url}")
    asyncio.run(
        run(
            run_id=None,
            car_name=CAR_NAME,
            quotes_url=_quotes_url,
        )
    )
