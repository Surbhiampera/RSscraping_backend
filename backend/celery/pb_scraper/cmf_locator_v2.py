import asyncio
import json
import os
import pathlib
import sys
from datetime import datetime
from typing import Optional
# from camoufox.async_api import 
from camoufox.async_api import AsyncCamoufox
import uuid
from pathlib import Path
sys.path.append(Path(__file__).parent.parent.as_posix())


from db_and_logging import (
    ScrapeLogger,
    LiveDBSync,
    get_connection,
    create_scrape_run,
    insert_scrape_input,
    insert_data_usage,
)
from policy_bazaar_utils.pb_utils import (
    PROXY_SETTINGS,
    handle_response,
    human_delay,
)
from policy_bazaar_utils.pb_flow_utils import (
    ALLOWED_ADDONS,
    get_rotated_name_and_mobile,
    capture_latest_quotes_payload,
    setup_all_popup_handlers,
    detect_quotes_page_indicators,
    set_idv_to_median,
    step_13_0_click_addons_filter,
    step_13_3_select_allowed_addons,
    step_14_extract_plan_details_and_coverage,
    step_16_final_wait,
    step_1_navigate_to_motor_insurance,
    step_3_locate_car_input,
    step_4_focus_car_input,
    step_5_type_car_number,
    step_6_locate_view_prices_button,
    expand_grouped_plans_show_btn,
    step_7_click_view_prices,
    step_10_select_policy_expiry,
    step_11_select_claim_status,
    step_12_handle_car_question_popup,
    step_8_wait_for_customer_form,
    step_9_fill_customer_details,
)
from policy_bazaar_utils.pb_data_tracker import DataUsageTracker


# ─── Constants ────────────────────────────────────────────────────────────────
CAR_NAME           = "NEXON_RETRY"
CAR_NUMBER         = "MH04KW1827"
SEEN_RESPONSES     = set()
POLICY_EXPIRY      = "Policy not expired yet"
CLAIM_STATUS       = "Not Sure"
USER_DIR           = "home/vigneshjayabalan/.mozilla/firefox"
BASE_MOTOR_URL     = "https://www.policybazaar.com/motor-insurance/"
MAX_RETRIES        = 2  # Phase 1 & 2
PHASE3_MAX_RETRIES = 3  # Phase 3A & 3B — more attempts for data extraction

USE_PROXY          = True   # ← Toggle: True = use proxy | False = proxyless


# ─── Phase helpers ────────────────────────────────────────────────────────────


async def _setup_addons(page) -> None:
    """Re-run addon filter selection (used after every quotes URL re-navigation)."""
    try:
        await expand_grouped_plans_show_btn(page)
        await human_delay(1000, 2000)
        await step_13_0_click_addons_filter(page)
        await human_delay(1000, 2000)
        await step_13_3_select_allowed_addons(page)
        await human_delay(1000, 2000)
        print("[ADDONS] Addon filters applied.")
    except Exception as e:
        print(f"[ADDONS] Addon setup skipped (non-fatal): {str(e)[:120]}")


async def _expand_all_plans(page, label: str = "EXPAND") -> None:
    try:
        total = await expand_grouped_plans_show_btn(page)
        print(f"[{label}] Expanded {total} plan group button(s).")
    except Exception as e:
        print(f"[{label}] Plan expansion skipped (non-fatal): {str(e)[:120]}")


async def _navigate_to_quotes(page, quotes_url: str, label: str) -> None:
    """Navigate to the captured quotes URL and wait for stabilisation."""
    print(f"[{label}] Re-navigating to quotes URL...")
    await step_1_navigate_to_motor_insurance(page, url=quotes_url)
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=20000)
    except Exception:
        pass
    await human_delay(4000, 6000)


# ─── Phase 1: Car registration + customer details ─────────────────────────────


async def phase1_registration(page, car_number: str, cust_name: str, phone: str, log, dbsync) -> None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"\n[PHASE 1] Attempt {attempt}/{MAX_RETRIES} – Car registration & customer details")

            log.step_start("STEP_1_NAVIGATE", "Navigate to motor insurance page")
            await step_1_navigate_to_motor_insurance(page, url=BASE_MOTOR_URL)
            log.step_success("STEP_1_NAVIGATE")
            dbsync.push_latest_step(log)

            await human_delay(200, 400)

            log.step_start("STEP_3_LOCATE_CAR_INPUT", "Locate car input field")
            car_input = await step_3_locate_car_input(page)
            log.step_success("STEP_3_LOCATE_CAR_INPUT")
            dbsync.push_latest_step(log)

            await human_delay(100, 200)
            await step_4_focus_car_input(car_input)

            log.step_start("STEP_5_TYPE_CAR", "Typing car registration")
            await step_5_type_car_number(car_input, car_number)
            log.step_success("STEP_5_TYPE_CAR")
            dbsync.push_latest_step(log)

            view_prices_btn = await step_6_locate_view_prices_button(page)
            await step_7_click_view_prices(page, view_prices_btn)
            await human_delay(100, 200)

            if not detect_quotes_page_indicators(page.url):
                await step_8_wait_for_customer_form(page)
                await step_9_fill_customer_details(page, cust_name, phone)
                print("[PHASE 1] Customer details filled successfully.")
            else:
                print("[PHASE 1] Already on quotes page – skipping customer form.")

            print(f"[PHASE 1] ✓ Completed on attempt {attempt}")
            return

        except Exception as e:
            print(f"[PHASE 1] Attempt {attempt} failed: {str(e)[:150]}")
            if attempt < MAX_RETRIES:
                print(f"[PHASE 1] Redirecting to base URL and retrying...")
                await human_delay(2000, 4000)
            else:
                print("[PHASE 1] All retries exhausted – aborting run.")
                raise


# ─── Phase 2: Policy / claim / addons selection ───────────────────────────────


async def phase2_quotes_setup(
    page,
    policy_expiry: str,
    claim_status: str,
    quotes_url: Optional[str],
    log,
    dbsync,
) -> tuple[str, int, int]:
    captured_url = quotes_url

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"\n[PHASE 2] Attempt {attempt}/{MAX_RETRIES} – Policy/claim/addons setup")

            await human_delay(3000, 5000)
            await human_delay(3000, 5000)

            log.step_start("STEP_10_POLICY_EXPIRY", "Select policy expiry")
            await step_10_select_policy_expiry(page, policy_expiry)
            log.step_success("STEP_10_POLICY_EXPIRY")
            dbsync.push_latest_step(log)

            await human_delay(3000, 5000)

            log.step_start("STEP_11_CLAIM_STATUS", "Select claim status")
            await step_11_select_claim_status(page, claim_status)
            log.step_success("STEP_11_CLAIM_STATUS")
            dbsync.push_latest_step(log)

            await step_12_handle_car_question_popup(page)

            if not captured_url or not detect_quotes_page_indicators(captured_url):
                captured_url = page.url
                print(f"[PHASE 2] Quotes URL captured: {captured_url[:80]}...")

            await human_delay(6000, 7000)
            default_idv, median_idv = await set_idv_to_median(page, action="get_default")
            print(f"[PHASE 2] Default IDV: {default_idv} | Median IDV: {median_idv}")

            await _setup_addons(page)

            print(f"[PHASE 2] ✓ Completed on attempt {attempt}")
            return captured_url, default_idv, median_idv

        except Exception as e:
            print(f"[PHASE 2] Attempt {attempt} failed: {str(e)[:150]}")
            if attempt < MAX_RETRIES and captured_url:
                print(f"[PHASE 2] Re-navigating to quotes URL and retrying...")
                await _navigate_to_quotes(page, captured_url, "PHASE 2")
            elif attempt < MAX_RETRIES:
                print("[PHASE 2] No quotes URL captured; retrying on current page...")
                await human_delay(3000, 5000)
            else:
                print("[PHASE 2] All retries exhausted – aborting run.")
                raise


# ─── Phase 3A: Default IDV scrape ─────────────────────────────────────────────


async def phase3a_scrape_default(page, quotes_url: str, log, dbsync):
    for attempt in range(1, PHASE3_MAX_RETRIES + 1):
        try:
            print(f"\n[PHASE 3A – DEFAULT] Attempt {attempt}/{PHASE3_MAX_RETRIES}")

            # ✅ Every retry: re-navigate + re-apply addons from scratch
            if attempt > 1:
                print("[PHASE 3A] Re-navigating and re-applying addons for retry...")
                await _navigate_to_quotes(page, quotes_url, "PHASE 3A")
                await _setup_addons(page)

            try:
                await page.wait_for_load_state("domcontentloaded", timeout=15000)
            except Exception:
                pass
            await human_delay(3000, 5000)

            await _expand_all_plans(page, label="PHASE 3A")
            await human_delay(1500, 2500)

            log.step_start("STEP_14_EXTRACT_COVERAGE", "Extract plan coverage")
            coverage_data = await step_14_extract_plan_details_and_coverage(page)
            log.step_success("STEP_14_EXTRACT_COVERAGE")
            dbsync.push_latest_step(log)

            if not coverage_data:
                raise Exception(f"step_14 returned 0 plans on attempt {attempt}")

            print(f"[PHASE 3A] ✓ Extracted {len(coverage_data)} plan(s) on attempt {attempt}")
            return coverage_data

        except Exception as e:
            print(f"[PHASE 3A] Attempt {attempt} failed: {str(e)[:150]}")
            if attempt >= PHASE3_MAX_RETRIES:
                print("[PHASE 3A] All retries exhausted – default data unavailable.")
                return None
            await human_delay(2000, 3000)


# ─── Phase 3B: Median IDV scrape ──────────────────────────────────────────────


async def phase3b_scrape_median(page, quotes_url: str, _idv_state: dict = None, log=None, dbsync=None):
    median_idv_used = None

    for attempt in range(1, PHASE3_MAX_RETRIES + 1):
        try:
            print(f"\n[PHASE 3B – MEDIAN] Attempt {attempt}/{PHASE3_MAX_RETRIES}")

            # ✅ Every retry: re-navigate + re-apply addons + set median IDV fresh
            if attempt > 1:
                print("[PHASE 3B] Re-navigating and re-applying addons for retry...")
                await _navigate_to_quotes(page, quotes_url, "PHASE 3B")
                await _setup_addons(page)
                await human_delay(3000, 5000)

            print("[PHASE 3B] Setting IDV to median...")

            log.step_start("STEP_18_SET_MEDIAN_IDV", "Set IDV to median")
            default_idv, median_idv = await set_idv_to_median(page, action="set_median")
            log.step_success("STEP_18_SET_MEDIAN_IDV")
            dbsync.push_latest_step(log)

            median_idv_used = median_idv
            print(f"[PHASE 3B] IDV set to median: {median_idv}")

            if _idv_state is not None:
                _idv_state["type"] = "median"
                _idv_state["value"] = median_idv

            await human_delay(10000, 120000)

            await _expand_all_plans(page, label="PHASE 3B – POST IDV")
            await human_delay(2000, 3000)

            coverage_data_median = await step_14_extract_plan_details_and_coverage(page)

            if not coverage_data_median:
                raise Exception(f"step_14 returned 0 median plans on attempt {attempt}")

            print(f"[PHASE 3B] ✓ Extracted {len(coverage_data_median)} median plan(s) on attempt {attempt}")
            return coverage_data_median, median_idv_used

        except Exception as e:
            print(f"[PHASE 3B] Attempt {attempt} failed: {str(e)[:150]}")
            if attempt >= PHASE3_MAX_RETRIES:
                print("[PHASE 3B] All retries exhausted – median data unavailable.")
                return None, median_idv_used
            await human_delay(2000, 3000)


# ─── Main orchestrator ────────────────────────────────────────────────────────


async def run(
    run_id,
    car_name,
    car_number,
    cust_name=None,
    phone=None,
    policy_expiry: Optional[str] = POLICY_EXPIRY,
    claim_status: Optional[str] = CLAIM_STATUS,
    QUOTES_URL: Optional[str] = None,
):
    if not run_id:
        run_id = str(uuid.uuid4())

    # ── Step 4: Initialize DB logging ─────────────────────────────────────────
    log = ScrapeLogger(None, run_id, car_number)

    conn   = get_connection()
    dbsync = LiveDBSync(run_id, conn)

    # ✅ create_scrape_run FIRST — inserts run_id into scrape_runs (parent row)
    create_scrape_run(
        conn,
        run_id=run_id,
        status="running",
        started_at=datetime.now(),
    )

    insert_scrape_input(conn, run_id, {
        "car_number":     car_number,
        "policy_expiry":  policy_expiry,
        "claim_status":   claim_status,
        "phone":          phone,
        "customer_name":  cust_name,
    })

    # ✅ TEST_STEP after inserts — parent row is visible, FK satisfied
    log.step_start("TEST_STEP", "Logger test")
    log.step_success("TEST_STEP")
    dbsync.push_latest_step(log)

    start_ts = datetime.now()

    # ── Local setup ───────────────────────────────────────────────────────────
    datadir = f"policy_bazaar_responses/{car_name}_{car_number}"
    os.makedirs(datadir, exist_ok=True)

    data_tracker = DataUsageTracker(label=f"{car_name}_{car_number}")

    if cust_name is None or phone is None:
        customer  = await get_rotated_name_and_mobile()
        cust_name = cust_name or customer["name"]
        phone     = phone     or customer["phone"]
        print(f"[ROTATOR] Using → {cust_name} / {phone}")

    _proxy = PROXY_SETTINGS if USE_PROXY else None
    _geoip = USE_PROXY
    print(f"[CONFIG] Proxy: {'enabled' if USE_PROXY else 'disabled (proxyless)'}")

    # ── Step 5: IDV state tracker ──────────────────────────────────────────────
    _idv_state = {"type": "default", "value": None}

    try:
        async with AsyncCamoufox(
            persistent_context=True,
            user_data_dir=USER_DIR,
            os="windows",
            block_images=True,
            enable_cache=True,
            geoip=_geoip,
            proxy=_proxy,
            headless=False,
            humanize=True,
        ) as context:

            page = context.pages[0] if context.pages else await context.new_page()

            # ── Step 6: Response listeners ────────────────────────────────────
            page.on(
                "response",
                lambda response: handle_response(response, datadir, SEEN_RESPONSES),
            )
            page.on(
                "response",
                lambda response: data_tracker.track(response),
            )
            page.on(
                "response",
                lambda response: dbsync.live_handle_response(
                    response,
                    _idv_state["type"],
                    _idv_state["value"],
                ),
            )

            try:
                await page.wait_for_load_state("domcontentloaded", timeout=15000)
                print("Initial page loaded.")
            except Exception as e:
                print(f"Initial DOM load timeout (continuing): {e}")

            # await setup_all_popup_handlers(page)
            # await setup_all_popup_handlers(page)
            await setup_all_popup_handlers(page)

            # ── PHASE 1: Registration ─────────────────────────────────────────
            data_tracker.set_phase("registration")
            if QUOTES_URL:
                print("\n[PHASE 1] Skipped – quotes URL provided directly.")
                await step_1_navigate_to_motor_insurance(page, url=QUOTES_URL)
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=20000)
                except Exception:
                    pass
                await human_delay(3000, 5000)
            else:
                await phase1_registration(page, car_number, cust_name, phone, log, dbsync)

            # ── PHASE 2: Policy / claim / addons ──────────────────────────────
            data_tracker.set_phase("setup")
            quotes_url, default_idv, median_idv = await phase2_quotes_setup(
                page, policy_expiry, claim_status, QUOTES_URL, log, dbsync
            )

            # ── PHASE 3A: Scrape with default IDV ─────────────────────────────
            data_tracker.set_phase("default")
            coverage_data_default = await phase3a_scrape_default(page, quotes_url, log, dbsync)

            if coverage_data_default:
                print(f"\n[RESULT – DEFAULT] {len(coverage_data_default)} plan(s) extracted:")
                for i, d in enumerate(coverage_data_default, 1):
                    if d:
                        print(f"  [{i}] Plan ID: {d.get('plan_id')} | Insurer: {d.get('insurer')}")
            else:
                print("\n[RESULT – DEFAULT] No coverage data extracted after retries.")

            # ── PHASE 3B: Scrape with median IDV ──────────────────────────────
            data_tracker.set_phase("median")
            coverage_data_median, actual_median = await phase3b_scrape_median(
                page, quotes_url, _idv_state, log, dbsync
            )

            if coverage_data_median:
                print(f"\n[RESULT – MEDIAN (IDV={actual_median})] {len(coverage_data_median)} plan(s) extracted:")
                for i, d in enumerate(coverage_data_median, 1):
                    if d:
                        print(f"  [{i}] Plan ID: {d.get('plan_id')} | Insurer: {d.get('insurer')}")
            else:
                print("\n[RESULT – MEDIAN] No median coverage data extracted after retries.")

            # ── Final wait ────────────────────────────────────────────────────
            data_tracker.set_phase("final")
            await step_16_final_wait(page, wait_duration=60000)

            # ── Report + save ─────────────────────────────────────────────────
            data_tracker.report()
            usage_summary = data_tracker.summary()
            usage_path = pathlib.Path(datadir) / "data_usage.json"
            usage_path.write_text(json.dumps(usage_summary, indent=2))
            print(f"[TRACKER] Usage saved → {usage_path}")

            # ✅ Push data usage to DB
            try:
                insert_data_usage(conn, run_id, usage_summary)
                print(f"[TRACKER] Usage pushed to DB ✓")
            except Exception as e:
                print(f"[TRACKER] Usage DB push failed (non-fatal): {e}")

        # ── Step 8: Finalize run – SUCCESS ────────────────────────────────────
        total_ms = int((datetime.now() - start_ts).total_seconds() * 1000)
        dbsync.finalize_run("SUCCESS", total_duration_ms=total_ms)
        conn.close()

        return {
            "default": coverage_data_default,
            "median":  coverage_data_median,
            "idv": {
                "default": default_idv,
                "median":  actual_median,
            },
        }

    except Exception as e:
        # ── Step 8: Finalize run – FAILED ─────────────────────────────────────
        total_ms = int((datetime.now() - start_ts).total_seconds() * 1000)
        dbsync.finalize_run("FAILED", total_duration_ms=total_ms, notes=str(e))
        conn.close()
        raise


if __name__ == "__main__":
    _quotes_url = sys.argv[1] if len(sys.argv) > 1 else None
    asyncio.run(
        run(
            run_id=None,
            car_name=CAR_NAME,
            car_number=CAR_NUMBER,
            QUOTES_URL=_quotes_url,
        )
    )
