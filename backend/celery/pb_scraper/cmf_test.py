import asyncio
from datetime import datetime
from pathlib import Path
import sys
from typing import Optional
from camoufox.async_api import AsyncCamoufox
import sys
sys.path.append(Path(__file__).parent.parent.as_posix())
from db_and_logging import (
    ScrapeLogger,
    LiveDBSync,
    get_connection,
    create_scrape_run,
    insert_scrape_input,
)

from pb_scraper.pb_flow_utils import (
    PROXY_SETTINGS,
    human_delay,
    detect_quotes_page_indicators,
    close_all_common_popups,
    set_idv_to_median,
    setup_call_us_now_popup_handler,
    setup_handle_coverage_selection,
    setup_intent_modal_handler,
    step_13_0_click_addons_filter,
    step_13_1_open_addons_section,
    step_13_3_select_allowed_addons,
    step_13_4_apply_filters,
    step_13_5_open_new_mobile_tab,
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

CAR_NAME = "SWIFT_RERUN"
CAR_NUMBER = "TN11AH5278"
CUST_NAME = "Vignesh J"
PHONE = "8939726746"
SEEN_RESPONSES = set()
POLICY_EXPIRY = "Policy not expired yet"  # User provides this - NO ROTATION
CLAIM_STATUS = "Not Sure"  # User provides this - NO ROTATION
USER_DIR = 'home/vigneshjayabalan/.mozilla/firefox'
# constraints = Screen(max_width=420, max_height=932)

# QUOTES_URL = "https://ci.policybazaar.com/v2/quotes?id=UHMrUDZpa0lTeW4rRXIraWNMdDNVRVlwU25GTmtTcmxtV2ZqdEVNMllNWT0%3d&id2=UEZpczJaUzhGWGxLMURXTmYzZlJ6QT09&t=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbnF1aXJ5SWQiOiIxMDQyODI3MDEwIiwibmJmIjoxNzcyNzgxODg1LCJleHAiOjE3NzgwNTIyODUsImlhdCI6MTc3Mjc4MTg4NSwiaXNzIjoiQ2FyQ0oifQ.mLzqwIpQQFDBuRro99bbLFRSJPIPpY-5v__NfnTMPDU"

async def run(
    run_id,
    car_number,
    cust_name : Optional[str] = CUST_NAME,
    phone : Optional[str] = PHONE,
    policy_expiry: Optional[str] = POLICY_EXPIRY,
    claim_status: Optional[str] = CLAIM_STATUS,
    url : Optional[str]= None,
    user_profile_dir: Optional[str] = USER_DIR,
):

    # No local file creation; all persistence happens in the DB
    log = ScrapeLogger(None, run_id, car_number)

    conn = get_connection()
    dbsync = LiveDBSync(run_id, conn)

    if type(phone) is not str:
        phone = str(phone)
        print(f"Converted phone to string: {phone}")

    try:
        create_scrape_run(conn, run_id=run_id, status="running", started_at=datetime.now())
        insert_scrape_input(conn, run_id, {
            "car_number": car_number,
            "policy_expiry": policy_expiry,
            "claim_status": claim_status,
            "phone": phone,
            "customer_name": cust_name,
        })
    except Exception:
        conn.close()
        raise

    # Mutable state so the response handler always uses the latest selected IDV
    _idv_state = {"type": "default", "value": None}

    def is_network_error(error: Exception) -> bool:
        msg = str(error).lower()
        return "timeout" in msg or "net::" in msg

    async def _run_step(
        step_key: str,
        description: str,
        coro,
        *args,
        post_success=None,
        error_type=None,
        **kwargs,
    ):
        log.step_start(step_key, description)
        try:
            result = await coro(*args, **kwargs)

            extra = {}
            if post_success:
                try:
                    extra = post_success(result) or {}
                except Exception:
                    extra = {}

            log.step_success(step_key, **extra)
            dbsync.push_latest_step(log)
            return result
        except Exception as e:
            err_type = error_type or ("NETWORK_ERROR" if is_network_error(e) else "STEP_ERROR")
            err_extra = {
                "error": str(e),
                "error_type": err_type,
            }
            if err_type == "NETWORK_ERROR":
                err_extra["proxy"] = PROXY_SETTINGS.get("server")
            log.step_error(step_key, **err_extra)
            dbsync.push_latest_step(log)
            raise

    start_ts = datetime.now()
    try:
        async with AsyncCamoufox(
            persistent_context=True,
            user_data_dir=user_profile_dir,
            os="windows",
            block_images=True,
            enable_cache=True,
            geoip=True,
            proxy=PROXY_SETTINGS,
            headless=False,
            humanize=True,
        ) as context:
            page = context.pages[0] if context.pages else await context.new_page()
            page.on(
                "response",
                lambda response: dbsync.live_handle_response(
                    response, _idv_state["type"], _idv_state["value"]
                ),
            )

            # STEP 0 — Initial page load
            await _run_step(
                "STEP_0_INIT_LOAD",
                "Initial page DOM load wait",
                page.wait_for_load_state,
                "domcontentloaded",
                timeout=15000,
            )

            await setup_intent_modal_handler(page)
            await setup_call_us_now_popup_handler(page)
            await setup_handle_coverage_selection(page)

            # STEPS 1-7: Navigation & Car Registration
            await _run_step(
                "STEP_1_NAVIGATE",
                "Navigate to motor insurance / quotes URL",
                step_1_navigate_to_motor_insurance,
                page,
                url=url,
            )

            if not url:
                await human_delay(500, 800)
                print("TAB 1: Setting up handlers for popups...")

                car_input = await _run_step(
                    "STEP_3_LOCATE_CAR_INPUT",
                    "Locate car registration number input",
                    step_3_locate_car_input,
                    page,
                )
                await human_delay(300, 600)

                await _run_step(
                    "STEP_4_FOCUS_CAR_INPUT",
                    "Focus car registration number input",
                    step_4_focus_car_input,
                    car_input,
                )

                await _run_step(
                    "STEP_5_TYPE_CAR_NUMBER",
                    "Type car registration number",
                    step_5_type_car_number,
                    car_input,
                    car_number,
                )

                view_prices_btn = await _run_step(
                    "STEP_6_LOCATE_VIEW_PRICES",
                    "Locate View Prices button",
                    step_6_locate_view_prices_button,
                    page,
                )

                await _run_step(
                    "STEP_7_CLICK_VIEW_PRICES",
                    "Click View Prices button",
                    step_7_click_view_prices,
                    page,
                    view_prices_btn,
                )

                await human_delay(300, 600)

                is_on_quotes_page = detect_quotes_page_indicators(page.url)

                if not is_on_quotes_page:
                    try:
                        await _run_step(
                            "STEP_8_WAIT_CUSTOMER_FORM",
                            "Wait for customer details form",
                            step_8_wait_for_customer_form,
                            page,
                        )

                        await _run_step(
                            "STEP_9_FILL_CUSTOMER_DETAILS",
                            "Fill customer name and phone",
                            step_9_fill_customer_details,
                            page,
                            cust_name,
                            phone,
                        )
                        print("Customer details successfully filled")
                    except Exception as e:
                        print(f"Customer form not found, may already be on quotes page: {e}")
                        print("Proceeding to next steps...")
                else:
                    print(" Skipping customer details steps - already on quotes page")
                    await human_delay(1000, 2000)

            # STEPS 10-11: Policy & Claim Selection (disabled)
            await human_delay(3000, 5000)
            await _run_step(
                "STEP_10_POLICY_EXPIRY",
                "Select policy expiry status",
                step_10_select_policy_expiry,
                page,
                policy_expiry,
            )
            await human_delay(3000, 5000)
            await _run_step(
                "STEP_11_CLAIM_STATUS",
                "Select claim status",
                step_11_select_claim_status,
                page,
                claim_status,
            )
            await human_delay(3000, 5000)   
            await _run_step(
                "STEP_12_CAR_QUESTION_POPUP",
                "Handle 'Car Question' popup if it appears",
                step_12_handle_car_question_popup,
                page,
            )
            await human_delay(3000, 5000)

            # STEP 12: Car Question Popup (disabled)
            await step_12_handle_car_question_popup(page)

            quotes_page_url = page.url
            print(f"\n Quotes Page URL captured: {quotes_page_url[:80]}...")

            # STEP 13.5: Open Independent Mobile Tab IMMEDIATELY AFTER STEP 12
            print("\n" + "=" * 70)
            print(" OPENING INDEPENDENT MOBILE TAB (STEP 13.5)")
            print("=" * 70)

            default, median = await _run_step(
                "STEP_13_IDV_GET_DEFAULT",
                "Read default and median IDV values",
                set_idv_to_median,
                page,
                action="get_default",
                post_success=lambda result: {"default_idv": result[0], "median_idv": result[1]} if isinstance(result, tuple) else {},
            )

            print(f"Default IDV value detected: {default}")
            print(f"Median IDV value detected: {median}")

            print("try passed, proceeding to addon selection...")

            await _run_step(
                "STEP_14_EXPAND_PLANS",
                "Expand grouped plans",
                expand_grouped_plans_show_btn,
                page,
            )
            await human_delay(1000, 2000)

            try:
                await _run_step(
                    "STEP_15_ADDONS_FILTER",
                    "Click addons filter",
                    step_13_0_click_addons_filter,
                    page,
                )
                await human_delay(1000, 2000)

                print("\nStep 13.3: Selecting allowed addons")
                await _run_step(
                    "STEP_16_SELECT_ADDONS",
                    "Select allowed addons",
                    step_13_3_select_allowed_addons,
                    page,
                )
                await human_delay(1000, 2000)

            except Exception as e:
                error_msg = str(e)[:150]
                print(f"\n  Error in addon selection flow: {error_msg}")
                print(f"  Will skip addon selection and proceed to coverage extraction")

            # WAIT FOR QUOTES TO LOAD
            print("\n" + "=" * 70)
            print(" WAITING FOR QUOTES TO LOAD")
            print("=" * 70)
            try:
                print("   Waiting for quotes page to fully load...")
                await page.wait_for_load_state("domcontentloaded", timeout=15000)
                print("   Quotes page loaded (domcontentloaded)")
            except Exception as e:
                print(f"   DOM load timeout: {e}")

            # Wait for content to stabilize
            print("   Waiting for content to stabilize...")
            await human_delay(3000, 5000)

            # Debug: Check page state before extraction
            page_title = await page.title()
            page_url = page.url
            print(f" Page Title: {page_title}")
            print(f" Page URL: {page_url[:100]}...")

            # Wait a bit more for any lazy-loaded content
            await human_delay(2000, 3000)
            coverage_data = await _run_step(
                "STEP_17_EXTRACT_COVERAGE",
                "Extract coverage and plan details",
                step_14_extract_plan_details_and_coverage,
                page,
            )

            if coverage_data:
                print(" Coverage data extracted successfully (per plan):")
                for i, data in enumerate(coverage_data, start=1):
                    if data:
                        print(f"   [{i}] Plan ID: {data.get('plan_id')} | Insurer: {data.get('insurer')}")
                    else:
                        print(f"   [{i}] No data extracted for this plan")
            else:
                print(" No coverage data extracted")

            await human_delay(5000, 2000)

            default, median = await _run_step(
                "STEP_18_IDV_SET_MEDIAN",
                "Set IDV to median value",
                set_idv_to_median,
                page,
                action="set_median",
                post_success=lambda result: {"median_idv": result[1]} if isinstance(result, tuple) else {},
            )
            _idv_state["type"] = "median"
            _idv_state["value"] = median

            await human_delay(10000, 120000)

            await _run_step(
                "STEP_19_EXTRACT_COVERAGE_MEDIAN",
                "Extract coverage at median IDV",
                step_14_extract_plan_details_and_coverage,
                page,
            )

            await human_delay(1000, 2000)

            # STEPS 15-16: Final wait
            await _run_step(
                "STEP_20_FINAL_WAIT",
                "Final wait for delayed responses",
                step_16_final_wait,
                page,
                wait_duration=60000,
            )

    except Exception as e:
        total_ms = int((datetime.now() - start_ts).total_seconds() * 1000)
        try:
            dbsync.finalize_run("FAILED", total_duration_ms=total_ms, notes=str(e))
        except Exception:
            pass
        raise
    else:
        total_ms = int((datetime.now() - start_ts).total_seconds() * 1000)
        dbsync.finalize_run("SUCCESS", total_duration_ms=total_ms)
    finally:
        conn.close()


if __name__ == "__main__":
    QUOTES_URL = None
    if len(sys.argv) > 1:
        QUOTES_URL = sys.argv[1]
    asyncio.run(run(run_id="b227bfdd-f401-49c0-a688-fbc1efff0cbc", car_number=CAR_NUMBER, cust_name=CUST_NAME, phone=PHONE, policy_expiry=POLICY_EXPIRY, claim_status=CLAIM_STATUS, url=QUOTES_URL))
