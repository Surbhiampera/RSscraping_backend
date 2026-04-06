import asyncio
import uuid
import sys
import os
from pathlib import Path
from typing import Optional
import random
from datetime import datetime
import json

# Setup paths FIRST
pb_scripts_path = str(Path(__file__).parent.parent / "policy_bazaar_scripts")
db_path = str(Path(__file__).parent)

if pb_scripts_path not in sys.path:
    sys.path.insert(0, pb_scripts_path)
if db_path not in sys.path:
    sys.path.insert(0, db_path)

# Now import from playwright and local modules
from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Playwright, Response
from playwright_stealth import Stealth

from pb_logger import ScrapeLogger
from db_live_sync import LiveDBSync
from db_v2 import (
    get_connection,
    create_scrape_run,
    insert_scrape_input,
)

from pb_utils import (  # type: ignore
    PROXY_SETTINGS,
    ANTI_DETECTION_JS,
    MOBILE_STEALTH_JS,
    bezier_mouse_move,
    fill_details_and_view_prices,
    handle_response,
    human_delay,
    random_human_noise,
    select_allowed_addons,
    select_claim_answer,
    select_expiry_answer,
    type_if_empty,
    wait_for_akamai_challenge,
    wait_for_intent_modal,
)

from data_rotator import (  # type: ignore
    DataRotator,
    data_rotator,
    MOBILE_DEVICE_MODELS,
    USER_AGENTS_MAP,
    MOBILE_NUMBERS,
    CUSTOMER_NAMES,
    CUSTOMER_DETAILS,
    get_rotated_device,
    get_rotated_user_agent,
    get_rotated_mobile,
    get_rotated_customer_name,
    get_rotated_policy_expiry,
    get_rotated_claim_status,
    get_all_rotated_data,
)

# =========================
# NETWORK ERROR DETECTION
# =========================
def is_network_error(error: Exception) -> bool:
    msg = str(error).lower()
    return any(k in msg for k in [
        "internet_disconnected",
        "connection_reset",
        "connection_refused",
        "name_not_resolved",
        "dns",
        "timed out",
        "proxy",
        "tunnel",
        "socket",
    ])


# ============================================================================
# TEST DATA INITIALIZATION (will be set in run())
# ============================================================================
# test default values
CAR_NAME = "SWIFT"
# CAR_NUMBER = "MH49BB1307"
CAR_NUMBER = "mh12vz2302"
PHONE = "8248336398"
CUST_NAME = "Surbhi"
POLICY_EXPIRY = "Policy not expired yet"
CLAIM_STATUS = "Not Sure"
CURRENT_DEVICE = "iPhone 14"
CURRENT_USER_AGENT = None
SEEN_RESPONSES = set()


# ✅ Comprehensive list of allowed addon titles
ALLOWED_ADDONS = {
    # Popular Core Addons
    "Zero Depreciation",
    "Nil Depreciation",
    "24x7 Roadside Assistance",
    "Roadside Assistance",
    "Engine Protection Cover",
    "Engine Secure",
    "Consumables Cover",
    "Consumables",
    "Key & Lock Replacement",
    "Key Replacement",
    "Invoice Price Cover",
    "Return to Invoice",
    "RTI Cover",
    "Tyre Protector",
    "Tyre Secure",
    "Loss of Personal Belongings",
    "Personal Belongings Cover",
    "Daily Allowance",
    "Emergency Transport and Hotel Allowance",
    "NCB Protection",
    "No Claim Bonus Protection",
    "NCB Retention",
    "Depreciation Waiver",
    "Bumper to Bumper",
    "Hydrostatic Lock Cover",
    "Gearbox Protection",
    "Battery Protect",
    "Electrical Accessories Cover",
    "Non Electrical Accessories Cover",
    "PA Cover for Passenger",
    "Unnamed Passenger Cover",
    "Named Driver PA Cover",
    "Legal Liability to Paid Driver",
    "Legal Liability to Employee",
    "IMT 23",
    "IMT 28",
    "IMT 29",
    "Windshield Glass Cover",
    "Glass Protection Cover",
    "Road Tax Cover",
    "EMI Protection",
    "Loan Protector",
    "Secure Towing",
    "Roadside Towing Cover",
    "Extended Warranty",
    "Breakdown Assistance",
    "Motor Protect",
    "Accidental Hospitalization Cover",
    "Consumable Expenses Cover",
}



# ============================================================================
# ASYNC HELPER FUNCTIONS
# ============================================================================
async def refresh_test_data():
    """Refresh all test data by rotating values."""
    global PHONE, CUST_NAME, POLICY_EXPIRY, CLAIM_STATUS, CURRENT_DEVICE, CURRENT_USER_AGENT
    test_data = await get_all_rotated_data()
    PHONE = test_data["mobile_number"] or "8978675645"
    CUST_NAME = test_data["customer_name"] or "Vignesh"
    POLICY_EXPIRY = test_data["policy_expiry"] or "Policy not expired yet"
    CLAIM_STATUS = test_data["claim_status"] or "Not Sure"
    CURRENT_DEVICE = test_data["device_model"] or "iPhone 14"
    CURRENT_USER_AGENT = test_data["user_agent"]
    print(f"🔄 Test data rotated: Phone={PHONE}, Name={CUST_NAME}, Device={CURRENT_DEVICE}")
    return test_data



# ============================================================================
# POLICY BAZAAR AUTOMATION FLOW
# ============================================================================
# This script contains the main automation flow for PolicyBazaar motor insurance.
# Utility functions are separated into pb_utils.py
# ============================================================================
async def run(
    car_name,
    car_number,
    phone: Optional[str] = PHONE,
    cust_name: Optional[str] = CUST_NAME,
    policy_expiry: Optional[str] = POLICY_EXPIRY,
    claim_status: Optional[str] = CLAIM_STATUS,
):
    timestamp_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, timestamp_id))

    base_dir = f"policy_bazaar_runs/{run_id}_{car_number}"
    os.makedirs(base_dir, exist_ok=True)

    datadir = f"{base_dir}/api"
    os.makedirs(datadir, exist_ok=True)
    log = ScrapeLogger(base_dir, run_id, car_number)
    conn = get_connection()
    dbsync = LiveDBSync(run_id, conn)
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
    
   
    
    log.info("RUN_START", "Scraper started", car=car_number)

    try:
        async with Stealth().use_async(async_playwright()) as p:
            iphone = p.devices["iPhone 13"]

            browser = await p.chromium.launch(
                headless=False,
            )

            # context = await browser.new_context(
            #     **iphone,
            #     proxy=PROXY_SETTINGS,
            #     ignore_https_errors=True,
            #     locale="en-IN",
            #     timezone_id="Asia/Kolkata",
            #     service_workers="block",
            #     color_scheme=random.choice(["light", "dark"]),
            #     reduced_motion="no-preference",
            # )
            context = await browser.new_context(
                **iphone,
                ignore_https_errors=True,
                locale="en-IN",
                timezone_id="Asia/Kolkata",
                service_workers="block",
                color_scheme=random.choice(["light", "dark"]),
                reduced_motion="no-preference",
            )


            page = await context.new_page()

            await page.add_init_script(ANTI_DETECTION_JS)
            await page.add_init_script(MOBILE_STEALTH_JS)
            page.on("response", dbsync.live_handle_response)   # ✅ KEEP THIS
            

           

            # STEP 1 — Navigation (FATAL)
            log.step_start("STEP_1_NAVIGATION", "Navigating to motor insurance page")
            try:
                await page.goto(
                    "https://www.policybazaar.com/motor-insurance/",
                    wait_until="domcontentloaded",
                    timeout=60000,
                )
                await human_delay(1500, 3000)
                log.step_success("STEP_1_NAVIGATION", current_url=page.url)
                dbsync.push_latest_step(log)
            except Exception as e:
                if is_network_error(e):
                    log.step_error(
                        "STEP_1_NAVIGATION",
                        error=str(e),
                        error_type="NETWORK_ERROR",
                        proxy=PROXY_SETTINGS.get("server"),
                    )
                    dbsync.push_latest_step(log)
                else:
                    log.step_error(
                        "STEP_1_NAVIGATION",
                        error=str(e),
                        error_type="PAGE_LOAD_ERROR",
                        url=page.url,
                    )
                    dbsync.push_latest_step(log)
                raise


            await wait_for_akamai_challenge(page)
            log.info("AKAMAI", "Akamai check completed", url=page.url)
            dbsync.push_latest_akamai(log)

            try:
                await page.wait_for_load_state("domcontentloaded", timeout=15000)
            except Exception as e:
             log.info("NON_FATAL_IGNORED", error=str(e))

            await human_delay(1000, 2500)

            # STEP 2 — Human noise (NON-FATAL)
            log.step_start("STEP_2_HUMAN_NOISE", "Simulating human behaviour")
            await random_human_noise(page)
            log.step_success("STEP_2_HUMAN_NOISE", current_url=page.url)
            dbsync.push_latest_step(log)

            await page.wait_for_timeout(random.randint(10000, 12000))
            await wait_for_intent_modal(page)
            
            print("Step 3: Locating car registration number input field")
            log.step_start(
             "STEP_3_LOCATE_CAR_INPUT",
             "Locating car registration number input field"
            )
            section_text = None
            try:
                main_div = page.locator("div#step-one-pq-popup")
                car_input_div = main_div.locator("div.input_box")
                car_input = car_input_div.locator("input.carRegistrationNumber")
                await car_input.wait_for(state="visible")
                section_text = "Please Fill Your Details" 
                log.step_success(
                "STEP_3_LOCATE_CAR_INPUT",
                selector="input.carRegistrationNumber",
                url=page.url,
                section=section_text
            )
                dbsync.push_latest_step(log)

            except Exception as e:
                log.step_error(
                    "STEP_3_LOCATE_CAR_INPUT",
                    error=str(e),
                    section=section_text,
                    url=page.url
                )
                dbsync.push_latest_step(log)

            await human_delay(600, 1400)

            print("Step 4: Focusing on car registration number input")
            log.step_start(
                "STEP_4_FOCUS_CAR_INPUT",
                "Focusing car registration number input"
            )
            section_text = None
            try:
                await car_input.click()
                section_text = "Please Fill Your Details"
                log.step_success(
                "STEP_4_FOCUS_CAR_INPUT",
                url=page.url,
                section=section_text
            )
                dbsync.push_latest_step(log)

            except Exception as e:
                log.step_error(
                    "STEP_4_FOCUS_CAR_INPUT",
                    error=str(e),
                    url=page.url,
                    section=section_text
                ) 
                dbsync.push_latest_step(log)
          
            await human_delay(500, 1200)

            # STEP 5 — Typing car number (FATAL)
            log.step_start("STEP_5_CAR_NUMBER", "Typing car number", car_number=car_number)
            try:
                await type_if_empty(car_input, car_number)
                log.step_success("STEP_5_CAR_NUMBER", current_url=page.url)
            except Exception as e:
                log.step_error(
                    "STEP_5_CAR_NUMBER",
                    error=str(e),
                    error_type="NETWORK_ERROR" if is_network_error(e) else "AUTOMATION_ERROR",
                    car_number=car_number,
                    url=page.url
                )
                dbsync.push_latest_step(log)
                raise
            await human_delay(1200, 2500)
            print("Step 6: Locating 'View Prices' button")
            log.step_start(
             "STEP_6_LOCATE_VIEW_PRICES",
             "Locating View Prices button"
            )     

            try:
                view_prices_btn = page.locator("button#btnSubmit:has-text('View Prices')")
                await view_prices_btn.wait_for(state="visible")

                box = await view_prices_btn.bounding_box()
                mouse_moved = False

                if box:
                    current_box = await page.locator("body").bounding_box()
                    if current_box:
                        current_x = current_box["x"] + current_box["width"] / 2
                        current_y = current_box["y"] + current_box["height"] / 2
                        target_x = box["x"] + box["width"] / 2
                        target_y = box["y"] + box["height"] / 2

                        await bezier_mouse_move(
                            page,
                            current_x,
                            current_y,
                            target_x,
                            target_y
                        )
                        mouse_moved = True

                log.step_success(
                    "STEP_6_LOCATE_VIEW_PRICES",
                    selector="button#btnSubmit:has-text('View Prices')",
                    mouse_move=mouse_moved,
                    url=page.url
                )
                dbsync.push_latest_step(log)

            except Exception as e:
                    log.step_error(
                    "STEP_6_LOCATE_VIEW_PRICES",
                    error=str(e),
                    selector="button#btnSubmit:has-text('View Prices')",
                    url=page.url
            )
                    dbsync.push_latest_step(log)
                    raise
            await human_delay(600, 1500)

            # STEP 7 — View Prices click (FATAL)
            log.step_start("STEP_7_VIEW_PRICES", "Clicking View Prices")
            try:
                before_url = page.url

                await view_prices_btn.click()
                await page.wait_for_timeout(1500)

                after_url = page.url

                if before_url != after_url:
                    log.info(
                        "REDIRECT",
                        "Redirect after View Prices",
                        from_url=before_url,
                        to_url=after_url
                    )

                log.step_success("STEP_7_VIEW_PRICES")
                dbsync.push_latest_step(log)
            except Exception as e:
               
                log.step_error(
                    "STEP_7_VIEW_PRICES",
                    error=str(e),
                    error_type="NETWORK_ERROR" if is_network_error(e) else "AUTOMATION_ERROR",
                    from_url=page.url
                )
                dbsync.push_latest_step(log)
                raise
             # Wait for navigation/response with challenge detection
            await page.wait_for_timeout(random.randint(1500, 2500))
            await wait_for_akamai_challenge(page)
            log.info("AKAMAI", "Akamai check completed", url=page.url)
            dbsync.push_latest_akamai(log)
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=10000)
            except Exception as e:
             log.info("NON_FATAL_IGNORED", error=str(e))


            await page.wait_for_timeout(random.randint(1000, 2000))

            await human_delay(800, 1800)
            print("Step 8: Waiting for Additional Details")
            log.step_start(
            "STEP_8_WAIT_ADDITIONAL_DETAILS",
            "Waiting for Additional Details form (Name input)"
        )

            try:
                await page.locator("input#txtName").wait_for(
                    state="visible",
                    timeout=60000
                )

                log.step_success(
                    "STEP_8_WAIT_ADDITIONAL_DETAILS",
                    selector="input#txtName",
                    url=page.url
                )
                dbsync.push_latest_step(log)
            except Exception as e:
                
                log.step_error(
                    "STEP_8_WAIT_ADDITIONAL_DETAILS",
                    error=str(e),
                    error_type="NETWORK_ERROR" if is_network_error(e) else "AUTOMATION_ERROR",
                    selector="input#txtName",
                    timeout_ms=60000,
                    url=page.url
                )
                dbsync.push_latest_step(log)
                raise
 # FATAL – cannot proceed without customer details form

            await human_delay(800, 1800)

            # STEP 9 — Customer details (FATAL)
            log.step_start(
                "STEP_9_CUSTOMER_DETAILS",
                "Filling customer details",
                name=cust_name,
                phone=phone
            )
            try:
                await fill_details_and_view_prices(page, cust_name, phone)

                log.step_success("STEP_9_CUSTOMER_DETAILS", current_url=page.url)
                dbsync.push_latest_step(log)
            except Exception as e:
                 log.step_error(
        "STEP_9_CUSTOMER_DETAILS",
        error=str(e),
        error_type="NETWORK_ERROR" if is_network_error(e) else "AUTOMATION_ERROR",
        name=cust_name,
        phone=phone,
        url=page.url
    )
                 dbsync.push_latest_step(log)
                 raise

            await page.wait_for_timeout(random.randint(3000, 5000))
            await wait_for_akamai_challenge(page)
            log.info("AKAMAI", "Akamai check completed", url=page.url)
            dbsync.push_latest_akamai(log)
            

            try:
                await page.wait_for_load_state("domcontentloaded", timeout=10000)
            except Exception as e:
             log.info("NON_FATAL_IGNORED", error=str(e))

            await page.wait_for_timeout(random.randint(5000, 10000))
            await human_delay(800, 1600)

            # STEP 10 — Policy expiry (FATAL)
            log.step_start("STEP_10_POLICY_EXPIRY", "Selecting policy expiry", value=policy_expiry)

            section_text = None
            try:
                await select_expiry_answer(page, policy_status=policy_expiry)
                section_text = "When does your policy expire?"
                log.step_success(
        "STEP_10_POLICY_EXPIRY",
        section=section_text,
        value=policy_expiry
    )
                dbsync.push_latest_step(log)
            except Exception as e:
                log.step_error(
                    "STEP_10_POLICY_EXPIRY",
                    error=str(e),
                    error_type=(
                        "NETWORK_ERROR"
                        if is_network_error(e)
                        else "UI_VARIANT"
                        if "Timeout" in str(e)
                        else "AUTOMATION_ERROR"
                    ),
                    value=policy_expiry
                )
                dbsync.push_latest_step(log)
                raise

            await human_delay(700, 1500)

            # STEP 11 — Claim status (FATAL)
            log.step_start("STEP_11_CLAIM_STATUS", "Selecting claim status", value=claim_status)
            section_text = None 
            try:
                await select_claim_answer(page, claim_answer=claim_status)
                section_text = "Claim detail"
                log.step_success(
        "STEP_11_CLAIM_STATUS",
        section=section_text,
        value=claim_status
    )
                dbsync.push_latest_step(log)
            except Exception as e:
                log.step_error(
        "STEP_11_CLAIM_STATUS",
        error=str(e),
        error_type="NETWORK_ERROR" if is_network_error(e) else "AUTOMATION_ERROR",
        value=claim_status
    )
                dbsync.push_latest_step(log)
                raise
            await human_delay(800, 2000)

            await page.wait_for_timeout(random.randint(2000, 3000))
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=10000)
            except Exception as e:
              log.info("NON_FATAL_IGNORED", error=str(e))


            # STEP 12 — Car popup (NON-FATAL)
               
            log.step_start("STEP_12_CAR_POPUP", "Handling car question popup")
            section_text = None

            popup_found = False
            try:
                await human_delay(500, 1000)

                print("  → Scanning for desktop popup (fadeIn)")
                fade_btn = page.locator(
                    ".popupBox.carQuestionPopup.fadeIn button.primaryBtnV2.width-100.fontMedium"
                )
                if await fade_btn.count() > 0:
                    try:
                        await fade_btn.first.wait_for(state="visible", timeout=4000)
                        await fade_btn.first.click()
                        await human_delay(500, 1000)
                        print("  → Car question popup (fadeIn) dismissed successfully")
                        popup_found = True
                    except Exception as e:
                        print(f"  → Could not click fadeIn button: {str(e)}")

                if not popup_found:
                    await human_delay(500, 1000)

                    print("  → Scanning for mobile popup (slideToTop)")
                    slide_btn = page.locator(
                        ".popupBox.carQuestionPopup.slideToTop button.primaryBtnV2.width-100.fontMedium"
                    )
                    if await slide_btn.count() > 0:
                        try:
                            await slide_btn.first.wait_for(state="visible", timeout=4000)
                            await slide_btn.first.click()
                            await human_delay(500, 1000)
                            print(
                                "  → Car question popup (slideToTop) dismissed successfully"
                            )
                            popup_found = True
                        except Exception as e:
                            print(f"  → Could not click slideToTop button: {str(e)}")

                if not popup_found:
                    print("  → No car question popup found")

            except Exception as e:
                print(f"  → Error during car question popup handling: {str(e)}")
                log.info(
                    "CAR_POPUP_ERROR",
                    "Error during car question popup handling",
                    error=str(e),
                    url=page.url
                )

            # ✅ Section assignment handled cleanly for both desktop & mobile
            if popup_found:
                section_text = "How much do you drive in a year?"

            log.step_success(
                "STEP_12_CAR_POPUP",
                popup_found=popup_found,
                section=section_text
            )
            dbsync.push_latest_step(log)
            await human_delay(800, 1500)

            await page.wait_for_timeout(random.randint(1500, 2500))
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=10000)
            except Exception as e:
                log.info("NON_FATAL_IGNORED", error=str(e))

#  # ===== FORCE MOBILE VIEW BEFORE STEP 13 =====
#             print("🔄 Forcing mobile view before Step 13 Addons...")

#             current_url = page.url

#             await context.close()

#             iphone = p.devices["iPhone 13"]

#             context = await browser.new_context(
#                 **iphone,
#                 ignore_https_errors=True,
#                 locale="en-IN",
#                 timezone_id="Asia/Kolkata",
#                 service_workers="block",
#                 color_scheme=random.choice(["light", "dark"]),
#                 reduced_motion="no-preference",
#             )

#             page = await context.new_page()

#             await page.add_init_script(ANTI_DETECTION_JS)
#             await page.add_init_script(MOBILE_STEALTH_JS)

#             await page.goto(current_url, wait_until="domcontentloaded")

#             await page.wait_for_load_state("domcontentloaded", timeout=15000)
#             await wait_for_akamai_challenge(page)

#             await human_delay(1000, 2000)

            log.step_start("STEP_13_ADDONS", "Selecting addons and applying filters")
            section_text = None

            try:
                clicked, skipped = await select_allowed_addons(
                    page,
                    ALLOWED_ADDONS
                )

                section_text = "Premium Breakup"

                log.step_success(
                    "STEP_13_ADDONS",
                    clicked=clicked,
                    skipped_already_checked=skipped,
                    url=page.url,
                    section=section_text
                )
                dbsync.push_latest_step(log)

            except Exception as e:
                log.step_error(
                    "STEP_13_ADDONS",
                    error=str(e),
                    error_type="NETWORK_ERROR" if is_network_error(e) else "AUTOMATION_ERROR",
                    url=page.url
                )
                dbsync.push_latest_step(log)
                raise

            print("Step 14: Checking and closing quote popup")
            log.step_start(
                "STEP_14_CLOSE_QUOTE_POPUP",
                "Checking and closing quote popup if present"
            )

            popup_detected = False
            popup_closed = False

            try:
                popup_container = page.locator(".inner.padding0")

                try:
                    await popup_container.first.wait_for(state="visible", timeout=5000)
                    popup_detected = True
                    log.info(
                        "QUOTE_POPUP_DETECTED",
                        "Quote popup detected",
                        selector=".inner.padding0",
                        url=page.url
                    )
                except:
                    log.info(
                        "QUOTE_POPUP_NOT_FOUND",
                        "No quote popup found",
                        selector=".inner.padding0",
                        url=page.url
                    )

                if popup_detected and await popup_container.count() > 0:
                    close_btn = popup_container.locator(".crossBtn")
                    try:
                        await close_btn.first.wait_for(state="visible", timeout=3000)
                        await close_btn.first.click()
                        await human_delay(400, 900)
                        popup_closed = True
                       
                    except Exception as inner_e:
                        log.warn(
                            "QUOTE_POPUP_CLOSE_FAILED",
                            "Popup close button found but click failed",
                            error=str(inner_e),
                            url=page.url
                        )
                log.step_success(
                "STEP_14_CLOSE_QUOTE_POPUP",
                popup_detected=popup_detected,
                popup_closed=popup_closed,
                url=page.url
            )
                dbsync.push_latest_step(log)
            except Exception as e:
                log.warn(
                    "QUOTE_POPUP_ERROR",
                    "Unexpected error while handling quote popup",
                    error=str(e),
                    url=page.url
                )
                log.step_error(
                "STEP_14_CLOSE_QUOTE_POPUP",
                error=str(e),
                error_type="NETWORK_ERROR" if is_network_error(e) else "AUTOMATION_ERROR",
                popup_detected=popup_detected,
                popup_closed=popup_closed,
                url=page.url
            )
                dbsync.push_latest_step(log)
            # STEP 16 — Final processing wait (NON-FATAL)
            await human_delay(800, 1500)
            print("Step 16: Final processing wait - Capturing delayed responses")
            await page.wait_for_timeout(60000)

            await context.close()
            await browser.close()
            log.run_success()
            dbsync.finalize_run("SUCCESS")
            # try:
            #     push_run_folder_to_db(base_dir)
            # except Exception as e:
            #     log.warn("DB_PUSH_FAIL", "Failed to push run to DB", error=str(e))
        
    # log.step_start(
    #     "STEP_16_FINAL_WAIT",
    #     "Final processing wait to capture delayed API responses"
    # )

    # try:
    #     await human_delay(800, 1500)
    #     await page.wait_for_timeout(60000)

    #     log.step_success(
    #         "STEP_16_FINAL_WAIT",
    #         waited_ms=60000,
    #         url=page.url
    #     )

    #     await context.close()
    #     await browser.close()
    #     log.run_success()

    # except Exception as e:
    #     log.step_error(
    #         "STEP_16_FINAL_WAIT",
    #         error=str(e),
    #         error_type="NETWORK_ERROR" if is_network_error(e) else "AUTOMATION_ERROR"
    #     )
    #     raise

    except Exception as e:
        log.run_error(
            error=str(e) or "UNKNOWN_ERROR",
            error_type="NETWORK_ERROR" if is_network_error(e) else "AUTOMATION_ERROR",
            car=car_number,
        )
        try:
            dbsync.finalize_run("FAILED")
        except Exception as e:
            log.warn("DB_PUSH_FAIL", "Failed to push run to DB", error=str(e))

    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(run(CAR_NAME, CAR_NUMBER, PHONE, CUST_NAME))
