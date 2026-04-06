"""
Utility functions for PolicyBazaar automation flow.
Each function represents a distinct step in the automation process.
"""

import asyncio
import random
import re
from typing import Optional
from xml.sax.xmlreader import Locator
from patchright.async_api import Page, async_playwright, Request
from playwright_stealth import Stealth
from policy_bazaar_utils.pb_utils import (
    ANTI_DETECTION_JS,
    MOBILE_STEALTH_JS,
    PROXY_SETTINGS,
    
    bezier_mouse_move,
    fill_details_and_view_prices,
    handle_response,
    human_delay,
    random_human_noise,
    select_claim_answer,
    select_expiry_answer,
    type_if_empty,
    wait_for_akamai_challenge,
    wait_for_intent_modal,
)

# Global variables for tracking coverage responses
_captured_response_data = None
_response_event = None

# ============================================================================
# CUSTOMER DETAILS TRACKING FOR SMART ROTATION
# ============================================================================
# ============================================================================
# CUSTOMER DATA ROTATOR
# ============================================================================
import json
import os

_customer_data = []
_customer_index = 0

def _load_customer_data(filepath: str = "customer_data.json"):
    """Load customer data from JSON file once."""
    global _customer_data
    if _customer_data:
        return  # already loaded
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        full_path = os.path.join(base_dir, filepath)
        with open(full_path, "r") as f:
            _customer_data = json.load(f)
        print(f"✅ Loaded {len(_customer_data)} customer records from {filepath}")
    except Exception as e:
        print(f"❌ Failed to load customer data: {e}")
        raise

async def get_rotated_name_and_mobile() -> dict:
    """Pick first customer, rotate them to the end, save back."""
    _load_customer_data()

    if not _customer_data:
        raise Exception("No customer data loaded")

    # Pick first
    pair = _customer_data[0]

    # Move first to end
    _customer_data.append(_customer_data.pop(0))

    # Save back to file
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        save_path = os.path.join(base_dir, "customer_data.json")
        with open(save_path, "w") as f:
            json.dump(_customer_data, f, indent=4)
        print(f"  💾 Saved rotation to: {save_path}")          # ← shows exact path
        print(f"  📋 Next in queue: {_customer_data[0]['name']}")  # ← confirms next run
    except Exception as e:
        print(f"  ❌ Failed to save rotation: {e}")             # ← shows the real error

    print(f"  🔄 Next up: {pair['name']} / {pair['phone']}")
    return {"name": pair["name"], "phone": pair["phone"]}

class CustomerDetailsTracker:
    """Tracks and manages customer details rotation with intelligent skipping."""
    
    def __init__(self):
        self.current_name = None
        self.current_phone = None
        self.previous_name = None
        self.previous_phone = None
        self.execution_count = 0
    
    async def rotate_customer_details(self):
        """Rotate to next customer pair."""
        self.previous_name = self.current_name
        self.previous_phone = self.current_phone
        # Use paired rotation to ensure name+phone uniqueness and persistence
        pair = await get_rotated_name_and_mobile()
        self.current_name = pair.get("name")
        self.current_phone = pair.get("phone")
        self.execution_count += 1
        
        return {
            "name": self.current_name,
            "phone": self.current_phone,
            "is_same_as_previous": (self.current_name == self.previous_name and self.current_phone == self.previous_phone),
            "execution_count": self.execution_count,
        }
    
    def get_current_pair(self):
        """Get current customer pair."""
        return {
            "name": self.current_name,
            "phone": self.current_phone,
        }
    
    def is_same_as_previous(self):
        """Check if current pair matches previous."""
        return (self.current_name == self.previous_name and 
                self.current_phone == self.previous_phone)

tracker = CustomerDetailsTracker()

async def initialize_test_data():
    """Initialize all test data by calling individual rotation functions."""
    global PHONE, CUST_NAME, CURRENT_DEVICE, CURRENT_USER_AGENT, CURRENT_DEVICE_CONFIG
    
    # Load device from support_device.json
    try:
        device_name, device_config = await rotate_device_from_json("support_device.json")
        if device_name and device_config:
            CURRENT_DEVICE = device_name
            CURRENT_DEVICE_CONFIG = device_config
            CURRENT_USER_AGENT = device_config.get("userAgent", "")
            print(f" Loaded device from support_device.json: {device_name}")
        else:
            raise Exception("No device found in support_device.json")
    except Exception as e:
        print(f" Could not load device from support_device.json: {e}")
        raise
    
    # Rotate phone and customer name together (paired rotation)
    pair = await get_rotated_name_and_mobile()
    PHONE = pair.get("phone")
    CUST_NAME = pair.get("name")
    
    # Policy expiry and claim status are NOT rotated - use fixed values
    
    print(f" Test data initialized:")
    print(f"    Device: {CURRENT_DEVICE}")
    print(f"   🔑 User Agent: {CURRENT_USER_AGENT[:80]}...")
    print(f"   👤 Customer: {CUST_NAME}")
    print(f"   📞 Phone: {PHONE}")
    
    return {
        "device": CURRENT_DEVICE,
        "device_config": CURRENT_DEVICE_CONFIG,
        "user_agent": CURRENT_USER_AGENT,
        "phone": PHONE,
        "name": CUST_NAME,
        "policy_expiry": POLICY_EXPIRY,
        "claim_status": CLAIM_STATUS,
    }


async def refresh_test_data():
    """Refresh all test data by rotating values."""
    return await initialize_test_data()


def detect_quotes_page_indicators(page_url: str) -> bool:
    """
    Detect if we're already on the quotes page based on URL patterns.
    
    Args:
        page_url: Current page URL
        
    Returns:
        True if URL indicates quotes page, False otherwise
    """
    if not page_url:
        return False
    
    # Common quotes page URL patterns
    quotes_indicators = [
        "/v2/quotes"
    ]
    
    page_url_lower = page_url.lower()
    return any(indicator in page_url_lower for indicator in quotes_indicators)


# ============================================================================
# BOUNDED CLICK UTILITY - Natural Human Clicking Within Element Boundaries
# ============================================================================
# ============================================================================
# GLOBAL POPUP PROTECTION FLAG
# ============================================================================
_popup_protection_active = False

def set_popup_protection(active: bool):
    global _popup_protection_active
    _popup_protection_active = active
    print(f"  🔒 Popup protection: {'ON' if active else 'OFF'}")


async def setup_all_popup_handlers(page: Page):
    """
    One-time setup. Registers event-driven auto-cancel handlers for all
    interruption popups.

    PROTECTED (never touched):
      - Policy expiry question   → Step 10  (flag-based)
      - Claim status question    → Step 11  (flag-based)
      - .popupBox.carQuestionPopup → Step 12 (selector-based)
      - .popupBody.padding0      → Addons section (selector-based)
      - .IDVPopup                → set_idv_to_median (selector-based)
      - .policyDetailPopup       → step_14 (selector-based)
    """
    async def _is_protected() -> bool:
        if _popup_protection_active:
            return True
        for sel in [
            ".popupBox.carQuestionPopup",
            ".popupBody.padding0",
            ".IDVPopup",
            ".policyDetailPopup",
        ]:
            try:
                el = page.locator(sel)
                if await el.count() > 0 and await el.is_visible():
                    return True
            except Exception:
                pass
        return False

    async def _safe_click(selector: str, timeout: int = 2000):
        try:
            btn = page.locator(selector).first
            if await btn.is_visible():
                await btn.click(timeout=timeout)
        except Exception:
            pass


    # 1. Exit intent popup
    async def _close_exit_intent():
        await _safe_click("div#exit-intent-popup-close")

    await page.add_locator_handler(
        page.locator("div#exit-intent-popup-container").first,
        _close_exit_intent,
        no_wait_after=True,
    )

    


    # 2. Call CTC overlay
    async def _close_ctc():
        try:
            ctc = page.locator("div.popupBox.padding0.fadeIn.exitPopup.ctcPopup")
            await ctc.locator("div.inner.padding0 div.crossBtn").click(timeout=2000)
            await ctc.wait_for(state="hidden", timeout=3000)
        except Exception:
            pass

    await page.add_locator_handler(
        page.locator(".blackOverLay.fadeIn.exitPopupOverlay"),
        _close_ctc,
        no_wait_after=True,
    )

    # 3. Coverage loading screen
    # 3. Coverage loading screen
    coverage_screen = page.locator(".mainLoadingScreenWrapper")
    async def _handle_coverage_screen():
        try:
            # Try "Complete Protection" first
            box = coverage_screen.locator(
                ".coverageBox",
                has=page.locator(".headingV4", has_text="Complete Protection"),
            )
            if await box.count() > 0 and await box.is_visible():
                await box.click(timeout=3000)
            else:
                # Fallback: pick first available coverageBox
                first_box = coverage_screen.locator(".coverageBox").first
                if await first_box.count() > 0:
                    await first_box.click(timeout=3000)

            # Click submit/continue
            submit = coverage_screen.locator(".chooseCoverageFooter button.submit")
            if await submit.count() > 0:
                await submit.click(timeout=3000)
            else:
                # Fallback: any primary button inside the screen
                btn = coverage_screen.locator("button.primaryBtnV2").first
                if await btn.count() > 0:
                    await btn.click(timeout=3000)

            # Wait for it to disappear so handler doesn't re-trigger
            await coverage_screen.wait_for(state="hidden", timeout=5000)

        except Exception:
            pass

    await page.add_locator_handler(coverage_screen, _handle_coverage_screen, no_wait_after=True)


    # 4. Choose coverage popup
    async def _skip_choose_coverage():
        if await _is_protected():
            return
        await _safe_click(".chooseCoverageWrap .chooseCoverageFooter .skip")
    await page.add_locator_handler(
        page.locator(".chooseCoverageWrap"), _skip_choose_coverage, no_wait_after=True
    )

    # 5. Generic .popupWrapper
    async def _close_popup_wrapper():
        if await _is_protected():
            return
        await _safe_click(".popupWrapper .crossBtn")
    await page.add_locator_handler(
        page.locator(".popupWrapper"), _close_popup_wrapper, no_wait_after=True
    )

    # 6. Any other .popupBox (not protected)
    # 6. Any other .popupBox (not protected)
    async def _close_generic_popup_box():
        if await _is_protected():
            return

        closed = False
        for sel in [".popupBox .crossBtn", ".popupBox .closeBtn",
                    ".popupBox button.close", ".popupBox [aria-label='Close']"]:
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0 and await btn.is_visible():
                    await btn.click(timeout=2000)
                    print(f"  ✅ Auto-closed popup using: {sel}")
                    closed = True
                    return
            except Exception:
                pass

        if not closed:
            try:
                popup = page.locator(".popupBox").first
                class_name = await popup.get_attribute("class")
                print(f"  ⚠️  UNKNOWN POPUP — class: '{class_name}'")
                print(f"  ⚠️  Add a handler for this in setup_all_popup_handlers!")
            except Exception:
                pass

    await page.add_locator_handler(
        page.locator(".popupBox"), _close_generic_popup_box, no_wait_after=True
    )


    print("✅ All popup handlers registered (event-driven)")





async def click_within_bounds(locator, padding=0.2):
    """
    Click within visible element boundaries using natural human positioning.
    
    Instead of clicking center point, this:
    - Gets element bounding box
    - Calculates clickable area with padding
    - Chooses random point within bounds
    - Mimics natural human click behavior
    
    Args:
        locator: Playwright locator object
        padding: Padding ratio (0.0-1.0) from element edges
                 0.2 = 20% padding on each side = 60% clickable area
    
    Returns:
        None (performs click)
    
    Example:
        # Click "Continue" button naturally within bounds
        await click_within_bounds(page.locator(".submit"))
    """
    try:
        # Get element bounding box
        box = await locator.bounding_box()
        
        if not box:
            # Fallback to normal click if bounding box not available
            await locator.click()
            return
        
        # Extract dimensions
        x, y, width, height = box["x"], box["y"], box["width"], box["height"]
        
        # Calculate clickable area with padding
        padding_x = width * padding
        padding_y = height * padding
        
        click_x_min = x + padding_x
        click_x_max = x + width - padding_x
        click_y_min = y + padding_y
        click_y_max = y + height - padding_y
        
        # Choose random point within bounds (natural human behavior)
        click_x = random.uniform(click_x_min, click_x_max)
        click_y = random.uniform(click_y_min, click_y_max)
        
        # Get page for click action
        page = locator.page
        
        # Perform human-like mouse movement and click
        await bezier_mouse_move(page, click_x, click_y)
        await human_delay(200, 400)
        await page.mouse.click(click_x, click_y)
        
    except Exception as e:
        # Fallback to standard click on error
        print(f"    Bounded click failed ({e}), using standard click")
        await locator.click()


async def click_within_bounds_force(locator, padding=0.2):
    """
    Force click within bounds (bypasses visibility checks).
    
    Args:
        locator: Playwright locator object
        padding: Padding ratio (0.0-1.0)
    
    Returns:
        None (performs click)
    """
    try:
        # Get bounding box
        box = await locator.bounding_box()
        
        if not box:
            # Fallback to force click
            await locator.click(force=True)
            return
        
        # Calculate click point within bounds
        x, y, width, height = box["x"], box["y"], box["width"], box["height"]
        padding_x = width * padding
        padding_y = height * padding
        
        click_x = random.uniform(x + padding_x, x + width - padding_x)
        click_y = random.uniform(y + padding_y, y + height - padding_y)
        
        page = locator.page
        
        # Force click at position
        await page.mouse.click(click_x, click_y)
        
    except Exception as e:
        # Final fallback
        await locator.click(force=True)

# ============================================================================
# STEP 1-7: NAVIGATION & CAR REGISTRATION
# ============================================================================

async def step_1_navigate_to_motor_insurance(page: Page, url : Optional[str] = None):
    """Step 1: Navigate to PolicyBazaar motor insurance page"""
    print("Step 1: Navigating to PolicyBazaar motor insurance page")
    
    url = url if url else "https://www.policybazaar.com/motor-insurance/"
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            print(f"  → Attempt {retry_count + 1}/{max_retries}...")
            response = await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=90000,  # 90s for slow proxies
            )
            print(f" Navigation successful (Status: {response.status if response else 'N/A'})")
            break
        except Exception as e:
            retry_count += 1
            error_msg = str(e)[:120]
            if retry_count < max_retries:
                print(f" Attempt {retry_count} failed: {error_msg}")
                print(f"  → Waiting before retry...")
                await human_delay(5000, 8000)
            else:
                print(f" Navigation failed after {max_retries} attempts")
                print(f"  Error: {error_msg}")
                raise
    
    await human_delay(2000, 4000)


async def step_1_5_check_akamai_challenge(page: Page):
    """Step 1.5: Check for Akamai challenges"""
    print("Step 1.5: Checking for Akamai challenges")
    await wait_for_akamai_challenge(page)
    
    # Wait for page to fully load
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=1500)
    except:
        pass  # Continue even if domcontentloaded doesn't complete
    
    await human_delay(100, 250)


async def step_2_add_random_human_noise(page: Page):
    """Step 2: Add random human noise"""
    print("Step 2: Add random human noise")
    await random_human_noise(page)

    # print("Step 2.5: waiting for intent modal to close")
    await page.wait_for_timeout(random.randint(10000, 12000))
    await wait_for_intent_modal(page)


async def step_3_locate_car_input(page: Page):
    """Step 3: Locate car registration number input field"""
    print("Step 3: Locating car registration number input field")
    car_input_div = page.locator("div.mainForm").locator("div.input_box")
    car_input = car_input_div.locator("input.carRegistrationNumber")
    return car_input


async def step_4_focus_car_input(car_input: Page):
    """Step 4: Focus on car registration number input"""
    await human_delay(600, 1400)

    print("Step 4: Focusing on car registration number input")
    await car_input.click()
    await human_delay(500, 1200)


async def step_5_type_car_number(car_input: Page, car_number: str):
    """Step 5: Type car number"""
    print(f"Step 5: Typing car number: {car_number}")
    await type_if_empty(car_input, car_number)

    await human_delay(1200, 2500)


async def step_6_locate_view_prices_button(page: Page):
    """Step 6: Locate 'View Prices' button"""
    print("Step 6: Locating 'View Prices' button")
    view_prices_btn = page.locator("button#btnSubmit:has-text('View Prices')")
    await view_prices_btn.wait_for(state="visible")
    return view_prices_btn


async def step_7_click_view_prices(page: Page, view_prices_btn):
    """Step 7: Click 'View Prices' button from registration form"""
    print("Step 7: Clicking 'View Prices' button from registration form")
    
    box = await view_prices_btn.bounding_box()
    if box:
        # Get current mouse position (approximate)
        current_box = await page.locator("body").bounding_box()
        if current_box:
            current_x = current_box["x"] + current_box["width"] / 2
            current_y = current_box["y"] + current_box["height"] / 2
            target_x = box["x"] + box["width"] / 2
            target_y = box["y"] + box["height"] / 2
            await bezier_mouse_move(page, current_x, current_y, target_x, target_y)

    await human_delay(600, 1500)
    await view_prices_btn.click()
    
# ============================================================================
# STEP 8-9: CUSTOMER DETAILS
# ============================================================================

async def step_8_wait_for_customer_form(page: Page):
    """Step 8: Waiting for Additional Details"""
    print("Step 8: Waiting for Additional Details")
    await page.locator("input#txtName").wait_for(state="visible", timeout=60000)
    await human_delay(800, 1800)


async def step_9_fill_customer_details(page: Page, cust_name: str, phone: str):
    """Step 9: Fill customer details and click View Prices to navigate to quotes"""
    print("\n" + "="*70)
    print("STEP 9: FILLING CUSTOMER DETAILS")
    print("="*70)
    print(f" Customer Name: {cust_name}")
    print(f" Customer Phone: {phone}")
    
    try:
        await fill_details_and_view_prices(page, cust_name, phone)
    except Exception as e:
        print(f"\n STEP 9 FAILED: {e}")
        raise

    #  Now navigating to QUOTES PAGE via View Prices button
    print("\nStep 9: Navigating to quotes page...")
    
    # Wait for form submission response
    await page.wait_for_timeout(random.randint(1500, 2500))

    # Wait for network activity
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=10000)
    except:
        pass

    await human_delay(800, 1000)
    
    print("Step 9 PASSED: Successfully navigated to quotes page")
    print("="*70)


# ============================================================================
# STEP 10-11: POLICY & CLAIM SELECTION
# ============================================================================

async def step_10_select_policy_expiry(page: Page, policy_expiry: str):
    """Step 10: Select policy expiry answer"""
    print("Step 10: Selecting policy expiry answer")
    print(f"   Selecting policy expiry status: {policy_expiry}")
    set_popup_protection(True)
    try:
        await select_expiry_answer(page, policy_status=policy_expiry)
    finally:
        set_popup_protection(False)
    await human_delay(700, 1500)


async def step_11_select_claim_status(page: Page, claim_status: str):
    """Step 11: Select claim answer"""
    print("Step 11: Selecting claim answer")
    print(f"   Selecting claim status: {claim_status}")
    set_popup_protection(True)
    try:
        await select_claim_answer(page, claim_answer=claim_status)
    finally:
        set_popup_protection(False)
    await human_delay(800, 2000)

    
    # Wait for page to settle after claim selection
    await page.wait_for_timeout(random.randint(500, 900))
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=1000)
    except:
        pass


# ============================================================================
# STEP 12: CAR QUESTION POPUP
# ============================================================================

async def step_12_handle_car_question_popup(page: Page):
    """Step 12: Check and handle car question popup"""
    print("Step 12: Checking and handling car question popup")
    popup_found = False
    try:
        await human_delay(500, 1000)
        
        # Check for Desktop view (fadeIn animation)
        print("  → Scanning for desktop popup (fadeIn)")
        fade_btn = page.locator(
            ".carQuestionPopup.fadeIn button.primaryBtnV2.width-100.fontMedium"
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
        
        # Check for Mobile view (slideToTop animation)
        if not popup_found:
            await human_delay(500, 1000)
            print("  → Scanning for mobile popup (slideToTop)")
            slide_btn = page.locator(
                ".carQuestionPopup.slideToTop button.primaryBtnV2.width-100.fontMedium"
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
    
    await human_delay(800, 1500)
    
    # Wait for page to settle before add-ons selection
    await page.wait_for_timeout(random.randint(1500, 2500))
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=10000)
    except:
        pass

# ============================================================================
# STEP Controlled Popup Handler
# ============================================================================

# ============================================================================
# STEP 13.0-13.4: ADDONS SELECTION
# ============================================================================

async def step_13_0_click_addons_filter(page: Page):
    """Step 13.0: Expand Addons filter accordion if collapsed"""
    print("Step 13.0: Clicking Addons filter")
    
    try:
        filters_div = page.locator("div.quoteFilters.non-sticky.show")
        await filters_div.wait_for(state="visible", timeout=10000)

        addons_section = filters_div.locator("label.accPanel").filter(
            has=page.locator("p:has-text('Addons')")
        )

        if await addons_section.count() == 0:
            raise Exception("Addons accPanel section not found in filters div")

        addons_acc_body = addons_section.locator("div.accBody")

        if await addons_acc_body.count() == 0:
            raise Exception("Addons accBody not found inside accPanel")

        if not await addons_acc_body.is_visible():
            print("  → Addons accordion is collapsed, expanding...")
            await addons_section.locator("div.accHead").click()
            await human_delay(500, 1000)
            await addons_acc_body.wait_for(state="visible", timeout=5000)
            print("  → Addons accordion expanded successfully")
        else:
            print("  → Addons accordion already expanded, skipping click")

    except Exception as e:
        print(f"  → Error clicking Addons filter: {str(e)}")
        raise



async def step_13_1_open_addons_section(page: Page):
    """Step 13.1: Open ADDONS section (NOT a popup - critical workflow step)"""

    print("Step 13.1: Opening ADDONS section")

    try:
        # First, wait for page to settle
        print("  → Waiting for page to load...")
        await human_delay(1500, 2000)
        
        # Check if Addons link is visible anywhere on the page
        print("  → Scanning for Addons link...")
        
        # Strategy 1: Look for any text containing "addon" (case-insensitive)
        addon_elements = page.locator("text=/addons?/i")
        count = await addon_elements.count()
        print(f"  → Found {count} elements with 'addon' text")
        
        if count > 0:
            # Find the clickable parent (likely a p tag or div)
            addons_link = None
            for i in range(count):
                element = addon_elements.nth(i)
                try:
                    # Get the element and check if it's clickable
                    is_visible = await element.is_visible()
                    if is_visible:
                        print(f"  → Element {i} is visible, attempting click...")
                        addons_link = element
                        break
                except:
                    continue
            
            if not addons_link:
                # If no single element found, try locating parent p tag
                print("  → No visible element found, trying parent locator...")
                addons_link = page.locator("p", has_text=re.compile(r"addons?", re.IGNORECASE)).first
                await addons_link.wait_for(state="visible", timeout=5000)
        else:
            # Strategy 2: If no text match, check page structure
            print("  → No 'addon' text found! Checking page structure...")
            html = await page.content()
            
            if "addon" in html.lower():
                print("  → 'addon' found in HTML but not visible - page may be loading")
                # Wait longer for page to fully load
                await human_delay(3000, 4000)
                # Retry with longer timeout
                addons_link = page.locator("p", has_text=re.compile(r"addons?", re.IGNORECASE)).first
                await addons_link.wait_for(state="visible", timeout=10000)
            else:
                raise Exception(" Addons link not found in page HTML - wrong page or step executed too early")
        
        # Click Addons section
        print("  → Clicking Addons link...")
        await addons_link.scroll_into_view_if_needed()
        await human_delay(300, 500)
        await addons_link.click()
        print("  → Addons link clicked")

        await human_delay(1000, 1500)

        # Wait for Addons section container to appear
        print("  → Waiting for Addons section container...")
        await page.wait_for_selector(".popupBody.padding0", timeout=20000)
        print("  → ADDONS section opened successfully")

        # 🔒 Close only external popups (do NOT close Addons section)
        

    except Exception as e:
        print(f"  →  Error opening ADDONS section: {str(e)}")
        print("  → Taking screenshot for debugging...")
        try:
            await page.screenshot(path="/tmp/addons_error.png")
            print("  → Screenshot saved to /tmp/addons_error.png")
        except:
            pass
        raise

    await human_delay(1000, 1500)


async def step_13_2_click_addons_tab(page: Page):
    """Step 13.2: Click Addons tab (workflow step, NOT popup closure)"""
    print("Step 13.2: Clicking Addons tab")
    addon_section = page.locator(".popupBody.padding0")

    try:
        await addon_section.wait_for(state="visible", timeout=10000)
        await human_delay(500, 800)

        addons_tab = addon_section.locator(".tabItem:has-text('Addons')")
        
        try:
            await addons_tab.wait_for(state="visible", timeout=5000)
            await addons_tab.evaluate("el => el.click()")
            print("  → Addons tab clicked successfully")
            await human_delay(2000, 2500)
            await human_delay(1000, 1500)
        except Exception as tab_error:
            # If Addons tab click fails, continue anyway - may already be visible
            print(f"   Addons tab wait/click failed ({str(tab_error)[:100]})")
            print(f"  → Continuing to next step (tab may already be active)...")
            await human_delay(1000, 1500)

        # 🔒 Close external popup if any
        

        print("  → Addons tab content ready")

    except Exception as e:
        print(f"   Error in Addons section: {str(e)[:100]}")
        print(f"  → Skipping to step 14 (extract coverage)...")
        # Don't raise - allow continuation to step 14
        return None

    return addon_section


async def step_13_4_apply_filters(page: Page):
    """Step 13.4: Click Apply Filters button"""
    print("Step 13.4: Clicking Apply Filters button")
    try:
        if page.is_closed():
            raise Exception(" Page closed unexpectedly!")

        await human_delay(1000, 1500)

        footer = page.locator("div.popupFooter")
        await footer.wait_for(state="visible", timeout=15000)
        await human_delay(600, 900)

        apply_button = footer.locator("div.row div.col div.primaryBtnV2.m0")

        if await apply_button.count() == 0:
            raise Exception(" Apply Filters button not found!")

        await human_delay(500, 800)

        print("  → Clicking Apply Filters...")
        await apply_button.first.scroll_into_view_if_needed()
        await human_delay(400, 600)
        await apply_button.first.click()

        print("  →  Apply Filters clicked successfully.")

        # 🔒 Popup check after clicking Apply
        await human_delay(1000, 1500)
        

        #  Proper Dynamic Page Reload Wait
        print("  → Waiting for quotes page reload...")

        await page.wait_for_load_state("networkidle", timeout=30000)
        await human_delay(2000, 3000)

        print("  → Quotes page reloaded successfully")

    except Exception as e:
        print(f"  → Error clicking Apply Filters: {str(e)}")

# ============================================================================
# STEP 13.7: OPEN NEW MOBILE TAB
# ============================================================================


async def set_idv_to_median(page: Page, action: str = "set_median") -> None:
    set_popup_protection(True)   # 🔒 protect IDV modal
    try:
        try:
            # 1. Click the IDV selection trigger
            await page.locator(".IDVInfo .dashedBtmBorderDark").click()
        except Exception as e:
            print(f"  → Error clicking IDV selection trigger: {str(e)}")
            print("trying alternative method to trigger IDV modal...")
            await trigger_idv_type_2(page)

        # 2. Wait for the IDV modal to appear
        idv_popup = page.locator(".IDVPopup")
        await idv_popup.wait_for(state="visible")

        try:
            choose_your_idv_text = idv_popup.locator("span:has-text('Choose your own IDV')")
            await choose_your_idv_text.first.click()
        except Exception as e:
            print(f"  → Error clicking 'Choose your own IDV' text: {str(e)}")
            print("  → Continuing with range slider interaction...")

        # 3. Read min and max from the range slider attributes
        range_input = idv_popup.locator("input[type='range']")
        min_val = int(await range_input.get_attribute("min"))
        max_val = int(await range_input.get_attribute("max"))
        default_val = int(await range_input.get_attribute("value"))

        # 4. Compute median
        median_val = (min_val + max_val) // 2

        if action == "set_median":
            # 5. Clear and fill the IDV text input
            idv_text_input = idv_popup.locator("div.idvInput input[type='text']")
            await idv_text_input.click()
            await idv_text_input.fill(str(median_val))
            await idv_text_input.press("Tab")

            # 7. Click the Update button
            await idv_popup.locator("button.primaryBtnV2").click()
        else:
            await idv_popup.locator("div.crossBtn").click()

        # 8. Wait for modal to close
        await idv_popup.wait_for(state="hidden")

    finally:
        set_popup_protection(False)   # 🔓 release

    return default_val, median_val




async def expand_grouped_plans_show_btn(page: Page) -> None:
    """
    Clicks the "Show" button to expand grouped plans if it exists.
    """
    try:
        show_btn = page.locator("div.planCardGroup .showMoreBtn")
        await show_btn.first.scroll_into_view_if_needed()
        if await show_btn.count() > 0 and await show_btn.first.is_visible():
            print("  → Expanding grouped plans via 'Show' button...")
            await show_btn.first.click()
            await human_delay(800, 1200)
            print("  → Grouped plans expanded")
        else:
            print("  → No grouped plans 'Show' button found, skipping expansion")

    except Exception as e:
        print(f"  → Error expanding grouped plans: {str(e)}")
        print("  → Skipping grouped plans expansion")


async def trigger_idv_type_2(page: Page) -> None:
    """
    Triggers the IDV Type 2 flow by clicking the appropriate element.
    """
    idv_type_2_trigger = page.locator("div.IDVDetailComponent .idvInputBox")
    if await idv_type_2_trigger.count() > 0 and await idv_type_2_trigger.first.is_visible():
        print("  → Triggering IDV Type 2 flow...")
        await idv_type_2_trigger.first.click()
        await human_delay(800, 1200)
        print("  → IDV Type 2 flow triggered")
    else:
        print("  → No IDV Type 2 trigger found, skipping this step")


# ============================================================================
# STEP 13.6: OPEN NEW MOBILE TAB
# ============================================================================

async def open_desktop_view_tab(
    browser,
    quotes_page_url: str,
    datadir: str,
    seen_responses: set,
):
    """
    Open a new independent desktop view browser context.
    
    Creates SEPARATE desktop device session for comparison/monitoring.
    No mobile simulation - pure desktop experience.
    
    Args:
        browser: Playwright browser object
        quotes_page_url: Starting URL (quotes page)
        datadir: Directory for saving responses
        seen_responses: Set to track seen response IDs
    
    Returns:
        Tuple of (new_context, new_page) - Independent desktop context and page
    """
    from pb_utils import handle_response, PROXY_SETTINGS
    
    print("\n" + "="*60)
    print("STEP 13.6: Opening Independent Desktop Tab")
    print("="*60 + "\n")
    
    try:
        # Create NEW independent desktop context (NOT mobile)
        print("🖥️  Creating independent desktop context...")
        print("   ├─ Device: Desktop (no mobile simulation)")
        print("   ├─ User Agent: Desktop user agent")
        print("   ├─ Viewport: Desktop viewport (1920x1080)")
        print("   ├─ Mobile: False")
        print("   └─ Independent: Does NOT share cookies/storage with TAB 1 or TAB 2")
        
        desktop_context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1920, "height": 1080},
            device_scale_factor=1,
            is_mobile=False,
            has_touch=False,
            proxy=PROXY_SETTINGS,
            ignore_https_errors=True,
            locale="en-IN",
            timezone_id="Asia/Kolkata",
            service_workers="block",
            color_scheme=random.choice(["light", "dark"]),
            reduced_motion="no-preference",
        )
        
        # Create new page in the desktop context
        print("🖥️  Creating new page in desktop context...")
        desktop_page = await desktop_context.new_page()
        
        # Inject anti-detection scripts
        print("  → Injecting anti-detection scripts...")
        await desktop_page.add_init_script(ANTI_DETECTION_JS)
        await desktop_page.add_init_script(MOBILE_STEALTH_JS)
        
        # Register response handler
        print("  → Registering response handler...")
        desktop_page.on(
            "response",
            lambda response: handle_response(response, datadir, seen_responses),
        )
        
        # Load the quotes page in desktop view
        print(f"🖥️  Loading page in desktop view...")
        print(f"  → URL: {quotes_page_url}")
        await desktop_page.goto(
            quotes_page_url,
            wait_until="domcontentloaded",
            timeout=60000,
        )
        
        # Wait for page to fully load
        print("   Waiting for page to fully load...")
        await human_delay(2000, 3000)
        try:
            await desktop_page.wait_for_load_state("networkidle", timeout=15000)
        except:
            print("    Network idle timeout (continuing anyway)")
        
        # Add human-like delay
        await human_delay(1500, 2500)
        
        print(f" Independent desktop tab opened successfully")
        print(f"   Device: Desktop (1920x1080)")
        print(f"   Mobile: False")
        print(f"   Has Touch: False")
        print(f"   Session: Independent (separate from TAB 1 and TAB 2)")
        print(f"   Page URL: {desktop_page.url}")
        
        # Verify desktop properties at runtime
        try:
            is_mobile = await desktop_page.evaluate("() => window.navigator.userAgent.toLowerCase().includes('mobile')")
            window_size = await desktop_page.evaluate("() => ({width: window.innerWidth, height: window.innerHeight})")
            touch_support = await desktop_page.evaluate("() => window.navigator.maxTouchPoints > 0")
            print(f"   Runtime Mobile Check: {is_mobile}")
            print(f"   Window Size: {window_size}")
            print(f"   Touch Support: {touch_support}")
        except Exception as e:
            print(f"     Could not verify runtime properties: {e}")
        
        return desktop_context, desktop_page
        
    except Exception as e:
        print(f" Error opening independent desktop tab: {e}")
        raise


async def step_13_5_open_new_mobile_tab(
    mobile_context,
    quotes_page_url: str,
    datadir: str,
    seen_responses: set,
):
    """
    Step 13.5: Open a new independent mobile browser context.
    
    Creates SEPARATE mobile device session (not inheriting from first tab).
    Loads the motor insurance page and user will go through same flow.
    
    Args:
        browser: Playwright browser object
        quotes_page_url: Starting URL (e.g., motor insurance page)
        device_config: Device configuration from support_device.json
        datadir: Directory for saving responses
        seen_responses: Set to track seen response IDs
    
    Returns:
        Tuple of (new_context, new_page) - Independent mobile context and page
    """
    from pb_utils import handle_response
    global _response_event, _captured_response_data
    
    try:
        # Initialize response tracking for step 14
        print(" Initializing response event handler...")
        _response_event = asyncio.Event()
        _captured_response_data = None
        
        # Create new page in the new mobile context
        print(" Creating new page in mobile context...")
        mobile_page = await mobile_context.new_page()

        # Register response handler for this new page
        print(" Registering response handler...")
        mobile_page.on(
            "response",
            lambda response: handle_response(response, datadir, seen_responses),
        )
        
        max_retries = 3
        retry_count = 0
        last_error = None
        
        while retry_count < max_retries:
            try:
                await mobile_page.goto(
                    quotes_page_url,
                    wait_until="domcontentloaded",
                    timeout=90000,  # 90s timeout
                )
                break
            except Exception as e:
                retry_count += 1
                last_error = e
                error_msg = str(e)[:150]
                print(f" Attempt {retry_count}/{max_retries} failed: {error_msg}")
                
                if retry_count < max_retries:
                    print(f"  Retrying in 5 seconds...")
                    await human_delay(5000, 5000)
                else:
                    print(f" Failed after {max_retries} attempts")
                    raise last_error
        
        try:
            await mobile_page.wait_for_load_state("domcontentloaded", timeout=15000)
        except:
            print("   Network idle timeout (continuing anyway)")

        return mobile_context, mobile_page
        
    except Exception as e:
        print(f" Error opening independent mobile tab: {e}")
        raise


# ============================================================================
# STEP 14: EXTRACT PLAN DETAILS & COVERAGE
# ============================================================================

async def _debug_page_structure(page: Page):
    """
    Comprehensive debug helper: Inspect page structure for View Coverage buttons.
    Provides detailed analysis of the DOM to understand why detection might be failing.
    """
    print("\n" + "="*70)
    print("🔧 DEBUG: Comprehensive Page Structure Analysis")
    print("="*70)
    
    try:
        # Get page title and URL
        title = await page.title()
        url = page.url
        print(f"\n📄 PAGE INFO:")
        print(f"  Title: {title}")
        print(f"  URL: {url[:100]}...")
        
        # Check all buttons and links
        all_buttons = await page.locator("button, a, [role='button']").count()
        print(f"\n🔘 BUTTONS & LINKS:")
        print(f"  Total button-like elements: {all_buttons}")
        
        # Check for elements with "View" or "Coverage" text
        view_elements = await page.locator("*", has_text="View").count()
        coverage_elements = await page.locator("*", has_text="Coverage").count()
        print(f"  Elements with 'View' text: {view_elements}")
        print(f"  Elements with 'Coverage' text: {coverage_elements}")
        
        # Check for specific class-based buttons
        print(f"\n🎨 CLASS-BASED ELEMENTS:")
        smaller_font = await page.locator(".smallerFont").count()
        print(f"  .smallerFont elements: {smaller_font}")
        
        show_more = await page.locator(".showMoreBtn").count()
        print(f"  .showMoreBtn elements: {show_more}")
        
        primary_btn = await page.locator(".primaryBtnV2").count()
        print(f"  .primaryBtnV2 elements: {primary_btn}")
        
        # Check plan containers
        print(f"\n📦 PLAN/CARD CONTAINERS:")
        plan_containers = await page.locator("[class*='plan'], [class*='card'], [class*='quote']").count()
        print(f"  Plan/Card/Quote containers: {plan_containers}")
        
        # Scan for "View Coverage" using JavaScript
        print(f"\n🔍 JAVASCRIPT SCAN (Direct DOM Inspection):")
        try:
            result = await page.evaluate("""
                () => {
                    // Find all elements containing "View Coverage"
                    const allElements = document.querySelectorAll('*');
                    const viewCoverageElements = [];
                    
                    for (let elem of allElements) {
                        if (elem.textContent && elem.textContent.includes('View Coverage')) {
                            // Only capture elements that directly contain the text (not nested)
                            const children = Array.from(elem.children);
                            const hasChildWithText = children.some(
                                child => child.textContent.includes('View Coverage')
                            );
                            
                            if (!hasChildWithText) {
                                viewCoverageElements.push({
                                    tag: elem.tagName,
                                    text: elem.textContent.substring(0, 50),
                                    classes: elem.className,
                                    id: elem.id,
                                    visible: elem.offsetParent !== null
                                });
                            }
                        }
                    }
                    
                    return {
                        count: viewCoverageElements.length,
                        elements: viewCoverageElements.slice(0, 10)  // First 10
                    };
                }
            """)
            
            print(f"  Found {result['count']} elements with 'View Coverage' text")
            for i, elem in enumerate(result['elements']):
                visibility = " VISIBLE" if elem['visible'] else " HIDDEN"
                print(f"    [{i}] {elem['tag']} ({visibility})")
                print(f"        Text: {elem['text']}")
                print(f"        Class: {elem['classes']}")
                print(f"        ID: {elem['id']}")
        
        except Exception as e:
            print(f"   JavaScript scan failed: {e}")
        
        # Get first 20 buttons and their details
        print(f"\n🔘 FIRST 20 BUTTONS/LINKS:")
        try:
            buttons_info = await page.evaluate("""
                () => {
                    const buttons = Array.from(document.querySelectorAll('button, a, [role="button"]'));
                    return buttons.slice(0, 20).map((b, idx) => ({
                        idx: idx,
                        tag: b.tagName,
                        text: b.innerText.substring(0, 40).trim(),
                        class: b.className.substring(0, 60),
                        visible: b.offsetParent !== null,
                        aria_label: b.getAttribute('aria-label'),
                        data_qa: b.getAttribute('data-qa')
                    }));
                }
            """)
            
            for btn in buttons_info:
                visibility = "" if btn['visible'] else ""
                print(f"    [{btn['idx']}] {visibility} {btn['tag']}: {btn['text']}")
                if btn['class']:
                    print(f"        Class: {btn['class']}")
                if btn['aria_label']:
                    print(f"        ARIA: {btn['aria_label']}")
        except:
            print("   Could not retrieve button details")
        
        # Check visibility of coverage-related text in viewport
        print(f"\n👁️ VIEWPORT & RENDERING:")
        try:
            viewport_info = await page.evaluate("""
                () => {
                    return {
                        scrollHeight: document.documentElement.scrollHeight,
                        scrollWidth: document.documentElement.scrollWidth,
                        clientHeight: window.innerHeight,
                        clientWidth: window.innerWidth,
                        scrollTop: window.scrollY,
                        scrollLeft: window.scrollX
                    };
                }
            """)
            
            print(f"  Viewport: {viewport_info['clientWidth']}x{viewport_info['clientHeight']}")
            print(f"  Document Size: {viewport_info['scrollWidth']}x{viewport_info['scrollHeight']}")
            print(f"  Current Scroll: ({viewport_info['scrollLeft']}, {viewport_info['scrollTop']})")
        except:
            print("   Could not get viewport info")
        
    except Exception as e:
        print(f"   Debug analysis failed: {e}")
    
    print("="*70 + "\n")



async def _capture_coverage_response(resp):
    global _captured_response_data, _response_event
    try:
        url = resp.url.lower()

        if "quote/plandetails" in url and resp.request.method == "POST":
            print("✅ Found plandetails response!")

            # ✅ Get response JSON
            try:
                data = await resp.json()
            except Exception as e:
                print(f"❌ Failed to decode JSON: {e}")
                data = None

            if not data:
                print("⚠️ No data in response")
                return

            # ✅ Get request payload properly
            request = resp.request
            post_data = request.post_data
            post_data_json = request.post_data_json

            print("POST RAW:", post_data)
            print("POST JSON:", post_data_json)

            _captured_response_data = data

            _captured_response_data["plan_id_data"] = {
                "planId": post_data_json.get("planId") if post_data_json else None,
                "addonComboId": post_data_json.get("addonComboId") if post_data_json else None
            }

            if _response_event:
                _response_event.set()

    except Exception as e:
        print(f"❌ Error in capture_response: {e}")


async def _setup_coverage_response_handler(page: Page):
    """
    Setup response handler to capture planDetails API responses.
    Registers a handler that will capture coverage data when View Coverage is clicked.
    """
    global _captured_response_data, _response_event

    print("📋 Setting up coverage response handler...")
    _response_event = asyncio.Event()
    _captured_response_data = None
    

    def response_handler(resp):
        asyncio.create_task(_capture_coverage_response(resp))
    
    page.on("response", response_handler)
    print(" Response handler registered")


async def retry_failed_coverages(
    page: Page,
    failed_indices: list[int],
    view_buttons,
    max_retries: int = 1
) -> list[dict | None]:
    """
    Retry coverage extraction for buttons that failed in the main loop.
    
    Args:
        page:           Playwright Page object
        failed_indices: List of button indices (0-based) that failed
        view_buttons:   The original Playwright Locator for all View Coverage buttons
        max_retries:    How many times to retry each failed button (default: 1)
    
    Returns:
        List of dicts with {"index": idx, "data": extracted_data | None}
    """
    global _captured_response_data, _response_event


    if not failed_indices:
        print("  No failed coverages to retry.")
        return []


    print(f"\n{'='*60}")
    print(f"RETRYING {len(failed_indices)} FAILED COVERAGE(S): {failed_indices}")
    print(f"{'='*60}")


    retry_results = []


    for idx in failed_indices:
        extracted_data = None
        attempt = 0


        while attempt < max_retries and extracted_data is None:
            attempt += 1
            print(f"\n  → Retry attempt {attempt}/{max_retries} for button [{idx+1}]...")


            try:
                # ✅ Re-locate fresh on every attempt — avoids stale locator
                # after DOM changes caused by previous modal open/close cycles
                fresh_buttons = page.locator(".smallerFont", has_text="View Coverage")
                btn = fresh_buttons.nth(idx)

                # ✅ Dismiss any lingering overlay or ghost modal before clicking
                await page.keyboard.press("Escape")
                await human_delay(500, 800)

                # Close any visible modal explicitly before attempting click
                try:
                    close_btn = page.locator(".policyDetailPopup .crossBtn").first
                    if await close_btn.is_visible(timeout=1500):
                        await close_btn.click()
                        await human_delay(500, 800)
                except Exception:
                    pass

                # Reset globals for this attempt
                _response_event = asyncio.Event()
                _captured_response_data = None

                # ✅ Scroll into view before clicking
                await btn.scroll_into_view_if_needed()
                await human_delay(400, 600)

                print(f"     Clicking 'View Coverage' button [{idx+1}]...")
                # ✅ force=True bypasses Playwright actionability checks
                # (visible / stable / not-obscured) which caused the 30s timeout
                # timeout=5000 — fail fast so next attempt starts quickly
                await btn.click(force=True, timeout=5000)
                await human_delay(500, 800)


                # Wait for modal
                try:
                    # ✅ timeout=10000 — reduced from 30000 for faster failure detection
                    await page.wait_for_selector(".policyDetailPopup", timeout=10000)
                    print(f"     Coverage modal opened")
                except Exception as e:
                    print(f"     Modal open failed: {str(e)[:80]}")
                    # Try closing any stale state before next attempt
                    await page.keyboard.press("Escape")
                    await human_delay(1000, 1500)
                    continue


                # Wait for API response
                try:
                    await asyncio.wait_for(_response_event.wait(), timeout=10)
                    print(f"     API response captured")
                except asyncio.TimeoutError:
                    print(f"     API response timed out on retry {attempt}")


                # Extract if we got data
                if _captured_response_data:
                    extracted_data = {
                        "plan_id": _captured_response_data.get("data", {}).get("planId"),
                        "insurer": _captured_response_data.get("data", {}).get("insurerName"),
                        "premium_breakup": _captured_response_data.get("data", {}).get("premiumBreakup"),
                        "coverage_details": _captured_response_data.get("data", {}).get("coverageDetails"),
                    }
                    print(f"     Extracted plan: {extracted_data.get('plan_id')}")
                else:
                    print(f"     Still no data on attempt {attempt}")


            except Exception as e:
                print(f"     Exception on retry {attempt} for [{idx+1}]: {str(e)[:80]}")
                # ✅ Always dismiss before next attempt to clear any stuck state
                try:
                    await page.keyboard.press("Escape")
                    await human_delay(800, 1200)
                except Exception:
                    pass


            finally:
                # Always try to close modal cleanly after every attempt
                try:
                    close_btn = page.locator(".policyDetailPopup .crossBtn").first
                    if await close_btn.is_visible():
                        await close_btn.click()
                    else:
                        await page.keyboard.press("Escape")
                    await human_delay(1000, 1500)
                except Exception:
                    await page.keyboard.press("Escape")
                    await human_delay(1000, 1500)


        retry_results.append({"index": idx, "data": extracted_data})


        if extracted_data is None:
            print(f"  ✗ Button [{idx+1}] failed after {max_retries} retry attempt(s)")
        else:
            print(f"  ✓ Button [{idx+1}] recovered successfully")


    recovered = sum(1 for r in retry_results if r["data"] is not None)
    print(f"\n  Retry complete: {recovered}/{len(failed_indices)} recovered")


    return retry_results


async def step_14_extract_plan_details_and_coverage(page: Page):
    """
    STEP 14: Extract plan details and coverage from View Coverage popups.
    Follows the exact flow from pb_planDetails.py
    """
    global _captured_response_data, _response_event



    await _setup_coverage_response_handler(page)

    print("\n" + "=" * 70)
    print("STEP 14: EXTRACT PLAN DETAILS & COVERAGE")
    print("=" * 70)

    # -------------------------------------------------------------------------
    # Inner utilities
    # -------------------------------------------------------------------------

    async def close_popup(check_first: bool = False):
        """Close the plan-details popup, falling back to Escape if needed."""
        close_btn = page.locator(".policyDetailPopup .crossBtn").first

        if check_first and not await close_btn.is_visible():
            return

        print("  → Closing coverage modal...")
        try:
            if await close_btn.is_visible():
                await close_btn.click()
            else:
                await page.keyboard.press("Escape")
            await human_delay(1000, 1500)
            print("   Modal closed")
        except Exception as e:
            print(f"   Error closing modal: {str(e)[:80]}")
            try:
                await close_btn.click()
                await human_delay(1000, 1500)
            except Exception:
                pass

    async def expand_all_plans():
        """Click every 'Show More' button so all plan cards are visible."""
        btns = page.locator(".showMoreBtn.width-100")
        count = await btns.count()
        print(f" Found {count} 'Show More' button(s).")
        for idx in range(count):
            btn = btns.nth(idx)
            try:
                await btn.scroll_into_view_if_needed()
                await btn.click()
                print(f" Clicked 'Show More' button #{idx + 1}")
                await page.wait_for_timeout(1500)
            except Exception as e:
                print(f" Failed to click 'Show More' button #{idx + 1}: {e}")

    async def locate_view_coverage_buttons():
        """
        Return a (locator, count) pair for 'View Coverage' buttons.
        Tries three strategies in order; returns the first that finds results.
        Falls back to 'Premium Breakup' button if no View Coverage buttons found.
        """
        strategies = [
            (".smallerFont",           "'.smallerFont' with text"),
            ("text=View Coverage",     "exact text match"),
            ("text=/view coverage/i",  "case-insensitive regex"),
            # --- Fallback: Premium Breakup ---
            (".smallerFont",           "'.smallerFont' Premium Breakup with text"),
            ("text=Premium Breakup",   "exact Premium Breakup text match"),
            ("text=/premium breakup/i","case-insensitive Premium Breakup regex"),
        ]
        for selector, label in strategies:
            try:
                if selector == ".smallerFont":
                    await page.wait_for_selector(selector, timeout=5000)
                    if "Premium Breakup" in label:
                        locator = page.locator(selector, has_text="Premium Breakup")
                    else:
                        locator = page.locator(selector, has_text="View Coverage")
                else:
                    locator = page.locator(selector)

                count = await locator.count()
                if count > 0:
                    print(f" Strategy ({label}): found {count} button(s)")
                    print(f" Using: {'Premium Breakup' if 'Premium Breakup' in label else 'View Coverage'} buttons")  # ✅ added here
                    return locator, count
            except Exception as e:
                print(f" Strategy ({label}) failed: {str(e)[:80]}")

        return None, 0



    async def wait_for_modal_and_api():
        """
        Click already-scrolled-into-view button, wait for popup + API response.
        Caller is responsible for resetting _response_event / _captured_response_data.
        """
        print("  → Waiting for coverage modal...")
        try:
            await page.wait_for_selector(".policyDetailPopup", timeout=30_000)
            print("   Coverage modal opened")
        except Exception as e:
            print(f"   Modal open timeout: {str(e)[:80]}")
            raise

        print("  → Waiting for planDetails API response...")
        try:
            await asyncio.wait_for(_response_event.wait(), timeout=10)
            print("   API response captured")
        except asyncio.TimeoutError:
            print("   API response timeout — continuing with whatever was captured")

    def extract_coverage_from_response() -> Optional[dict]:
        """Parse the globally captured API payload into a tidy dict."""
        if not _captured_response_data:
            return None
        data = _captured_response_data.get("data", {})
        return {
            "plan_id":        data.get("planId"),
            "insurer":        data.get("insurerName"),
            "premium_breakup": data.get("premiumBreakup"),
            "coverage_details": data.get("coverageDetails"),
        }

    async def process_single_button(btn, idx: int, total: int) -> Optional[dict]:
        """
        Handle the full lifecycle for one 'View Coverage' button:
        close any stale popup → click → wait → extract → close.
        Returns extracted data dict, or None on failure.
        """
        global _captured_response_data, _response_event

        print(f"\n Processing button [{idx + 1}/{total}]...")
        await close_popup(check_first=True)

        _response_event = asyncio.Event()
        _captured_response_data = None

        await btn.scroll_into_view_if_needed()
        await human_delay(300, 500)

        print("  → Clicking 'View Coverage' button...")
        await btn.click()
        await human_delay(500, 800)

        await wait_for_modal_and_api()

        extracted = extract_coverage_from_response()
        if extracted:
            print(f"   Coverage data extracted for plan: {extracted.get('plan_id')}")
        else:
            print("   No coverage data captured")

        await close_popup()
        return extracted

    # -------------------------------------------------------------------------
    # Main flow
    # -------------------------------------------------------------------------
    try:
        await expand_all_plans()

        print("\n Locating 'View Coverage' buttons...")
        view_buttons, total_buttons = await locate_view_coverage_buttons()
        print(f"\n Total 'View Coverage' buttons found: {total_buttons}")

        if total_buttons == 0:
            print(" No View Coverage buttons found on page")
            return None

        results: list[Optional[dict]] = []
        failed_indices: list[int] = []

        for idx in range(total_buttons):
            try:
                btn = view_buttons.nth(idx)
                data = await process_single_button(btn, idx, total_buttons)
                results.append(data)
                if data is None:
                    failed_indices.append(idx)
            except Exception as e:
                print(f"   Error processing button [{idx + 1}]: {str(e)[:80]}")
                results.append(None)
                failed_indices.append(idx)

        if failed_indices:
            print(f"\n  {len(failed_indices)} plan(s) failed, retrying...")
            retry_results = await retry_failed_coverages(page, failed_indices, view_buttons)
            for r in retry_results:
                results[r["index"]] = r["data"]

        print(f"\n Coverage extraction complete: {len(results)} plan(s) processed")
        return results or None

    except Exception as e:
        print(f" Step 14 error: {str(e)[:150]}")
        return None

# ================================
# MAIN ASYNC RUN FUNCTION
# ================================
async def run():

    global response_event, captured_response_data, EXECUTION_METADATA_LIST

    EXECUTION_METADATA_LIST = []
    response_event = asyncio.Event()
    captured_response_data = None

    async with Stealth().use_async(async_playwright()) as p:

        browser = await p.chromium.launch(
            headless=False,
            proxy=PROXY_SETTINGS,
        )

        context = await browser.new_context(
            locale="en-IN",
            timezone_id="Asia/Kolkata",
            ignore_https_errors=True,
        )

        page = await context.new_page()

        def response_handler(resp):
            asyncio.create_task(handle_response(resp, datadir="./responses", seen_responses=set()))

        page.on("response", response_handler)

        print("Opening page...")
        await page.goto(URL, wait_until="load", timeout=90000)

        await page.wait_for_timeout(5000)

        # CALL STEP 14
        results = await step_14_extract_plan_details_and_coverage(page)

        await browser.close()

        return results


def extract_addon_ids_from_payload(payload_str: str) -> set[int]:
    """
    Util: Parse the latest quotes request payload and extract
    already-selected addon filter IDs (sectionType == 'ADDONS').
    Returns a set of integer filterIds.
    """
    import json
    try:
        payload = json.loads(payload_str)
        filters = payload.get("filters", [])
        return {
            f["filterId"]
            for f in filters
            if f.get("sectionType") == "ADDONS"
        }
    except Exception as e:
        print(f"  → [extract_addon_ids_from_payload] Failed to parse payload: {e}")
        return set()


async def capture_latest_quotes_payload(page: Page) -> set[int]:
    """
    Util: Intercept the most recent quotes/quote API request and
    extract already-selected addon IDs from its POST body.
    Returns a set of already-selected addon filterIds.
    """

    captured = {"payload": None}

    async def handle_request(request: Request):
        global captured_addons
        if "quote/quotes" in request.url.lower() and request.method == "POST":
            try:
                captured["payload"] = request.post_data
                captured_addons = extract_addon_ids_from_payload(captured["payload"])
            except Exception:
                pass

    page.on("request", handle_request)

    # # Small wait to capture any in-flight requests triggered by page state
    # await page.wait_for_timeout(2000)
    # page.remove_listener("request", handle_request)

    if captured["payload"]:
        print(f"  → Captured quotes payload from network")
        return captured_addons
    else:
        print("  → No quotes payload captured, assuming no addons pre-selected")
        return set()


async def get_already_selected_addons(page: Page) -> set[str]:
    """
    Util: Extract already-selected addon names from the filterList.
    Clicks '+N More' button first if present to expand all selected filters.
    Returns a set of lowercased addon name strings.
    """
    print("  → Fetching already-selected addons from filterList...")

    filter_list = page.locator("ul.filterList")

    try:
        await filter_list.wait_for(state="visible", timeout=10000)
    except Exception:
        print("  → filterList not visible, assuming no addons pre-selected")
        return set()

    # Click "+N More" if present to reveal all selected filters
    more_btn = filter_list.locator("li div.tags.underline.extra")
    if await more_btn.count() > 0 and await more_btn.is_visible():
        print("  → Expanding filterList via '+N More' button...")
        await more_btn.click()
        await more_btn.wait_for(state="hidden", timeout=5000)
        print("  → filterList fully expanded")

    # Collect all tag spans (skip the "+N More" div, it's gone now)
    tag_spans = filter_list.locator("li div.tags.type2 span")
    total = await tag_spans.count()

    selected = set()
    for i in range(total):
        text = (await tag_spans.nth(i).inner_text()).strip().lower()
        if text:
            selected.add(text)

    print(f"  → Already selected filters: {selected}")
    return selected


async def step_13_3_select_allowed_addons(page: Page):
    """Step 13.3: Select all addons not already present in the filterList"""
    print("Step 13.3: Selecting allowed addons")

    # Get already-selected addon names from filterList
    already_selected = await get_already_selected_addons(page)

    filters_div = page.locator("div.quoteFilters.non-sticky.show")
    addons_section = filters_div.locator("label.accPanel").filter(
        has=page.locator("p:has-text('Addons')")
    )
    addon_section = addons_section.locator("div.accBody .inner")

    async def click_see_all_addons():
        # Click "See all" if present to expand hidden addons
        see_all_btn = addon_section.locator("div.viewAll button")
        if await see_all_btn.count() > 0 and await see_all_btn.is_visible():
            print("  → Expanding all addons via 'See all' button...")
            await see_all_btn.click()
            await human_delay(800, 1200)
            # Wait for the button to disappear or new checkboxes to load
            await see_all_btn.wait_for(state="hidden", timeout=5000)
            print("  → All addons expanded")


    await click_see_all_addons()
    try:
        await addon_section.wait_for(state="visible", timeout=20000)
        print("  → Waiting for addon checkboxes to fully render...")
        await human_delay(1200, 1600)

        labels = addon_section.locator(".customCheckbox label")
        total = await labels.count()

        if total == 0:
            await human_delay(1000, 1500)
            total = await labels.count()

        print(f"  → Total addon items found: {total}")
        await human_delay(600, 900)

        clicked = 0
        skipped = 0

        for i in range(total):
            label = labels.nth(i)
            square_locator = label.locator(".square")
            text_locator = label.locator("p.text.valignMiddle")

            if await text_locator.count() == 0:
                continue

            title = await text_locator.locator("span").nth(0).inner_text()
            title = title.strip()
            if "must buy" in title.lower():
                title = title.split("Must Buy")[0].strip()

            # Skip if already present in filterList (case-insensitive)
            if title.lower() in already_selected:
                print(f"  → Skipping (already in filterList): {title}")
                skipped += 1
                await human_delay(400, 600)
                continue

            print(f"  → Selecting addon: {title}")
            await human_delay(600, 900)
            await square_locator.click()

            # Popup check AFTER EACH ADDON
            await human_delay(800, 1200)
            # await handle_non_addon_popups(page)
            clicked += 1
            await click_see_all_addons()

        print(f"  → Total Addons Selected: {clicked}")
        await human_delay(600, 900)
        print(f"  → Already Selected (Skipped): {skipped}")
        print("  → Preparing to apply filters...")
        await human_delay(1000, 1500)

        # Final popup check before Apply
        # await handle_non_addon_popups(page)

    except Exception as e:
        print(f"  → Error selecting addons: {str(e)}")
        raise
# ================================
# SYNC WRAPPER FUNCTION
# ================================
def run_sync():
    """
    Sync callable function.
    Returns extracted plan data.
    """
    return asyncio.run(run())


# ================================
# ENTRY POINT
# ================================
if __name__ == "__main__":

    final_result = run_sync()

    print("\n==============================")
    print("FINAL RESULT:")
    print("==============================\n")
    print(final_result)

# ============================================================================
# STEP 15-16: CLOSE POPUPS & FINAL WAIT
# ============================================================================

async def step_15_close_quote_popup(page: Page):
    """Step 15: Check and close any remaining popups"""
    print("Step 15: Checking and closing any remaining popups")
    try:
        popup_container = page.locator(".inner.padding0")
        # Wait for popup to appear (with timeout)
        try:
            await popup_container.first.wait_for(state="visible", timeout=6000)
            print("  → Quote popup detected")
        except:
            print("  → No quote popup found (timeout)")
        
        if await popup_container.count() > 0:
            close_btn = popup_container.locator(".crossBtn")
            try:
                await close_btn.first.wait_for(state="visible", timeout=3000)
                await close_btn.first.click()
                await human_delay(400, 900)
                print("  → Quote popup closed successfully")
            except Exception as inner_e:
                print(f"  → Could not click close button: {str(inner_e)}")
    except Exception as e:
        print(f"  → Could not close quote popup: {str(e)}")
    
    await human_delay(800, 1500)


async def step_16_final_wait(page: Page, wait_duration: int = 60000):
    """Step 16: Final processing wait - Capturing delayed responses"""
    print("Step 16: Final processing wait - Capturing delayed responses")
    await page.wait_for_timeout(wait_duration)



# ============================================================================
# BACKGROUND MONITOR: Choose Coverage Popup Auto-Skip Handler
# ============================================================================





