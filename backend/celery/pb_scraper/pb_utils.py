import asyncio
import json
import random
from typing import Tuple, Set
from urllib.parse import urlparse
from playwright.async_api import Browser, BrowserContext, Page, Playwright, Response, expect

from backend.core.config import settings

# ========================
# PROXY CONFIG (OXYLABS)
# ========================
PROXY_SETTINGS = {
    "server": settings.PB_PROXY_SERVER,
    "username": settings.PB_PROXY_USERNAME,
    "password": settings.PB_PROXY_PASSWORD,
}

# ========================
# SAFE MOBILE STEALTH JS
# ========================
MOBILE_STEALTH_JS = """
// --- webdriver ---
Object.defineProperty(navigator, 'webdriver', {
  get: () => undefined
});

// --- touch support ---
Object.defineProperty(navigator, 'maxTouchPoints', {
  get: () => 5
});

// --- platform ---
Object.defineProperty(navigator, 'platform', {
  get: () => 'iPhone'
});

// --- vendor ---
Object.defineProperty(navigator, 'vendor', {
  get: () => 'Apple Computer, Inc.'
});

// --- languages ---
Object.defineProperty(navigator, 'languages', {
  get: () => ['en-IN', 'en']
});

// --- permissions (safe override) ---
const originalQuery = navigator.permissions.query;
navigator.permissions.query = (parameters) => (
  parameters.name === 'notifications'
    ? Promise.resolve({ state: Notification.permission })
    : originalQuery(parameters)
);

// --- storage estimate (Angular-safe) ---
Object.defineProperty(navigator, 'storage', {
  value: {
    estimate: async () => ({
      quota: 120000000,
      usage: 2000000
    })
  }
});

// --- matchMedia mobile fixes ---
const originalMatchMedia = window.matchMedia;
window.matchMedia = (query) => {
  if (query === '(pointer: coarse)') {
    return { matches: true, media: query, onchange: null };
  }
  return originalMatchMedia(query);
};
"""

# ========================
# ANTI-DETECTION JS (Desktop)
# ========================
ANTI_DETECTION_JS = """
// Remove webdriver property
Object.defineProperty(navigator, 'webdriver', {
    get: () => undefined
});

// Override the plugins property to use a custom getter
Object.defineProperty(navigator, 'plugins', {
    get: () => [1, 2, 3, 4, 5]
});

// Override the languages property
Object.defineProperty(navigator, 'languages', {
    get: () => ['en-US', 'en']
});

// Chrome runtime
window.chrome = {
    runtime: {}
};

// Permissions
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) => (
    parameters.name === 'notifications' ?
        Promise.resolve({ state: Notification.permission }) :
        originalQuery(parameters)
);

// Override getBattery if it exists
if (navigator.getBattery) {
    navigator.getBattery = () => Promise.resolve({
        charging: true,
        chargingTime: 0,
        dischargingTime: Infinity,
        level: 1
    });
}

// Canvas fingerprinting protection - add noise
const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
HTMLCanvasElement.prototype.toDataURL = function(type) {
    if (type === 'image/png' || type === undefined) {
        const context = this.getContext('2d');
        if (context) {
            const imageData = context.getImageData(0, 0, this.width, this.height);
            for (let i = 0; i < imageData.data.length; i += 4) {
                imageData.data[i] += Math.floor(Math.random() * 3) - 1;
            }
            context.putImageData(imageData, 0, 0);
        }
    }
    return originalToDataURL.apply(this, arguments);
};

// WebGL fingerprinting protection
const getParameter = WebGLRenderingContext.prototype.getParameter;
WebGLRenderingContext.prototype.getParameter = function(parameter) {
    if (parameter === 37445) {
        return 'Intel Inc.';
    }
    if (parameter === 37446) {
        return 'Intel Iris OpenGL Engine';
    }
    return getParameter.apply(this, arguments);
};

// AudioContext fingerprinting protection
if (window.AudioContext || window.webkitAudioContext) {
    const AudioContext = window.AudioContext || window.webkitAudioContext;
    const originalCreateAnalyser = AudioContext.prototype.createAnalyser;
    AudioContext.prototype.createAnalyser = function() {
        const analyser = originalCreateAnalyser.apply(this, arguments);
        const originalGetFloatFrequencyData = analyser.getFloatFrequencyData;
        analyser.getFloatFrequencyData = function(array) {
            originalGetFloatFrequencyData.apply(this, arguments);
            for (let i = 0; i < array.length; i++) {
                array[i] += Math.random() * 0.0001;
            }
        };
        return analyser;
    };
}

// Remove automation indicators
delete navigator.__proto__.webdriver;

// Override Notification permission
Object.defineProperty(Notification, 'permission', {
    get: () => 'default'
});

// Fix missing properties
if (!window.outerHeight) {
    window.outerHeight = window.innerHeight;
}
if (!window.outerWidth) {
    window.outerWidth = window.innerWidth;
}
"""

# Module-level state for JSON response tracking
_json_counter = {"count": 1}

def extract_api_name(url: str) -> str:
    """
    Convert URL path to DB-friendly API name.
    Example:
    /carapi/Quote/QuoteQuestions → carapi_Quote_QuoteQuestions
    """
    path = urlparse(url).path  # /carapi/Quote/QuoteQuestions
    parts = [p for p in path.split("/") if p]
    return "_".join(parts)
def build_mobile_context_kwargs(
    playwright: Playwright,
    device_name: str = "iPhone 15",
    use_proxy: bool = False,
) -> dict:
    """Return context kwargs with mobile defaults and optional proxy."""
    device = playwright.devices[device_name]
    context_kwargs = {
        **device,
        "ignore_https_errors": True,
        "locale": "en-IN",
        "timezone_id": "Asia/Kolkata",
        "service_workers": "block",
        "color_scheme": random.choice(["light", "dark"]),
        "reduced_motion": "no-preference",
    }

    if use_proxy:
        context_kwargs["proxy"] = PROXY_SETTINGS

    return context_kwargs


async def setup_mobile_page(
    playwright: Playwright,
    headless: bool = False,
    device_name: str = "iPhone 15",
    use_proxy: bool = False,
) -> Tuple[Browser, BrowserContext, Page]:
    """Create browser, context, and page with mobile stealth script injected."""
    browser = await playwright.chromium.launch(headless=headless)
    context = await browser.new_context(
        **build_mobile_context_kwargs(
            playwright=playwright,
            device_name=device_name,
            use_proxy=use_proxy,
        )
    )
    await context.add_init_script(MOBILE_STEALTH_JS)
    page = await context.new_page()
    return browser, context, page


async def collect_mobile_state(page: Page) -> dict:
    """Fetch a snapshot of the mobile-related navigator/window properties."""
    return await page.evaluate(
        """
        () => ({
          ua: navigator.userAgent,
          platform: navigator.platform,
          touch: navigator.maxTouchPoints,
          width: window.innerWidth,
          pointer: matchMedia('(pointer: coarse)').matches
        })
        """
    )


# ========================
# JSON & RESPONSE HANDLING
# ========================
def save_json(basedir: str, filename: str, data: dict) -> None:
    """Save JSON data to a file."""
    path = os.path.join(basedir, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

# ========================
# NETWORK ERROR DETECTION
# ========================
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

async def handle_response(
    response: Response, basedir: str, seen_responses: Set[str]
) -> None:
    """Handle and save JSON API responses."""
    try:
        if "policybazaar.com/carapi" not in response.url:
            return
        if "application/json" in (response.headers.get("content-type") or ""):
            try:
                    response_data = await response.json()
            except Exception:
                    return  # Ignore non-JSON or empty responses silently

            # Skip saving if status is 201
            if response.status == 201:
                return
            if not "/quote" in response.url.lower():
                return

            # Skip saving if data["data"] is None
            if (
                isinstance(response_data, dict)
                and response_data.get("data", "___MISSING___") is None
            ):
                return

            fname = f"api_response_{_json_counter['count']}.json"
            url = response.url

            print(f"JSON count: {_json_counter['count']}, URL: {url}")

            if url:
                seen_responses.add(url)

            save_json(
                basedir,
                fname,
                {"url": url, "status": response.status, "data": response_data},
            )

            _json_counter["count"] += 1

    except Exception as e:
        error_msg = str(e)
        error_type = "NETWORK_ERROR" if is_network_error(e) else "RESPONSE_PARSE_ERROR"

        print(
            f"⚠️ API response handling failed | "
            f"type={error_type} | "
            f"url={getattr(response, 'url', 'unknown')} | "
            f"error={error_msg}"
        )



# ========================
# TIMING UTILITIES
# ========================
async def human_delay(a: int = 300, b: int = 800) -> None:
    """Add a random human-like delay between a and b milliseconds."""
    await asyncio.sleep(random.uniform(a / 1000, b / 1000))


# ========================
# FORM INTERACTION UTILITIES
# ========================

async def select_radio_answer(page: Page, answer_text: str) -> None:
    """Select a radio button answer by text."""
    await page.locator(
        ".popupBox .popupBody .customRadioBtnV2 .radioBox p", has_text=answer_text
    ).click()


async def select_claim_answer(page: Page, claim_answer: str) -> None:
    """Select claim answer (Yes/No)."""
    try:
        print(f"selecting claim status : {claim_answer}")
        await select_radio_answer(page, claim_answer)
    except Exception as e:
        print(f"Error selecting claim answer: {e}")


async def select_expiry_answer(page: Page, policy_status: str) -> None:

    try:
        dont_know_locator = page.locator(
            "div.headingV4.textBlue.underline.fontMedium.text-center.cursorPointer.mb-8",
            has_text="Don't know policy expiry date?",
        )

        await dont_know_locator.wait_for(timeout=5000)

        if await dont_know_locator.count() > 0 and await dont_know_locator.is_visible():
            await dont_know_locator.click()
            await human_delay(500, 1200)

        # 🔍 DEBUG: list all visible policy expiry options
        print("DEBUG: Available policy expiry options:")
        options = page.locator(
            ".popupBox .popupBody .customRadioBtnV2 .radioBox p"
        )
        count = await options.count()
        for i in range(count):
            print(" -", (await options.nth(i).inner_text()).strip())

        print(f"selecting policy expiry status : {policy_status}")
        await select_radio_answer(page, policy_status)
        
    except Exception as e:
        print(f"Error selecting policy expiry answer: {e}")

async def wait_for_intent_modal(page):
    locator = page.locator("div#exit-intent-popup-container").first

    if await locator.is_visible(timeout=10000):
        close_btn = locator.locator("div#exit-intent-popup-close")
        if await close_btn.is_visible():
            await close_btn.click()


async def type_if_empty(input_elem, value: str) -> None:
    """Type with more human-like patterns including occasional mistakes."""
    await input_elem.clear()
    await input_elem.fill("")

    # Occasionally simulate backspace (typing mistake)
    mistake_chance = random.random()
    mistake_pos = random.randint(1, len(value) - 1) if len(value) > 2 else None

    for i, ch in enumerate(value):
        # Simulate a typing mistake
        if mistake_chance < 0.15 and mistake_pos and i == mistake_pos:
            wrong_char = random.choice("abcdefghijklmnopqrstuvwxyz0123456789")
            await input_elem.type(wrong_char, delay=random.randint(80, 170))
            await human_delay(100, 300)
            await input_elem.press("Backspace")
            await human_delay(150, 400)

        await input_elem.type(ch, delay=random.randint(80, 200))

        # Occasional longer pauses (thinking)
        if random.random() < 0.1:
            await human_delay(300, 800)


async def fill_details_and_view_prices(page: Page, name: str, phone: str) -> None:
    """Fill customer name and phone, then click View Prices."""

    name_input = page.locator("#txtName")
    await name_input.scroll_into_view_if_needed()

    if await name_input.input_value() != name:
        await type_if_empty(name_input, name)  

    await human_delay(500, 1200)

    phone_input = page.locator("#mobNumber")
    await phone_input.scroll_into_view_if_needed()
    if await phone_input.input_value() != phone:
        await type_if_empty(phone_input, phone) 

    await human_delay(700, 1500)

    # ✅ VALIDATION: Verify customer details were filled before proceeding
    # Wait for values to actually be set in the input fields
    max_retries = 3
    for retry in range(max_retries):
        filled_name = await name_input.input_value()
        filled_phone = await phone_input.input_value()
        
        if filled_name and filled_phone:
            print(f"   ✅ Name field validated: {filled_name}")
            print(f"   ✅ Phone field validated: {filled_phone}")
            print("   ✅ Customer details validated - proceeding to View Prices")
            break
        else:
            if retry < max_retries - 1:
                print(f"   ⏳ Retry {retry + 1}/{max_retries - 1}: Waiting for field values...")
                await human_delay(500, 800)
            else:
                raise Exception(
                    f"❌ STEP 9 FAILED: Customer details not filled properly!\n"
                    f"   Expected Name: {name}, Got: {filled_name}\n"
                    f"   Expected Phone: {phone}, Got: {filled_phone}\n"
                    f"   Cannot proceed to quotes page without customer details!"
                )

    await page.locator("div.button.btnOrange:has-text('View Prices')").click()


# ========================
# MOUSE MOVEMENT UTILITIES
# ========================
async def bezier_mouse_move(
    page: Page, start_x: float, start_y: float, end_x: float, end_y: float
) -> None:
    """Move mouse using Bezier curve for more human-like movement."""
    # Control points for Bezier curve (creates natural arc)
    cp1_x = (
        start_x + (end_x - start_x) * random.uniform(0.2, 0.4) + random.uniform(-50, 50)
    )
    cp1_y = (
        start_y + (end_y - start_y) * random.uniform(0.2, 0.4) + random.uniform(-50, 50)
    )
    cp2_x = (
        start_x + (end_x - start_x) * random.uniform(0.6, 0.8) + random.uniform(-50, 50)
    )
    cp2_y = (
        start_y + (end_y - start_y) * random.uniform(0.6, 0.8) + random.uniform(-50, 50)
    )

    steps = random.randint(15, 35)
    for i in range(steps + 1):
        t = i / steps
        # Cubic Bezier curve formula
        x = (
            (1 - t) ** 3 * start_x
            + 3 * (1 - t) ** 2 * t * cp1_x
            + 3 * (1 - t) * t**2 * cp2_x
            + t**3 * end_x
        )
        y = (
            (1 - t) ** 3 * start_y
            + 3 * (1 - t) ** 2 * t * cp1_y
            + 3 * (1 - t) * t**2 * cp2_y
            + t**3 * end_y
        )
        await page.mouse.move(x, y)
        await asyncio.sleep(random.uniform(0.005, 0.015))


async def random_human_noise(page: Page) -> None:
    """Add more realistic human-like behavior patterns."""
    # Random scrolls with variable speeds
    for _ in range(random.randint(1, 3)):
        scroll_amount = random.randint(150, 500)
        scroll_steps = random.randint(3, 8)
        step_size = scroll_amount / scroll_steps
        for _ in range(scroll_steps):
            await page.mouse.wheel(0, step_size + random.uniform(-10, 10))
            await human_delay(50, 150)
        await human_delay(400, 1200)

    # More natural mouse movements using Bezier curves
    box = await page.locator("body").bounding_box()
    if box:
        current_x = random.uniform(box["x"] + 10, box["x"] + box["width"] - 10)
        current_y = random.uniform(box["y"] + 10, box["y"] + box["height"] - 10)

        for _ in range(random.randint(2, 4)):
            target_x = random.uniform(box["x"] + 10, box["x"] + box["width"] - 10)
            target_y = random.uniform(box["y"] + 10, box["y"] + box["height"] - 10)
            await bezier_mouse_move(page, current_x, current_y, target_x, target_y)
            current_x, current_y = target_x, target_y
            await human_delay(200, 700)


# ========================
# CHALLENGE DETECTION
# ========================
async def wait_for_akamai_challenge(page: Page, timeout: int = 30000) -> bool:
    """Wait for and detect Akamai Bot Manager challenges / block pages."""
    try:
        challenge_selectors = [
            "iframe[src*='akamai']",
            "div[id*='challenge']",
            "div[class*='challenge']",
            "div[id*='akamai']",
            "div[class*='akamai']",
        ]

        end = asyncio.get_event_loop().time() + (timeout / 1000)
        detected = False

        while asyncio.get_event_loop().time() < end:
            # 1) DOM‑based challenge hints
            for selector in challenge_selectors:
                try:
                    element = page.locator(selector).first
                    if await element.is_visible(timeout=1000):
                        print(f"⚠️ Akamai challenge element visible: {selector}")
                        detected = True
                        # Give JS challenge time to run
                        await page.wait_for_timeout(random.randint(5000, 10000))
                        # If it disappears, assume solved
                        if not await element.is_visible(timeout=1000):
                            print("✅ Akamai challenge element gone (likely solved)")
                            return True
                except Exception:
                    continue

            # 2) URL‑based hints (redirect to challenge / blocked page)
            url = page.url.lower()
            if any(p in url for p in ["akam", "akamai", "bot-challenge", "challenge"]):
                print(f"⚠️ Possible Akamai challenge URL: {url}")
                detected = True
                await page.wait_for_timeout(random.randint(5000, 10000))
                # Re-check after wait
                return True

            # 3) Body text‑based block detection
            try:
                body_text = (await page.text_content("body") or "").lower()
                block_markers = [
                    "access denied",
                    "request blocked",
                    "you don't have permission to access",
                    "reference #",
                ]
                if any(m in body_text for m in block_markers):
                    print("⚠️ Akamai block page detected via text markers")
                    return True
            except Exception:
                pass

            await asyncio.sleep(0.5)

        if detected:
            print("⚠️ Akamai challenge suspected but not clearly resolved in time")
        return detected
    except Exception as e:
        print(f"Challenge detection error: {e}")
        return False


# ========================
# POPUP HANDLING UTILITIES
# ========================
async def close_popupbody_popup(page: Page) -> None:
    """Close popup with crossBtn."""
    try:
        await human_delay(500, 1000)

        inner = page.locator(".inner.padding0 .crossBtn")
        if await inner.count() > 0:
            await inner.click()
            await human_delay(500, 1000)
            return

        await human_delay(500, 1000)
        body = page.locator(".popupBody.padding0 .crossBtn")
        if await body.count() > 0:
            await body.click()
            await human_delay(500, 1000)
            return
    except:
        pass


async def close_urgent_popup(page: Page) -> None:
    """Close urgent popup."""
    try:
        await human_delay(500, 1000)
        btn = page.locator("button[aria-label='Close']")
        if await btn.count() > 0:
            await btn.click()
            await human_delay(500, 1000)
    except:
        pass


async def close_car_question_popup(page: Page) -> None:
    """Close car question popup continue button."""
    try:
        await human_delay(500, 1000)

        # Desktop view
        fade = page.locator(
            ".popupBox.carQuestionPopup.fadeIn button.primaryBtnV2.width-100.fontMedium"
        )
        if await fade.count() > 0:
            await fade.click()
            await human_delay(500, 1000)
            return

        await human_delay(500, 1000)
        # iPhone view
        slide = page.locator(
            ".popupBox.carQuestionPopup.slideToTop button.primaryBtnV2.width-100.fontMedium"
        )
        if await slide.count() > 0:
            await slide.click()
            await human_delay(500, 1000)
    except:
        pass


async def close_quote_popup(page: Page) -> None:
    """Close quote popup with crossBtn. Wait for inner.padding0."""
    try:
        await human_delay(500, 1000)

        inner = page.locator(".inner.padding0")
        if await inner.count() > 0:
            await inner.wait_for(state="visible", timeout=5000)
            close_btn = inner.locator(".crossBtn")
            if await close_btn.count() > 0:
                await close_btn.click()
                await human_delay(500, 1000)
                return

        await human_delay(500, 1000)
        body = page.locator(".popupBody.padding0 .crossBtn")
        if await body.count() > 0:
            await body.click()
            await human_delay(500, 1000)
    except:
        pass


# ========================
# ADDONS SELECTION & FILTERS
# ========================


async def select_allowed_addons(page: Page, allowed_addons: set) -> tuple[int, int]:
    """
    Select only allowed addons from the popup and apply filters.
    Returns (clicked_count, skipped_count)
    """
    if page.is_closed():
        raise Exception("❌ Page closed unexpectedly!")

    # Open ADDONS popup if not already open
    if not await page.locator(".popupWrapper").is_visible():
        await page.locator("p.smallerFont:has-text('ADDONS')").first.evaluate(
            "el => el.click()"
        )
        await human_delay(500, 1000)

    await page.wait_for_selector(".popupBody.padding0")
    popup = page.locator(".popupBody.padding0")

    # Click Addons tab
    addons_tab = popup.locator(".tabItem:has-text('Addons')")
    await addons_tab.evaluate("el => el.click()")
    await human_delay(1500, 2000)

    # Get addon checkboxes
    labels = popup.locator(".cssTabContent .customCheckbox label")
    total = await labels.count()
    print(f"\nTotal checkbox items found: {total}")

    clicked = 0
    skipped_already_checked = 0

    for i in range(total):
        label = labels.nth(i)
        text_locator = label.locator(".text.valignMiddle")
        square_locator = label.locator(".square")

        if await text_locator.count() == 0:
            continue

        title = (await text_locator.inner_text()).strip()

        if title in allowed_addons:
            # Check if already checked
            is_checked = await label.locator("input[type='checkbox']").is_checked()

            if is_checked:
                print(f"Already checked → Skipping: {title}")
                skipped_already_checked += 1
                continue

            print(f"Clicking allowed addon: {title}")
            await square_locator.click()
            await human_delay(600, 900)
            clicked += 1
        else:
            print(f"Skipping: {title}")

    print(f"\n✅ Total Allowed Addons Clicked: {clicked}")
    print(f"☑️ Already Checked (Skipped): {skipped_already_checked}")

    # Let DOM settle
    await human_delay(800, 1200)

    # Click Apply Filters button
    if page.is_closed():
        raise Exception("❌ Page closed unexpectedly!")

    footer = page.locator("div.popupFooter")
    await footer.wait_for(state="visible", timeout=15000)

    apply_button = footer.locator("div.row div.col div.primaryBtnV2.m0")
    count = await apply_button.count()
    if count == 0:
        raise Exception("❌ Apply Filters button not found!")

    print("Clicking Apply Filters...")
    try:
        await apply_button.first.scroll_into_view_if_needed()
        await apply_button.first.click(timeout=5000)
    except Exception as e:
        print(f"Normal click failed ({e}), trying force click...")
        await apply_button.first.click(force=True)

    print("✅ Apply Filters clicked successfully.")

    # Wait for filters to apply and page to update
    await human_delay(5000, 6000)
    print("Waiting for page to update...")
    await human_delay(3000, 4000)

    return clicked, skipped_already_checked
