# bot_worker.py
import asyncio
import json
import re
import logging
import urllib.parse
import os
import time
import random
from datetime import date, datetime, timedelta, timezone
# from collections import deque # No longer needed for the queue
import argparse # Import argparse

# --- Third-Party Libraries ---
import tweepy
from playwright.async_api import async_playwright
from dotenv import load_dotenv

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="Twitter Correction Bot Worker")
parser.add_argument("bot_id", choices=['grammar', 'english'], help="Identifier for the bot type ('grammar' or 'english')")
args = parser.parse_args()
BOT_ID = args.bot_id # Will be 'grammar' or 'english'
# --- End Argument Parsing ---


# --- Configuration & Constants ---
load_dotenv()

# Control Log Verbosity
DEBUG_MODE = os.getenv("DEBUG_MODE", "True").lower() in ("true", "1", "t")
LOG_LEVEL = logging.DEBUG if DEBUG_MODE else logging.INFO

# --- Load Credentials Based on BOT_ID ---
API_KEY_VAR = f"API_KEY_{BOT_ID.upper()}"
API_SECRET_VAR = f"API_SECRET_{BOT_ID.upper()}"
BEARER_TOKEN_VAR = f"BEARER_TOKEN_{BOT_ID.upper()}"
ACCESS_TOKEN_VAR = f"ACCESS_TOKEN_{BOT_ID.upper()}"
ACCESS_TOKEN_SECRET_VAR = f"ACCESS_TOKEN_SECRET_{BOT_ID.upper()}"

CREDENTIALS = {
    "api_key": os.getenv(API_KEY_VAR),
    "api_secret": os.getenv(API_SECRET_VAR),
    "bearer_token": os.getenv(BEARER_TOKEN_VAR),
    "access_token": os.getenv(ACCESS_TOKEN_VAR),
    "access_token_secret": os.getenv(ACCESS_TOKEN_SECRET_VAR),
}
# --- End Credential Loading ---

# Operational Parameters
DAILY_CORRECTION_LIMIT = int(os.getenv(f"DAILY_LIMIT_{BOT_ID.upper()}", 15))
SCRAPER_TIMEOUT_S = 120
TWEETER_TIMEOUT_S = 60
MAX_INTERVAL_JITTER_S = 300
MIN_SLEEP_BETWEEN_CYCLES_S = 60
SECONDS_IN_DAY = 24 * 60 * 60
MAX_TWEET_AGE_DAYS = 2
SCRAPE_MAX_TWEETS = 30
MIN_ENGAGEMENT_QUERY = os.getenv(f"MIN_ENGAGEMENT_{BOT_ID.upper()}", "(min_retweets:50 OR min_faves:100)")
NITTER_INSTANCES = [
    "https://nitter.net",
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
    "https://nitter.cz",
]

# --- Persistent Queue Configuration ---
MAX_PROCESSED_QUEUE_SIZE = 200 # Max items to keep in the recent history file
PERSISTENT_QUEUE_DIR = "/app/temp_data" # Directory mapped to the Docker volume
PERSISTENT_QUEUE_FILENAME = os.path.join(PERSISTENT_QUEUE_DIR, f"processed_queue_{BOT_ID}.json")
# --- End Persistent Queue Configuration ---

# --- Error Pairs Definition (Select based on BOT_ID) ---
ERROR_PAIRS_GRAMMAR = [
    ("انشاء الله", "إن شاء الله"),
    ("إنشاء الله", "إن شاء الله"),
    ("لاكن", "لكن"),
    ("ضلم", "ظلم"),
    ("ضالم", "ظالم"),
    ("خطاء", "خطأ"),
    ("هاذا", "هذا"),
]

ERROR_PAIRS_ENGLISH = [
    ("ميتنج", "اجتماع"),
    ("ميتنغ", "اجتماع"),
    ("ميتنق", "اجتماع"),
    ("انفايت", "دعوة"),
    ("إنفايت", "دعوة"),
    ("انڤايت", "دعوة"),
    ("إنڤايت", "دعوة"),
    ("إيڤينت", "حدث"),
    ("ايڤينت", "حدث"),
    ("اڤينت", "حدث"),
    ("ايڤنت", "حدث"),
    ("ايفنت", "حدث"),
    ("إيفنت", "حدث"),
    ("إيفينت", "حدث"),
    ("إفينت", "حدث"),
    ("افينت", "حدث"),
    ("برفكت", "مثالي"),
    ("بيرفكت", "مثالي"),
    ("بيرفيكت", "مثالي"),
    ("برفيكت", "مثالي"),
    ("بروجكت", "مشروع"),
    ("داتا", "بيانات"),
    ("الداتا", "البيانات"),
]

ERROR_PAIRS = ERROR_PAIRS_GRAMMAR if BOT_ID == 'grammar' else ERROR_PAIRS_ENGLISH
# --- End Error Pairs ---

# --- Logging Setup ---
log_filename = f"bot_log_{BOT_ID}_{date.today().strftime('%Y-%m-%d')}.log"
logging.basicConfig(
    level=LOG_LEVEL,
    format=f'%(asctime)s - %(levelname)s - [{BOT_ID.upper()}] - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(f"bot_worker.{BOT_ID}")
log.info(f"[{BOT_ID.upper()}] Logging initialized. Level: {logging.getLevelName(LOG_LEVEL)}")
# --- End Logging Setup ---

# --- Ensure Queue Directory Exists ---
try:
    os.makedirs(PERSISTENT_QUEUE_DIR, exist_ok=True)
    log.info(f"[{BOT_ID.upper()}] Ensured persistent queue directory exists: {PERSISTENT_QUEUE_DIR}")
except OSError as e:
    log.critical(f"[{BOT_ID.upper()}] Could not create persistent queue directory {PERSISTENT_QUEUE_DIR}: {e}. Exiting.")
    exit(1)
# --- End Directory Check ---

# --- Global RAM Queue Variables ---
# REMOVED - We now use the persistent file
# processed_queue = deque(maxlen=MAX_PROCESSED_QUEUE_SIZE)
# processed_ids_set = set()
log.info(f"[{BOT_ID.upper()}] Using persistent processed queue file: {PERSISTENT_QUEUE_FILENAME} (max size: {MAX_PROCESSED_QUEUE_SIZE}).")
# --- End Global Queue Variables ---


# --- Tweepy Client Initialization ---
# (No changes needed here)
tweepy_client = None
if not all(CREDENTIALS.values()):
    missing_keys = [k for k, v in CREDENTIALS.items() if not v]
    env_vars_needed = [f"{k.upper()}_{BOT_ID.upper()}" for k in missing_keys]
    log.critical(f"[{BOT_ID.upper()}] Twitter API credentials missing! Check .env file for: {', '.join(env_vars_needed)}. Exiting.")
    exit(1)
try:
    tweepy_client = tweepy.Client(
        bearer_token=CREDENTIALS["bearer_token"],
        consumer_key=CREDENTIALS["api_key"],
        consumer_secret=CREDENTIALS["api_secret"],
        access_token=CREDENTIALS["access_token"],
        access_token_secret=CREDENTIALS["access_token_secret"],
        wait_on_rate_limit=True,
    )
    auth_user = tweepy_client.get_me()
    log.info(f"[{BOT_ID.upper()}] Tweepy Client (v2) initialized successfully for @{auth_user.data.username}")
except tweepy.errors.TweepyException as e:
    log.critical(f"[{BOT_ID.upper()}] Failed to initialize Tweepy client: {e}", exc_info=True)
    exit(1)
except Exception as e:
    log.critical(f"[{BOT_ID.upper()}] Unexpected error during Tweepy client initialization: {e}", exc_info=True)
    exit(1)
# --- End Tweepy Client Initialization ---


# --- Helper Functions ---
# (load_json_file and save_json_file are crucial now)

def load_json_file(filename, default=None):
    """Safely loads a JSON file, returning default if missing or invalid."""
    try:
        # Ensure directory exists before trying to read (might be redundant but safe)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "r", encoding="utf-8") as f:
            content = f.read()
            if not content: # Handle empty file case
                 log.debug(f"[{BOT_ID.upper()}] File is empty: {filename}. Returning default.")
                 return default
            return json.loads(content)
    except FileNotFoundError:
        log.debug(f"[{BOT_ID.upper()}] File not found: {filename}. Returning default.")
        return default
    except json.JSONDecodeError:
        log.warning(f"[{BOT_ID.upper()}] Invalid JSON in file: {filename}. Returning default.")
        # Optionally: Backup corrupt file here before returning default
        return default
    except Exception as e:
        log.error(f"[{BOT_ID.upper()}] Error loading {filename}: {e}. Returning default.", exc_info=DEBUG_MODE)
        return default

def save_json_file(filename, data):
    """Safely saves data to a JSON file, returning True on success, False otherwise."""
    try:
        # Ensure directory exists before trying to write
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        log.debug(f"[{BOT_ID.upper()}] Successfully saved data to {filename}")
        return True
    except (IOError, OSError, Exception) as e: # Catch broader OS errors too
        log.error(f"[{BOT_ID.upper()}] Error writing to {filename}: {e}", exc_info=DEBUG_MODE)
        return False


def get_corrections_made_today():
    """Gets the list of corrected tweet IDs from today's log file (specific to this bot instance)."""
    # (No changes needed here, already uses BOT_ID)
    today_str = date.today().strftime("%Y-%m-%d")
    corrections_file = f"{PERSISTENT_QUEUE_DIR}/corrections_{BOT_ID}_{today_str}.json"
    corrections_list = load_json_file(corrections_file, default=[]) # Use our robust loader

    if isinstance(corrections_list, list):
        valid_ids = [str(item) for item in corrections_list if isinstance(item, (str, int))]
        if len(valid_ids) != len(corrections_list):
            log.warning(f"[{BOT_ID.upper()}] Corrections file {corrections_file} contained non-ID items. Cleaned list.")
        return valid_ids
    else:
        log.warning(f"[{BOT_ID.upper()}] Corrections file {corrections_file} contained invalid data type (expected list). Resetting.")
        if save_json_file(corrections_file, []): # Use our robust saver
             log.info(f"[{BOT_ID.upper()}] Successfully reset {corrections_file} to an empty list.")
        else:
             log.error(f"[{BOT_ID.upper()}] Failed to reset corrupt corrections file {corrections_file}.")
        return []

# (extract_number and parse_tweet_timestamp remain unchanged)
def extract_number(text): # ... (same as before) ...
    if not text: return 0
    text = text.replace(",", "").strip()
    match = re.search(r"([\d.]+)([KM]?)", text, re.IGNORECASE)
    if not match: return 0
    try:
        num = float(match.group(1))
        suffix = match.group(2).upper() if match.group(2) else ""
        if suffix == "K": return int(num * 1000)
        if suffix == "M": return int(num * 1000000)
        return int(num)
    except ValueError:
        return 0

def parse_tweet_timestamp(timestamp_str): # ... (same as before) ...
    try:
        timestamp_str = re.sub(r'\s+', ' ', timestamp_str).strip()
        parts = timestamp_str.split('·')
        if len(parts) != 2: raise ValueError("Timestamp format incorrect: Missing '·' separator")
        date_part = parts[0].strip()
        time_part_full = parts[1].strip()
        time_parts = time_part_full.split(' ')
        if len(time_parts) < 2: raise ValueError("Timestamp format incorrect: Missing time or timezone")
        timezone_str = time_parts[-1].upper()
        time_value_str = " ".join(time_parts[:-1])
        dt_str = f"{date_part} {time_value_str}"
        tweet_time_naive = datetime.strptime(dt_str, "%b %d, %Y %I:%M %p")
        if timezone_str == "UTC":
            return tweet_time_naive.replace(tzinfo=timezone.utc)
        else:
            log.warning(f"[{BOT_ID.upper()}] Non-UTC timezone '{timezone_str}' detected: '{timestamp_str}'. Assuming UTC.")
            return tweet_time_naive.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError, IndexError) as e:
        log.debug(f"[{BOT_ID.upper()}] Could not parse timestamp '{timestamp_str}'. Error: {e}")
        return None
# --- End Helper Functions ---


# --- Core Function 1: Scraper ---
# (_extract_tweet_data_async and scrape_tweets remain unchanged)
async def _extract_tweet_data_async(item, error_pairs, connected_instance_url): # ... (same logic) ...
    try:
        tweet_link_element = await item.query_selector("a.tweet-link")
        username_element = await item.query_selector("a.username")
        timestamp_element = await item.query_selector("span.tweet-date a")
        tweet_text_element = await item.query_selector("div.tweet-content")

        if not all([tweet_link_element, username_element, timestamp_element, tweet_text_element]):
            log.debug("Skipping item: Missing essential elements.")
            return None

        tweet_link_raw = await tweet_link_element.get_attribute("href")
        tweet_link = urllib.parse.urljoin(connected_instance_url, tweet_link_raw) if tweet_link_raw else None
        tweet_id_match = re.search(r"/(?:status|statuses)/(\d+)", tweet_link) if tweet_link else None
        tweet_id = tweet_id_match.group(1) if tweet_id_match else None
        if not tweet_id:
            log.debug(f"Skipping item: Could not extract tweet ID from link '{tweet_link}'.")
            return None

        username = (await username_element.inner_text()).strip()
        timestamp_str = (await timestamp_element.get_attribute("title") or await timestamp_element.inner_text()).strip()
        tweet_text = (await tweet_text_element.inner_text()).strip()

        if tweet_text.startswith("RT @"):
            log.debug(f"Skipping tweet {tweet_id}: Retweet.")
            return None

        found_error = next(
            (
                {"incorrect": incorrect, "correct": correct}
                for incorrect, correct in error_pairs
                if re.search(r"\b" + re.escape(incorrect) + r"\b", tweet_text, re.IGNORECASE | re.UNICODE)
            ),
            None,
        )
        if not found_error: return None

        replies, retweets, likes, quotes = 0, 0, 0, 0
        stats_elements = await item.query_selector_all("div.tweet-stats .tweet-stat")
        for stat_element in stats_elements:
            try:
                icon_container = await stat_element.query_selector("div.icon-container")
                if icon_container:
                    stat_text = await icon_container.inner_text()
                    stat_value = extract_number(stat_text)
                    icon = await icon_container.query_selector("span[class^='icon-']")
                    if icon:
                        icon_class = await icon.get_attribute("class") or ""
                        if any(k in icon_class for k in ["comment", "reply"]): replies = stat_value
                        elif "retweet" in icon_class: retweets = stat_value
                        elif any(k in icon_class for k in ["heart", "like", "favorite"]): likes = stat_value
                        elif "quote" in icon_class: quotes = stat_value
            except Exception as e:
                log.debug(f"Minor error extracting stat for {tweet_id}: {e}")

        parsed_timestamp = parse_tweet_timestamp(timestamp_str)
        if not parsed_timestamp:
             log.debug(f"Skipping tweet {tweet_id}: Invalid timestamp '{timestamp_str}'.")
             return None

        return {
            "username": username, "timestamp_str": timestamp_str, "parsed_timestamp": parsed_timestamp,
            "tweet": tweet_text, "link": tweet_link, "tweet_id": tweet_id,
            "error_found": found_error,
            "engagement": {"replies": replies, "retweets": retweets, "likes": likes, "quotes": quotes},
        }
    except Exception as e:
        log.warning(f"Error processing a tweet element: {e}", exc_info=DEBUG_MODE)
        return None

async def scrape_tweets(error_pairs_to_use): # ... (same logic) ...
    log.info("Starting tweet scraping process...")
    incorrect_words_query = " OR ".join([f'"{pair[0]}"' for pair in error_pairs_to_use])
    base_query = f"({incorrect_words_query}) {MIN_ENGAGEMENT_QUERY} lang:ar -filter:retweets -filter:replies"
    encoded_query = urllib.parse.quote(base_query)
    search_url_template = "/search?f=tweets&q={query}&since=&until=&near="
    log.info(f"Constructed base query: {base_query}")

    fetched_tweets = []
    processed_tweet_ids_this_scrape = set()
    connected_instance = None

    async with async_playwright() as p:
        try:
            browser = await p.firefox.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
                java_script_enabled=True,
            )
            await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            page = await context.new_page()

            for instance in NITTER_INSTANCES:
                search_url = instance + search_url_template.format(query=encoded_query)
                log.info(f"Trying Nitter instance: {instance}")
                try:
                    await page.goto(search_url, timeout=30000, wait_until="domcontentloaded")
                    await page.wait_for_selector("div.timeline .timeline-item, div.timeline span.error-panel", timeout=20000)
                    no_results = await page.query_selector("div.timeline span.error-panel")
                    if no_results:
                        log.warning(f"Instance {instance} returned no results: {await no_results.inner_text()}")
                        continue
                    connected_instance = instance
                    log.info(f"Successfully connected to {instance}.")
                    break
                except Exception as e:
                    log.warning(f"Failed on {instance}: {e}")
                    await asyncio.sleep(1)

            if not connected_instance:
                log.error("Could not retrieve results from any Nitter instance.")
                await browser.close()
                return []

            await page.wait_for_timeout(3000)
            tweet_elements = await page.query_selector_all("div.timeline-item:not(.show-more)")
            log.info(f"Found {len(tweet_elements)} potential tweet elements.")

            tasks = [_extract_tweet_data_async(item, error_pairs_to_use, connected_instance) for item in tweet_elements]
            results = await asyncio.gather(*tasks)

            for tweet_data in results:
                if tweet_data and tweet_data["tweet_id"] not in processed_tweet_ids_this_scrape:
                    if len(fetched_tweets) < SCRAPE_MAX_TWEETS:
                        fetched_tweets.append(tweet_data)
                        processed_tweet_ids_this_scrape.add(tweet_data["tweet_id"])
                        log.debug(f"Added candidate tweet {tweet_data['tweet_id']}")
                    else:
                        log.info(f"Reached scrape limit ({SCRAPE_MAX_TWEETS}).")
                        break

            await browser.close()

        except Exception as e:
            log.error(f"Error during Playwright scraping: {e}", exc_info=DEBUG_MODE)
            return []

    log.info(f"Scraping finished. Found {len(fetched_tweets)} candidates.")
    return fetched_tweets
# --- End Core Function 1 ---


# --- Core Function 2: Process and Correct ---

# (_post_correction_reply_internal remains unchanged)
def _post_correction_reply_internal(tweet_id, correction_message): # ... (same logic) ...
    if not (tweet_id and correction_message and tweepy_client):
        log.error("Cannot post reply: tweet_id, message, or client missing.")
        return False, "internal_error"
    try:
        log.debug(f"Attempting to reply to tweet {tweet_id}")
        response = tweepy_client.create_tweet(text=correction_message, in_reply_to_tweet_id=tweet_id)
        if response and response.data and "id" in response.data:
            log.info(f"Successfully replied to {tweet_id}. New tweet ID: {response.data['id']}")
            return True, None
        else:
            log.error(f"Failed to reply to {tweet_id}. Unexpected API response: {response}")
            return False, "api_error"
    except tweepy.errors.Forbidden as e:
        error_str = str(e).lower()
        if any(phrase in error_str for phrase in [
            "you are not allowed to reply", "cannot reply to users who protect their tweets",
            "user is suspended", "you are unable to perform this action",
            "cannot send replies to the users who are not following you",
            "not allowed to create a tweet with duplicate content"
           ]):
            log.warning(f"Reply forbidden/duplicate for {tweet_id}: {e}")
            return False, "tweet_specific_error"
        else:
            log.error(f"Failed to reply (Forbidden - 403) to {tweet_id}: {e}", exc_info=DEBUG_MODE)
            return False, "api_error"
    except tweepy.errors.NotFound as e:
        log.warning(f"Failed to reply to {tweet_id} (Not Found - 404): {e}")
        return False, "tweet_specific_error"
    except tweepy.errors.TweepyException as e:
        log.error(f"Failed to reply (TweepyException) to {tweet_id}: {e}", exc_info=DEBUG_MODE)
        return False, "api_error"
    except Exception as e:
        log.error(f"Unexpected error replying to {tweet_id}: {e}", exc_info=True)
        return False, "internal_error"


def _is_valid_candidate(tweet, already_corrected_ids, persistent_queue_set):
    """
    Checks if a tweet is a valid candidate for correction.
    Now checks against the persistent queue set passed as an argument.
    """
    tweet_id = tweet.get("tweet_id")
    parsed_timestamp = tweet.get("parsed_timestamp")
    error_info = tweet.get("error_found")

    # Basic validation
    if not all([tweet_id, parsed_timestamp, error_info]): return False
    if not isinstance(parsed_timestamp, datetime): return False
    if not isinstance(error_info, dict): return False

    # Check persistent queue (loaded once per cycle)
    if tweet_id in persistent_queue_set:
        log.debug(f"Skipping {tweet_id}: In recently processed persistent queue.")
        return False

    # Check daily corrected log
    if tweet_id in already_corrected_ids:
        log.debug(f"Skipping {tweet_id}: Already corrected today.")
        return False

    # Check tweet age
    if (datetime.now(timezone.utc) - parsed_timestamp).days > MAX_TWEET_AGE_DAYS:
        log.debug(f"Skipping {tweet_id}: Too old ({parsed_timestamp.date()}).")
        return False

    return True # Passed all checks

def process_and_correct_tweet(candidate_tweets, already_corrected_ids):
    """
    Finds the best candidate, attempts correction, and updates the persistent queue file.
    """
    if not candidate_tweets:
        log.info("No candidates provided for processing.")
        return None

    # Load the persistent queue *once* for this processing cycle for efficiency
    persistent_queue_ids = load_json_file(PERSISTENT_QUEUE_FILENAME, default=[])
    if not isinstance(persistent_queue_ids, list): # Ensure it's a list
        log.warning(f"Persistent queue file {PERSISTENT_QUEUE_FILENAME} contained invalid data type. Treating as empty.")
        persistent_queue_ids = []
    persistent_queue_set = set(persistent_queue_ids) # Use set for fast lookups in _is_valid_candidate

    # Filter candidates using the loaded queue set
    valid_candidates = [
        t for t in candidate_tweets
        if _is_valid_candidate(t, already_corrected_ids, persistent_queue_set)
    ]
    log.info(f"Processing {len(valid_candidates)} valid candidates (after filtering against daily log and persistent queue).")

    if not valid_candidates:
        log.info("No valid candidates found after filtering.")
        return None

    # Prioritize most recent valid candidate
    valid_candidates.sort(key=lambda t: t["parsed_timestamp"], reverse=True)
    log.debug(f"Top candidate chosen: {valid_candidates[0]['tweet_id']}")

    corrected_tweet_id = None
    for candidate in valid_candidates:
        tweet_id = candidate["tweet_id"]
        incorrect = candidate["error_found"]["incorrect"]
        correct = candidate["error_found"]["correct"]
        log.info(f"Attempting correction for {tweet_id}: '{incorrect}' -> '{correct}'")

        # --- Add to Persistent Queue BEFORE attempting API call ---
        # Check again directly against the list to handle the order and size limit
        if tweet_id not in persistent_queue_ids:
            log.debug(f"Adding {tweet_id} to persistent queue file: {PERSISTENT_QUEUE_FILENAME}")
            persistent_queue_ids.append(tweet_id) # Add to the end

            # Enforce max size (remove oldest from the front)
            while len(persistent_queue_ids) > MAX_PROCESSED_QUEUE_SIZE:
                removed_id = persistent_queue_ids.pop(0)
                log.debug(f"Persistent queue exceeded max size ({MAX_PROCESSED_QUEUE_SIZE}). Removed oldest ID: {removed_id}")

            # Save updated queue back to file IMMEDIATELY
            if not save_json_file(PERSISTENT_QUEUE_FILENAME, persistent_queue_ids):
                # Log error but continue - the bot might re-attempt this on restart if file save fails,
                # but stopping the bot might be worse.
                log.error(f"CRITICAL: Failed to save updated persistent queue to {PERSISTENT_QUEUE_FILENAME}! Bot may re-process recent tweets on restart.")
            else:
                log.debug(f"Successfully updated persistent queue file.")
                # Update the set used for filtering subsequent candidates in *this* cycle
                persistent_queue_set.add(tweet_id)
                # Remove oldest if needed from set as well (though less critical as list is source of truth)
                if len(persistent_queue_ids) < MAX_PROCESSED_QUEUE_SIZE + (len(persistent_queue_set) - len(persistent_queue_ids)): # Heuristic check if set grew too large relative to list
                     persistent_queue_set = set(persistent_queue_ids) # Re-sync set if trimming happened

        else:
             # This case should ideally not happen if filtering worked, but is a safeguard
             log.warning(f"Tweet {tweet_id} was already in persistent queue list when attempting to add. This might indicate a race condition or logic issue.")

        # --- End Update Persistent Queue ---


        # Construct message
        correction_message = f"❌ {incorrect}\n✅ {correct}"
        log.debug(f"Correction message: \"{correction_message.replace(chr(10), ' ')}\"")

        # Post reply (API call)
        success, error_type = _post_correction_reply_internal(tweet_id, correction_message)

        if success:
            log.info(f"Success for {tweet_id}.")
            corrected_tweet_id = tweet_id
            break # Exit loop on successful correction
        elif error_type == "tweet_specific_error":
            log.warning(f"Skipping {tweet_id} due to tweet-specific issue ({error_type}). Trying next candidate.")
            # ID remains in the persistent queue as it was processed (attempted)
            continue # Try the next valid candidate
        elif error_type in ["api_error", "internal_error"]:
            log.error(f"Stopping correction cycle due to non-tweet-specific error ({error_type}) while processing {tweet_id}.")
            # ID remains in the persistent queue
            corrected_tweet_id = None # Ensure we don't report success
            break # Stop trying other candidates this cycle
        else:
             log.error(f"Unknown error type '{error_type}' processing {tweet_id}. Stopping cycle.")
             corrected_tweet_id = None
             break # Stop trying other candidates

    # --- Cycle Summary ---
    if corrected_tweet_id:
        log.info(f"Correction cycle finished successfully. Corrected Tweet ID: {corrected_tweet_id}")
    else:
        log.info("Correction cycle finished. No suitable candidate was corrected this cycle.")

    return corrected_tweet_id # Return the ID if successful, None otherwise

# --- End Core Function 2 ---


# --- Core Function 3: Interval Management and Main Loop ---
def manage_interval_and_run():
    """Main control loop for this bot worker instance."""
    log.info(f"================ Starting Bot Worker: {BOT_ID.upper()} ================")
    log.info(f"Daily Limit: {DAILY_CORRECTION_LIMIT}, Min Engagement: {MIN_ENGAGEMENT_QUERY}")
    log.info(f"Debug Mode: {DEBUG_MODE}, Max Persistent Queue: {MAX_PROCESSED_QUEUE_SIZE}")
    log.info(f"Persistent Queue File: {PERSISTENT_QUEUE_FILENAME}")
    log.info("===========================================================")

    try:
        base_interval_s = SECONDS_IN_DAY / DAILY_CORRECTION_LIMIT if DAILY_CORRECTION_LIMIT > 0 else SECONDS_IN_DAY
        log.info(f"Target base interval: ~{base_interval_s / 60:.1f} minutes")
    except ZeroDivisionError:
        base_interval_s = SECONDS_IN_DAY
        log.warning("DAILY_CORRECTION_LIMIT is 0, interval set to 24h.")

    while True:
        start_time = time.monotonic()
        current_time_utc = datetime.now(timezone.utc)
        today_date_obj = current_time_utc.date()
        today_str = today_date_obj.strftime("%Y-%m-%d")

        corrections_made_today_ids = get_corrections_made_today()
        corrections_count = len(corrections_made_today_ids)

        # Load persistent queue size for logging
        current_persistent_queue = load_json_file(PERSISTENT_QUEUE_FILENAME, default=[])
        persistent_queue_count = len(current_persistent_queue) if isinstance(current_persistent_queue, list) else 0

        log.info(f"--- Cycle Check ({current_time_utc.strftime('%H:%M:%S %Z')}) ---")
        log.info(f"Corrections on {today_str}: {corrections_count}/{DAILY_CORRECTION_LIMIT}. Persistent Queue Size: {persistent_queue_count}.")

        if corrections_count < DAILY_CORRECTION_LIMIT:
            log.info("Daily limit not reached. Proceeding with cycle.")

            # 1. Scrape
            fetched_tweets = asyncio.run(scrape_tweets(ERROR_PAIRS))

            # 2. Process & Correct
            if fetched_tweets:
                log.info(f"Scraper found {len(fetched_tweets)} potential tweet candidates.")
                # Pass daily corrected IDs for filtering
                corrected_tweet_id = process_and_correct_tweet(fetched_tweets, set(corrections_made_today_ids)) # Pass as set for efficiency

                if corrected_tweet_id:
                    # 3. Log Daily Correction Success
                    # Ensure the ID is a string for JSON compatibility
                    str_corrected_tweet_id = str(corrected_tweet_id)
                    corrections_made_today_ids.append(str_corrected_tweet_id) # Append the new ID
                    corrections_log_file = f"corrections_{BOT_ID}_{today_str}.json"
                    # Save the updated list back
                    if not save_json_file(corrections_log_file, corrections_made_today_ids):
                        log.critical(f"CRITICAL: Failed to save daily correction log '{corrections_log_file}' after correcting {str_corrected_tweet_id}!")
                    else:
                        log.info(f"Successfully updated and saved daily correction log for {str_corrected_tweet_id}.")
            else:
                log.info("Scraper returned no candidates this cycle.")

            # Calculate sleep time
            jitter = random.uniform(-MAX_INTERVAL_JITTER_S, MAX_INTERVAL_JITTER_S)
            sleep_duration_s = max(MIN_SLEEP_BETWEEN_CYCLES_S, base_interval_s + jitter)
            log.info(f"Cycle finished. Base interval sleep: {sleep_duration_s:.0f}s.")

        else: # Daily limit reached
            log.info(f"Daily correction limit ({DAILY_CORRECTION_LIMIT}) reached for {today_str}.")
            try:
                 next_day_start_utc = datetime.combine(today_date_obj + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
                 seconds_until_next_day = (next_day_start_utc - current_time_utc).total_seconds()
                 # Add a small random buffer past midnight
                 sleep_duration_s = max(MIN_SLEEP_BETWEEN_CYCLES_S, seconds_until_next_day + random.randint(60, 300))
                 log.info(f"Sleeping until after midnight UTC (~{sleep_duration_s / 3600:.2f}h).")
            except Exception as e:
                 log.error(f"Error calculating sleep until midnight: {e}. Sleeping for 1 hour as fallback.", exc_info=DEBUG_MODE)
                 sleep_duration_s = 3600

        # Ensure minimum sleep time
        actual_sleep = max(MIN_SLEEP_BETWEEN_CYCLES_S, sleep_duration_s)
        cycle_duration = time.monotonic() - start_time
        log.info(f"Cycle took {cycle_duration:.2f}s. --- Sleeping for {actual_sleep:.0f} seconds ---")
        time.sleep(actual_sleep)

# --- End Core Function 3 ---


# --- Script Entry Point ---
if __name__ == "__main__":
    log.info(f"Executing main function for bot instance: {BOT_ID.upper()}")
    try:
        manage_interval_and_run()
    except KeyboardInterrupt:
        log.info(f"[{BOT_ID.upper()}] KeyboardInterrupt received. Shutting down gracefully.")
    except Exception as e:
        log.critical(f"[{BOT_ID.upper()}] An uncaught exception occurred in the main loop: {e}", exc_info=True)
    finally:
        log.info(f"[{BOT_ID.upper()}] Bot worker process terminated.")
# --- End Script Entry Point ---