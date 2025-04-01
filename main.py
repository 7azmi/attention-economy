# single_bot_script_ram_queue.py
import asyncio
import json
import re
import logging
import urllib.parse
import os
import time
import random
from datetime import date, datetime, timedelta, timezone
from collections import deque # Import deque

# --- Third-Party Libraries ---
import tweepy
from playwright.async_api import async_playwright
from dotenv import load_dotenv

# --- Configuration & Constants ---
load_dotenv()

# Control Log Verbosity
DEBUG_MODE = os.getenv("DEBUG_MODE", "False").lower() in ("true", "1", "t")
LOG_LEVEL = logging.DEBUG if DEBUG_MODE else logging.INFO

# Twitter API Credentials
CREDENTIALS = { # ... (same as before) ...
    "api_key": os.getenv("API_KEY"),
    "api_secret": os.getenv("API_SECRET"),
    "bearer_token": os.getenv("BEARER_TOKEN"),
    "access_token": os.getenv("ACCESS_TOKEN"),
    "access_token_secret": os.getenv("ACCESS_TOKEN_SECRET"),
}

# Operational Parameters
DAILY_CORRECTION_LIMIT = 45
SCRAPER_TIMEOUT_S = 120
TWEETER_TIMEOUT_S = 60
MAX_INTERVAL_JITTER_S = 300
MIN_SLEEP_BETWEEN_CYCLES_S = 60
SECONDS_IN_DAY = 24 * 60 * 60
MAX_TWEET_AGE_DAYS = 2
SCRAPE_MAX_TWEETS = 30
MIN_ENGAGEMENT_QUERY = "(min_retweets:25 OR min_faves:50)"
NITTER_INSTANCES = [
    "https://nitter.net",
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
    "https://nitter.cz",
]
# --- REMOVED Queue File Config ---
# --- NEW: RAM Queue Config ---
MAX_PROCESSED_QUEUE_SIZE = 2000 # Keep track of this many recent IDs in RAM
# --- End Configuration ---

# --- Error Pairs Definition ---
ERROR_PAIRS = [ # ... (same as before) ...
    ("انشاء الله", "إن شاء الله"),
    ("إنشاء الله", "إن شاء الله"),
    ("لاكن", "لكن"),
    ("ضلم", "ظلم"),
    ("ضالم", "ظالم"),
    ("خطاء", "خطأ"),
    ("هاذا", "هذا"),
]
# --- End Error Pairs ---

# --- Logging Setup ---
log_filename = f"bot_log_{date.today().strftime('%Y-%m-%d')}.log"
logging.basicConfig( # ... (same as before) ...
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)
log.info(f"Logging initialized. Level: {logging.getLevelName(LOG_LEVEL)}")
# --- End Logging Setup ---

# --- NEW: Global RAM Queue Variables ---
# Initialized once when the script starts, lives only in memory
processed_queue = deque(maxlen=MAX_PROCESSED_QUEUE_SIZE)
processed_ids_set = set() # For fast lookups O(1)
log.info(f"In-memory processed queue initialized (max size: {MAX_PROCESSED_QUEUE_SIZE}).")
# --- End Global Queue Variables ---

# --- Tweepy Client Initialization ---
tweepy_client = None # ... (same initialization logic as before) ...
if not all(CREDENTIALS.values()):
    log.critical("Twitter API credentials missing! Check .env file. Exiting.")
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
    log.info(f"Tweepy Client (v2) initialized successfully for @{auth_user.data.username}")
except tweepy.errors.TweepyException as e:
    log.critical(f"Failed to initialize Tweepy client: {e}", exc_info=True)
    exit(1)
except Exception as e:
    log.critical(f"Unexpected error during Tweepy client initialization: {e}", exc_info=True)
    exit(1)
# --- End Tweepy Client Initialization ---

# --- Helper Functions ---

# --- REMOVED load_processed_queue ---
# --- REMOVED save_processed_queue ---

def load_json_file(filename, default=None): # ... (same as before) ...
    """Safely loads a JSON file, returning default if missing or invalid."""
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        log.debug(f"File not found: {filename}. Returning default.")
        return default
    except json.JSONDecodeError:
        log.warning(f"Invalid JSON in file: {filename}. Returning default.")
        return default
    except Exception as e:
        log.error(f"Error loading {filename}: {e}. Returning default.", exc_info=DEBUG_MODE)
        return default

def save_json_file(filename, data): # ... (same as before) ...
    """Safely saves data to a JSON file, returning True on success, False otherwise."""
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        log.debug(f"Successfully saved data to {filename}")
        return True
    except (IOError, Exception) as e:
        log.error(f"Error writing to {filename}: {e}", exc_info=DEBUG_MODE)
        return False

def get_corrections_made_today(): # ... (same as before) ...
    """Gets the list of corrected tweet IDs from today's log file."""
    today_str = date.today().strftime("%Y-%m-%d")
    corrections_file = f"corrections_{today_str}.json"
    corrections_list = load_json_file(corrections_file, default=[])

    if isinstance(corrections_list, list):
        valid_ids = [str(item) for item in corrections_list if isinstance(item, (str, int))]
        if len(valid_ids) != len(corrections_list):
            log.warning(f"Corrections file {corrections_file} contained non-ID items. Cleaned list.")
        return valid_ids
    else:
        log.warning(f"Corrections file {corrections_file} contained invalid data type. Assuming 0 corrections and attempting to reset.")
        if save_json_file(corrections_file, []):
             log.info(f"Successfully reset {corrections_file} to an empty list.")
        else:
             log.error(f"Failed to reset corrupt corrections file {corrections_file}.")
        return []

def extract_number(text): # ... (same as before) ...
    """Extracts the first number from a string, handling K/M suffixes."""
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
    """Parses Nitter's timestamp string into a timezone-aware datetime object."""
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
            log.warning(f"Non-UTC timezone '{timezone_str}' detected in timestamp '{timestamp_str}'. Assuming UTC.")
            return tweet_time_naive.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError, IndexError) as e:
        log.debug(f"Could not parse timestamp '{timestamp_str}'. Error: {e}")
        return None

# --- End Helper Functions ---


# --- Core Function 1: Scraper ---
# _extract_tweet_data_async and scrape_tweets remain unchanged from the previous version
async def _extract_tweet_data_async(item, error_pairs, connected_instance_url): # ... (same as before) ...
    """Async helper to extract data from a single tweet element (Internal use)."""
    try:
        tweet_link_element = await item.query_selector("a.tweet-link")
        username_element = await item.query_selector("a.username")
        timestamp_element = await item.query_selector("span.tweet-date a")
        tweet_text_element = await item.query_selector("div.tweet-content")

        if not all([tweet_link_element, username_element, timestamp_element, tweet_text_element]):
            log.debug("Skipping item: Missing essential elements (link, user, date, content).")
            return None

        tweet_link_raw = await tweet_link_element.get_attribute("href")
        tweet_link = urllib.parse.urljoin(connected_instance_url, tweet_link_raw) if tweet_link_raw else None
        tweet_id_match = re.search(r"/(?:status|statuses)/(\d+)", tweet_link) if tweet_link else None
        tweet_id = tweet_id_match.group(1) if tweet_id_match else None
        if not tweet_id:
            log.debug(f"Skipping item: Could not extract tweet ID from link '{tweet_link}'.")
            return None # Essential

        username = (await username_element.inner_text()).strip()
        timestamp_str = (await timestamp_element.get_attribute("title") or await timestamp_element.inner_text()).strip()
        tweet_text = (await tweet_text_element.inner_text()).strip()

        if tweet_text.startswith("RT @"):
            log.debug(f"Skipping tweet {tweet_id}: Looks like a Retweet.")
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
             log.debug(f"Skipping tweet {tweet_id}: Could not parse timestamp '{timestamp_str}'.")
             return None

        return {
            "username": username,
            "timestamp_str": timestamp_str,
            "parsed_timestamp": parsed_timestamp,
            "tweet": tweet_text,
            "link": tweet_link,
            "tweet_id": tweet_id,
            "error_found": found_error,
            "engagement": {"replies": replies, "retweets": retweets, "likes": likes, "quotes": quotes},
        }
    except Exception as e:
        log.warning(f"Error processing a tweet element: {e}", exc_info=DEBUG_MODE)
        return None

async def scrape_tweets(error_pairs): # ... (same as before) ...
    """Scrapes Nitter for tweets containing specific errors."""
    log.info("Starting tweet scraping process...")
    incorrect_words_query = " OR ".join([f'"{pair[0]}"' for pair in error_pairs])
    base_query = f"({incorrect_words_query}) {MIN_ENGAGEMENT_QUERY} lang:ar -filter:retweets -filter:replies"
    encoded_query = urllib.parse.quote(base_query)
    search_url_template = "/search?f=tweets&q={query}&since=&until=&near="
    log.info(f"Constructed base query: {base_query}")

    fetched_tweets = []
    processed_tweet_ids_this_scrape = set() # Local set for this scrape run only
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
                log.info(f"Trying Nitter instance: {instance} -> {search_url}")
                try:
                    await page.goto(search_url, timeout=30000, wait_until="domcontentloaded")
                    await page.wait_for_selector("div.timeline .timeline-item, div.timeline span.error-panel", timeout=20000)
                    no_results = await page.query_selector("div.timeline span.error-panel")
                    if no_results:
                        no_results_text = await no_results.inner_text()
                        log.warning(f"Instance {instance} returned no results: {no_results_text.strip()}")
                        continue
                    connected_instance = instance
                    log.info(f"Successfully connected to {instance} and found timeline.")
                    break
                except Exception as e:
                    log.warning(f"Failed connection or content wait on {instance}: {e}")
                    await asyncio.sleep(1)

            if not connected_instance:
                log.error("Could not retrieve results from any Nitter instance.")
                await browser.close()
                return []

            await page.wait_for_timeout(3000)

            tweet_elements = await page.query_selector_all("div.timeline-item:not(.show-more)")
            log.info(f"Found {len(tweet_elements)} potential tweet elements on the page.")

            tasks = [_extract_tweet_data_async(item, error_pairs, connected_instance) for item in tweet_elements]
            results = await asyncio.gather(*tasks)

            for tweet_data in results:
                if tweet_data and tweet_data["tweet_id"] not in processed_tweet_ids_this_scrape:
                    if len(fetched_tweets) < SCRAPE_MAX_TWEETS:
                        fetched_tweets.append(tweet_data)
                        processed_tweet_ids_this_scrape.add(tweet_data["tweet_id"])
                        log.debug(f"Added candidate tweet {tweet_data['tweet_id']} ({tweet_data['error_found']['incorrect']})")
                    else:
                        log.info(f"Reached scrape limit ({SCRAPE_MAX_TWEETS}).")
                        break

            await browser.close()

        except Exception as e:
            log.error(f"Error during Playwright scraping process: {e}", exc_info=DEBUG_MODE)
            return []

    log.info(f"Scraping finished. Found {len(fetched_tweets)} potential candidates.")
    return fetched_tweets
# --- End Core Function 1 ---


# --- Core Function 2: Process and Correct ---

def _post_correction_reply_internal(tweet_id, correction_message): # ... (same as before) ...
    """Internal helper to post reply, returning status and error type."""
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
            log.error(f"Failed to reply to {tweet_id}. Unexpected API response structure: {response}")
            return False, "api_error"
    except tweepy.errors.Forbidden as e:
        error_str = str(e).lower()
        if any(phrase in error_str for phrase in [
            "you are not allowed to reply", "cannot reply to users who protect their tweets",
            "user is suspended", "you are unable to perform this action",
            "cannot send replies to the users who are not following you",
            "not allowed to create a tweet with duplicate content"
           ]):
            log.warning(f"Reply forbidden/duplicate for tweet {tweet_id}. Specific Reason: {e}")
            return False, "tweet_specific_error"
        else:
            log.error(f"Failed to reply to {tweet_id} (Forbidden - 403): {e}", exc_info=DEBUG_MODE)
            return False, "api_error"
    except tweepy.errors.NotFound as e:
        log.warning(f"Failed to reply to {tweet_id}: Tweet likely deleted (Not Found - 404). {e}")
        return False, "tweet_specific_error"
    except tweepy.errors.TweepyException as e:
        log.error(f"Failed to reply to {tweet_id} (TweepyException): {e}", exc_info=DEBUG_MODE)
        return False, "api_error"
    except Exception as e:
        log.error(f"Unexpected error replying to {tweet_id}: {e}", exc_info=True)
        return False, "internal_error"

def _is_valid_candidate(tweet, already_corrected_ids):
    """Validates a single tweet candidate, including check against RAM processed queue."""
    global processed_ids_set # Access the global RAM set for checking
    tweet_id = tweet.get("tweet_id")
    parsed_timestamp = tweet.get("parsed_timestamp")
    error_info = tweet.get("error_found")

    if not tweet_id: return False
    if not parsed_timestamp or not isinstance(parsed_timestamp, datetime): return False
    if not error_info or not isinstance(error_info, dict): return False

    # --- Check against the RAM processed queue ---
    if tweet_id in processed_ids_set:
        log.debug(f"Skipping candidate {tweet_id}: Found in recently processed RAM queue.")
        return False
    # --- End RAM Check ---

    if tweet_id in already_corrected_ids:
        log.debug(f"Skipping candidate {tweet_id}: Already corrected today.")
        return False

    if (datetime.now(timezone.utc) - parsed_timestamp).days > MAX_TWEET_AGE_DAYS:
        log.debug(f"Skipping candidate {tweet_id}: Too old (posted {parsed_timestamp.date()}).")
        return False

    return True

def process_and_correct_tweet(candidate_tweets, already_corrected_ids):
    """
    Selects the best candidate, attempts correction, handles errors, and updates RAM processed queue.
    """
    global processed_queue, processed_ids_set # Allow modification of RAM globals
    if not candidate_tweets:
        log.info("No candidates provided for processing.")
        return None

    valid_candidates = [
        tweet for tweet in candidate_tweets
        if _is_valid_candidate(tweet, already_corrected_ids)
    ]
    log.info(f"Processing {len(valid_candidates)} valid candidates (out of {len(candidate_tweets)} fetched).")

    if not valid_candidates:
        log.info("No valid candidates found after filtering.")
        return None

    valid_candidates.sort(key=lambda t: t["parsed_timestamp"], reverse=True)
    log.debug(f"Sorted valid candidates. Top candidate ID: {valid_candidates[0]['tweet_id']} @ {valid_candidates[0]['parsed_timestamp']}")

    corrected_tweet_id = None
    for candidate in valid_candidates:
        tweet_id = candidate["tweet_id"]
        incorrect = candidate["error_found"]["incorrect"]
        correct = candidate["error_found"]["correct"]

        log.info(f"Attempting correction for candidate: ID {tweet_id}, Error: '{incorrect}' -> '{correct}'")

        # --- Add to RAM processed queue *before* attempting reply ---
        if tweet_id not in processed_ids_set:
            log.debug(f"Adding tweet {tweet_id} to RAM processed queue.")
            processed_queue.append(tweet_id) # deque automatically handles maxlen
            processed_ids_set.add(tweet_id)
            # --- REMOVED save_processed_queue() call ---
        # --- End Queue Logic ---

        correction_message = f"❌ {incorrect}\n✅ {correct}"
        log.debug(f"Correction message: \"{correction_message.replace(chr(10), ' ')}\"")

        success, error_type = _post_correction_reply_internal(tweet_id, correction_message)

        if success:
            log.info(f"Successfully posted correction for tweet {tweet_id}.")
            corrected_tweet_id = tweet_id
            break # Exit loop on first success

        elif error_type == "tweet_specific_error":
            log.warning(f"Skipping candidate {tweet_id} due to tweet-specific issue. Trying next.")
            # ID was already added to RAM processed queue for this run
            continue # Try the next candidate

        elif error_type in ["api_error", "internal_error"]:
            log.error(f"Stopping correction attempts this cycle due to non-tweet-specific error ({error_type}) for tweet {tweet_id}.")
            corrected_tweet_id = None
            break # Exit the loop for this cycle

        else:
             log.error(f"Unknown error state processing tweet {tweet_id}. Stopping cycle.")
             corrected_tweet_id = None
             break


    if corrected_tweet_id:
        log.info(f"Correction cycle successful. Corrected tweet: {corrected_tweet_id}")
    else:
        log.info("No suitable candidate could be corrected in this cycle.")

    return corrected_tweet_id

# --- End Core Function 2 ---


# --- Core Function 3: Interval Management and Main Loop ---

def manage_interval_and_run(): # ... (Mostly same as before) ...
    """Main control loop for the bot."""
    log.info("===========================================")
    log.info("Starting Bot Main Loop")
    log.info(f"Daily Correction Limit: {DAILY_CORRECTION_LIMIT}")
    log.info(f"Debug Mode: {DEBUG_MODE}")
    log.info(f"In-Memory Processed Queue Size: {MAX_PROCESSED_QUEUE_SIZE}")
    log.info("===========================================")

    # --- REMOVED load_processed_queue() call ---

    try:
        base_interval_s = SECONDS_IN_DAY / DAILY_CORRECTION_LIMIT if DAILY_CORRECTION_LIMIT > 0 else SECONDS_IN_DAY
        log.info(f"Target base interval between successful cycles: ~{base_interval_s / 60:.1f} minutes")
    except ZeroDivisionError:
        base_interval_s = SECONDS_IN_DAY
        log.warning("DAILY_CORRECTION_LIMIT is 0, setting interval to 24 hours.")

    while True:
        start_time = time.monotonic()
        current_time_utc = datetime.now(timezone.utc)
        today_date_obj = current_time_utc.date()
        today_str = today_date_obj.strftime("%Y-%m-%d")

        corrections_made_today_ids = get_corrections_made_today()
        corrections_count = len(corrections_made_today_ids)
        log.info(f"--- Starting Cycle Check ({current_time_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}) ---")
        # Log current RAM queue size
        log.info(f"Corrections made on {today_str}: {corrections_count}/{DAILY_CORRECTION_LIMIT}. RAM Processed Queue Size: {len(processed_queue)}.")

        corrected_in_this_cycle = False
        if corrections_count < DAILY_CORRECTION_LIMIT:
            log.info("Daily limit not reached. Proceeding with scrape and correct.")

            # 1. Scrape
            fetched_tweets = asyncio.run(scrape_tweets(ERROR_PAIRS))

            # 2. Process & Correct
            if fetched_tweets:
                log.info(f"Scraper found {len(fetched_tweets)} candidates. Processing...")
                # Pass today's corrected IDs (validation still uses global RAM queue)
                corrected_tweet_id = process_and_correct_tweet(fetched_tweets, corrections_made_today_ids)

                if corrected_tweet_id:
                    # 3. Log Correction Success (Daily Log - still uses file)
                    corrections_made_today_ids.append(corrected_tweet_id)
                    corrections_log_file = f"corrections_{today_str}.json"
                    if not save_json_file(corrections_log_file, corrections_made_today_ids):
                        log.critical(f"CRITICAL: Failed to save updated daily corrections log '{corrections_log_file}'!")
                    else:
                        log.info(f"Successfully logged daily correction for {corrected_tweet_id}.")
                        corrected_in_this_cycle = True
            else:
                log.info("Scraper did not return any candidates.")

            # Calculate sleep time
            jitter = random.uniform(-MAX_INTERVAL_JITTER_S, MAX_INTERVAL_JITTER_S)
            sleep_duration_s = max(MIN_SLEEP_BETWEEN_CYCLES_S, base_interval_s + jitter)
            log.info(f"Cycle finished. Base interval sleep: {sleep_duration_s:.0f} seconds.")

        else: # Daily limit reached
            log.info(f"Daily correction limit ({DAILY_CORRECTION_LIMIT}) reached for {today_str}.")
            try:
                 next_day_start_utc = datetime.combine(today_date_obj + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
                 seconds_until_next_day = (next_day_start_utc - current_time_utc).total_seconds()
                 sleep_duration_s = max(MIN_SLEEP_BETWEEN_CYCLES_S, seconds_until_next_day + random.randint(60, 300))
                 log.info(f"Sleeping until after midnight UTC (approx {sleep_duration_s / 3600:.2f} hours).")
            except Exception as e:
                 log.error(f"Error calculating time until midnight: {e}. Sleeping for 1 hour as fallback.")
                 sleep_duration_s = 3600

        actual_sleep = max(MIN_SLEEP_BETWEEN_CYCLES_S, sleep_duration_s)
        log.info(f"--- Cycle End --- Sleeping for {actual_sleep:.0f} seconds ---")
        time.sleep(actual_sleep)

# --- End Core Function 3 ---


# --- Script Entry Point ---
if __name__ == "__main__":
    try:
        manage_interval_and_run()
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt received. Shutting down gracefully.")
    except Exception as e:
        log.critical(f"An uncaught exception occurred in the main loop: {e}", exc_info=True)
    finally:
        log.info("Bot script terminated. In-memory queue is lost.")
# --- End Script Entry Point ---