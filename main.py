# single_bot_script.py
import asyncio
import json
import re
import logging
import urllib.parse
import os
import time
import random
from datetime import date, datetime, timedelta, timezone

# --- Third-Party Libraries ---
import tweepy
from playwright.async_api import async_playwright
from dotenv import load_dotenv

# --- Configuration & Constants ---
load_dotenv()  # Load .env variables (API keys, etc.)

# Control Log Verbosity (Set DEBUG_MODE=False for production)
DEBUG_MODE = os.getenv("DEBUG_MODE", "False").lower() in ("true", "1", "t")
LOG_LEVEL = logging.DEBUG if DEBUG_MODE else logging.INFO

# Twitter API Credentials
CREDENTIALS = {
    "api_key": os.getenv("API_KEY"),
    "api_secret": os.getenv("API_SECRET"),
    "bearer_token": os.getenv("BEARER_TOKEN"),
    "access_token": os.getenv("ACCESS_TOKEN"),
    "access_token_secret": os.getenv("ACCESS_TOKEN_SECRET"),
}

# Operational Parameters
DAILY_CORRECTION_LIMIT = 30    # Target number of corrections per day
SCRAPER_TIMEOUT_S = 120       # Max time for scraper (seconds) - Adjusted for single script
TWEETER_TIMEOUT_S = 60        # Timeout for individual tweet post attempt (within tweepy client)
MAX_INTERVAL_JITTER_S = 300   # Max random variation +/- for cycle interval (seconds)
MIN_SLEEP_BETWEEN_CYCLES_S = 60 # Minimum time to wait before next cycle (seconds)
SECONDS_IN_DAY = 24 * 60 * 60 # For interval calculation
MAX_TWEET_AGE_DAYS = 2        # Ignore tweets older than this
SCRAPE_MAX_TWEETS = 30        # Max candidates to fetch per scrape
MIN_ENGAGEMENT_QUERY = "(min_retweets:20 OR min_faves:50)" # Nitter engagement filter
NITTER_INSTANCES = [          # List of Nitter instances to try
    "https://nitter.net",
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
    "https://nitter.cz",
]
# --- End Configuration ---

# --- Error Pairs Definition ---
ERROR_PAIRS = [
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
logging.basicConfig(
    level=LOG_LEVEL, # Set level based on DEBUG_MODE
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)
log.info(f"Logging initialized. Level: {logging.getLevelName(LOG_LEVEL)}")
# --- End Logging Setup ---


# --- Tweepy Client Initialization ---
tweepy_client = None
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
        wait_on_rate_limit=True, # Automatically wait if rate limited
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

def load_json_file(filename, default=None):
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

def save_json_file(filename, data):
    """Safely saves data to a JSON file, returning True on success, False otherwise."""
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        log.debug(f"Successfully saved data to {filename}")
        return True
    except (IOError, Exception) as e:
        log.error(f"Error writing to {filename}: {e}", exc_info=DEBUG_MODE)
        return False

def get_corrections_made_today():
    """Gets the list of corrected tweet IDs from today's log file."""
    today_str = date.today().strftime("%Y-%m-%d")
    corrections_file = f"corrections_{today_str}.json"
    corrections_list = load_json_file(corrections_file, default=[])

    if isinstance(corrections_list, list):
        # Validate that it contains strings (or numbers convertible to strings)
        valid_ids = [str(item) for item in corrections_list if isinstance(item, (str, int))]
        if len(valid_ids) != len(corrections_list):
            log.warning(f"Corrections file {corrections_file} contained non-ID items. Cleaned list.")
            # Optionally save the cleaned list back
            # save_json_file(corrections_file, valid_ids)
        return valid_ids
    else:
        log.warning(f"Corrections file {corrections_file} contained invalid data type. Assuming 0 corrections and attempting to reset.")
        if save_json_file(corrections_file, []):
             log.info(f"Successfully reset {corrections_file} to an empty list.")
        else:
             log.error(f"Failed to reset corrupt corrections file {corrections_file}.")
        return []

def extract_number(text):
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

def parse_tweet_timestamp(timestamp_str):
    """Parses Nitter's timestamp string into a timezone-aware datetime object."""
    # Expected format: "Mar 30, 2025 · 1:39 AM UTC" or similar locale
    try:
        # Clean up potential extra whitespace
        timestamp_str = re.sub(r'\s+', ' ', timestamp_str).strip()

        # Split date, time, and timezone parts - more robustly
        parts = timestamp_str.split('·')
        if len(parts) != 2:
            raise ValueError("Timestamp format incorrect: Missing '·' separator")

        date_part = parts[0].strip()
        time_part_full = parts[1].strip()

        # Find the timezone (usually the last word)
        time_parts = time_part_full.split(' ')
        if len(time_parts) < 2:
             raise ValueError("Timestamp format incorrect: Missing time or timezone")

        timezone_str = time_parts[-1].upper()
        time_value_str = " ".join(time_parts[:-1]) # Join remaining parts for time

        # Standardize date format if possible (e.g., handle different month abbreviations)
        # This might need adjustments based on actual Nitter output variations
        # Assuming English month abbreviations for now
        dt_str = f"{date_part} {time_value_str}"
        tweet_time_naive = datetime.strptime(dt_str, "%b %d, %Y %I:%M %p")

        # Handle timezone - Nitter *usually* provides UTC
        if timezone_str == "UTC":
            return tweet_time_naive.replace(tzinfo=timezone.utc)
        else:
            # If timezone is not UTC, we might have issues if it's ambiguous
            # For simplicity, we'll log a warning and assume UTC, but this could be improved
            log.warning(f"Non-UTC timezone '{timezone_str}' detected in timestamp '{timestamp_str}'. Assuming UTC.")
            return tweet_time_naive.replace(tzinfo=timezone.utc)

    except (ValueError, TypeError, IndexError) as e:
        log.debug(f"Could not parse timestamp '{timestamp_str}'. Error: {e}")
        return None

# --- End Helper Functions ---


# --- Core Function 1: Scraper ---

async def _extract_tweet_data_async(item, error_pairs, connected_instance_url):
    """Async helper to extract data from a single tweet element (Internal use)."""
    try:
        # Essential elements
        tweet_link_element = await item.query_selector("a.tweet-link")
        username_element = await item.query_selector("a.username")
        timestamp_element = await item.query_selector("span.tweet-date a")
        tweet_text_element = await item.query_selector("div.tweet-content")

        if not all([tweet_link_element, username_element, timestamp_element, tweet_text_element]):
            log.debug("Skipping item: Missing essential elements (link, user, date, content).")
            return None

        # Extract core data
        tweet_link_raw = await tweet_link_element.get_attribute("href")
        tweet_link = urllib.parse.urljoin(connected_instance_url, tweet_link_raw) if tweet_link_raw else None
        tweet_id_match = re.search(r"/(?:status|statuses)/(\d+)", tweet_link) if tweet_link else None
        tweet_id = tweet_id_match.group(1) if tweet_id_match else None
        username = (await username_element.inner_text()).strip()
        timestamp_str = (await timestamp_element.get_attribute("title") or await timestamp_element.inner_text()).strip()
        tweet_text = (await tweet_text_element.inner_text()).strip()

        if not tweet_id:
            log.debug(f"Skipping item: Could not extract tweet ID from link '{tweet_link}'.")
            return None
        if tweet_text.startswith("RT @"):
            log.debug(f"Skipping tweet {tweet_id}: Looks like a Retweet.")
            return None

        # Check for errors
        found_error = next(
            (
                {"incorrect": incorrect, "correct": correct}
                for incorrect, correct in error_pairs
                if re.search(r"\b" + re.escape(incorrect) + r"\b", tweet_text, re.IGNORECASE | re.UNICODE) # Use word boundaries
            ),
            None,
        )
        if not found_error:
            # log.debug(f"No target errors found in tweet {tweet_id}.") # Can be noisy
            return None

        # Extract stats
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
                        # Check more robustly for keywords in class names
                        if any(k in icon_class for k in ["comment", "reply"]): replies = stat_value
                        elif "retweet" in icon_class: retweets = stat_value
                        elif any(k in icon_class for k in ["heart", "like", "favorite"]): likes = stat_value
                        elif "quote" in icon_class: quotes = stat_value
            except Exception as e:
                log.debug(f"Minor error extracting stat for {tweet_id}: {e}")

        # Parse timestamp
        parsed_timestamp = parse_tweet_timestamp(timestamp_str)
        if not parsed_timestamp:
             log.debug(f"Skipping tweet {tweet_id}: Could not parse timestamp '{timestamp_str}'.")
             return None # Skip if timestamp is invalid


        return {
            "username": username,
            "timestamp_str": timestamp_str, # Keep original string
            "parsed_timestamp": parsed_timestamp, # Store datetime object
            "tweet": tweet_text,
            "link": tweet_link,
            "tweet_id": tweet_id,
            "error_found": found_error,
            "engagement": {"replies": replies, "retweets": retweets, "likes": likes, "quotes": quotes},
        }

    except Exception as e:
        log.warning(f"Error processing a tweet element: {e}", exc_info=DEBUG_MODE)
        return None

async def scrape_tweets(error_pairs):
    """
    Scrapes Nitter for tweets containing specific errors.

    Args:
        error_pairs (list): A list of tuples, where each tuple is (incorrect_word, correct_word).

    Returns:
        list: A list of tweet dictionaries found, or an empty list if none found or on error.
    """
    log.info("Starting tweet scraping process...")
    incorrect_words_query = " OR ".join([f'"{pair[0]}"' for pair in error_pairs])
    # Ensure query is URL-safe and handles Arabic characters correctly
    base_query = f"({incorrect_words_query}) {MIN_ENGAGEMENT_QUERY} lang:ar -filter:retweets -filter:replies"
    encoded_query = urllib.parse.quote(base_query)
    search_url_template = "/search?f=tweets&q={query}&since=&until=&near=" # Added f=tweets
    log.info(f"Constructed base query: {base_query}")

    fetched_tweets = []
    processed_tweet_ids = set()
    connected_instance = None

    async with async_playwright() as p:
        try:
            browser = await p.firefox.launch(headless=True) # Consider headless=False for debugging
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
                java_script_enabled=True, # Ensure JS is enabled
            )
            await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            page = await context.new_page()

            # Try connecting to Nitter instances
            for instance in NITTER_INSTANCES:
                search_url = instance + search_url_template.format(query=encoded_query)
                log.info(f"Trying Nitter instance: {instance} -> {search_url}")
                try:
                    # Go to page and wait for network idle or a timeline item
                    await page.goto(search_url, timeout=30000, wait_until="domcontentloaded") # Increased timeout slightly
                    # Wait for either timeline items or a 'no results' message
                    await page.wait_for_selector("div.timeline .timeline-item, div.timeline span.error-panel", timeout=20000)

                    # Check if results were actually found
                    no_results = await page.query_selector("div.timeline span.error-panel")
                    if no_results:
                        no_results_text = await no_results.inner_text()
                        log.warning(f"Instance {instance} returned no results: {no_results_text.strip()}")
                        continue # Try next instance

                    connected_instance = instance
                    log.info(f"Successfully connected to {instance} and found timeline.")
                    break # Stop trying instances
                except Exception as e:
                    log.warning(f"Failed connection or content wait on {instance}: {e}")
                    await asyncio.sleep(1) # Small delay before next try

            if not connected_instance:
                log.error("Could not retrieve results from any Nitter instance.")
                await browser.close()
                return []

            # Give page a bit more time to potentially load more items via JS
            await page.wait_for_timeout(3000)

            # Scrape tweets
            tweet_elements = await page.query_selector_all("div.timeline-item:not(.show-more)") # Exclude 'show more' button
            log.info(f"Found {len(tweet_elements)} potential tweet elements on the page.")

            tasks = []
            for item in tweet_elements:
                 tasks.append(_extract_tweet_data_async(item, error_pairs, connected_instance))

            results = await asyncio.gather(*tasks)

            for tweet_data in results:
                if tweet_data and tweet_data["tweet_id"] not in processed_tweet_ids:
                    if len(fetched_tweets) < SCRAPE_MAX_TWEETS:
                        fetched_tweets.append(tweet_data)
                        processed_tweet_ids.add(tweet_data["tweet_id"])
                        log.debug(f"Added candidate tweet {tweet_data['tweet_id']} ({tweet_data['error_found']['incorrect']})")
                    else:
                        log.info(f"Reached scrape limit ({SCRAPE_MAX_TWEETS}).")
                        break # Stop processing elements if limit reached

            await browser.close()

        except Exception as e:
            log.error(f"Error during Playwright scraping process: {e}", exc_info=DEBUG_MODE)
            return [] # Return empty list on major scraping error

    log.info(f"Scraping finished. Found {len(fetched_tweets)} potential candidates.")
    return fetched_tweets

# --- End Core Function 1 ---


# --- Core Function 2: Process and Correct ---

def _post_correction_reply_internal(tweet_id, correction_message):
    """Internal helper to post reply, returning status and error type."""
    if not (tweet_id and correction_message and tweepy_client):
        log.error("Cannot post reply: tweet_id, message, or client missing.")
        return False, "internal_error"

    try:
        log.debug(f"Attempting to reply to tweet {tweet_id}")
        response = tweepy_client.create_tweet(text=correction_message, in_reply_to_tweet_id=tweet_id)
        if response and response.data and "id" in response.data:
            log.info(f"Successfully replied to {tweet_id}. New tweet ID: {response.data['id']}")
            return True, None # Success
        else:
            # Handle cases where response might be unexpected but not an exception
            log.error(f"Failed to reply to {tweet_id}. Unexpected API response structure: {response}")
            return False, "api_error" # Treat as a non-tweet-specific API error

    except tweepy.errors.Forbidden as e:
        error_str = str(e).lower()
        if any(phrase in error_str for phrase in [
            "you are not allowed to reply",
            "cannot reply to users who protect their tweets",
            "user is suspended",
            "you are unable to perform this action", # Generic permission issue
            "cannot send replies to the users who are not following you", # Send reply setting
            "not allowed to create a tweet with duplicate content" # Duplicate check
           ]):
            log.warning(f"Reply forbidden/duplicate for tweet {tweet_id}. Specific Reason: {e}")
            return False, "tweet_specific_error" # User setting, suspension, duplicate
        else:
            # Other Forbidden errors might be less specific
            log.error(f"Failed to reply to {tweet_id} (Forbidden - 403): {e}", exc_info=DEBUG_MODE)
            return False, "api_error" # Treat as a general API error

    except tweepy.errors.NotFound as e:
        log.warning(f"Failed to reply to {tweet_id}: Tweet likely deleted (Not Found - 404). {e}")
        return False, "tweet_specific_error" # Tweet is gone

    except tweepy.errors.TweepyException as e:
        # Catch other Tweepy/API errors (like rate limits, connection issues, etc.)
        log.error(f"Failed to reply to {tweet_id} (TweepyException): {e}", exc_info=DEBUG_MODE)
        return False, "api_error" # General API/Tweepy error

    except Exception as e:
        # Catch any other unexpected errors during the API call
        log.error(f"Unexpected error replying to {tweet_id}: {e}", exc_info=True)
        return False, "internal_error" # Code error

def _is_valid_candidate(tweet, already_corrected_ids):
    """Validates a single tweet candidate."""
    tweet_id = tweet.get("tweet_id")
    parsed_timestamp = tweet.get("parsed_timestamp")
    error_info = tweet.get("error_found")

    if not tweet_id: return False # Must have an ID
    if not parsed_timestamp or not isinstance(parsed_timestamp, datetime): return False # Must have valid parsed timestamp
    if not error_info or not isinstance(error_info, dict): return False # Must have error info

    if tweet_id in already_corrected_ids:
        log.debug(f"Skipping candidate {tweet_id}: Already corrected today.")
        return False

    # Check age
    if (datetime.now(timezone.utc) - parsed_timestamp).days > MAX_TWEET_AGE_DAYS:
        log.debug(f"Skipping candidate {tweet_id}: Too old (posted {parsed_timestamp.date()}).")
        return False

    # Optional: Add engagement checks here if needed later
    # engagement = tweet.get("engagement", {})
    # if engagement.get("retweets", 0) < 10 and engagement.get("likes", 0) < 20:
    #    log.debug(f"Skipping candidate {tweet_id}: Low engagement.")
    #    return False

    return True

def process_and_correct_tweet(candidate_tweets, already_corrected_ids):
    """
    Selects the best candidate, attempts to post a correction, and handles errors.
    Tries the next best candidate if a tweet-specific error occurs.

    Args:
        candidate_tweets (list): List of tweet dictionaries from the scraper.
        already_corrected_ids (list): List of tweet IDs already corrected today.

    Returns:
        str: The tweet ID of the successfully corrected tweet, or None if no correction
             was made in this cycle (due to errors or no suitable candidates).
    """
    if not candidate_tweets:
        log.info("No candidates provided for processing.")
        return None

    # 1. Filter valid candidates
    valid_candidates = [
        tweet for tweet in candidate_tweets
        if _is_valid_candidate(tweet, already_corrected_ids)
    ]
    log.info(f"Processing {len(valid_candidates)} valid candidates (out of {len(candidate_tweets)} fetched).")

    if not valid_candidates:
        log.info("No valid candidates found after filtering.")
        return None

    # 2. Sort candidates (e.g., most recent first)
    # Sorting by parsed_timestamp (datetime object) descending
    valid_candidates.sort(key=lambda t: t["parsed_timestamp"], reverse=True)
    log.debug(f"Sorted valid candidates. Top candidate ID: {valid_candidates[0]['tweet_id']} @ {valid_candidates[0]['parsed_timestamp']}")

    # 3. Iterate and attempt correction
    corrected_tweet_id = None
    for candidate in valid_candidates:
        tweet_id = candidate["tweet_id"]
        incorrect = candidate["error_found"]["incorrect"]
        correct = candidate["error_found"]["correct"]

        log.info(f"Attempting correction for candidate: ID {tweet_id}, Error: '{incorrect}' -> '{correct}'")
        correction_message = f"❌ {incorrect}\n✅ {correct}"
        log.debug(f"Correction message: \"{correction_message.replace(chr(10), ' ')}\"") # Replace newline for compact log

        success, error_type = _post_correction_reply_internal(tweet_id, correction_message)

        if success:
            log.info(f"Successfully posted correction for tweet {tweet_id}.")
            corrected_tweet_id = tweet_id
            break # Exit loop on first success

        elif error_type == "tweet_specific_error":
            log.warning(f"Skipping candidate {tweet_id} due to tweet-specific issue (e.g., protected, deleted, duplicate). Trying next.")
            continue # Try the next candidate in the sorted list

        elif error_type in ["api_error", "internal_error"]:
            log.error(f"Stopping correction attempts this cycle due to non-tweet-specific error ({error_type}) for tweet {tweet_id}.")
            # Do not try further candidates in *this cycle* if it's likely an API or internal issue
            corrected_tweet_id = None # Ensure no ID is returned
            break # Exit the loop for this cycle

        else: # Should not happen, but safety break
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

def manage_interval_and_run():
    """Main control loop for the bot."""
    log.info("===========================================")
    log.info("Starting Bot Main Loop")
    log.info(f"Daily Correction Limit: {DAILY_CORRECTION_LIMIT}")
    log.info(f"Debug Mode: {DEBUG_MODE}")
    log.info("===========================================")

    try:
        base_interval_s = SECONDS_IN_DAY / DAILY_CORRECTION_LIMIT if DAILY_CORRECTION_LIMIT > 0 else SECONDS_IN_DAY
        log.info(f"Target base interval between successful cycles: ~{base_interval_s / 60:.1f} minutes")
    except ZeroDivisionError:
        base_interval_s = SECONDS_IN_DAY
        log.warning("DAILY_CORRECTION_LIMIT is 0, setting interval to 24 hours.")


    while True:
        start_time = time.monotonic()
        current_time_utc = datetime.now(timezone.utc)
        today_date_obj = current_time_utc.date() # Use date object for file naming
        today_str = today_date_obj.strftime("%Y-%m-%d") # String for file names

        corrections_made_today_ids = get_corrections_made_today()
        corrections_count = len(corrections_made_today_ids)
        log.info(f"--- Starting Cycle Check ({current_time_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}) ---")
        log.info(f"Corrections made on {today_str}: {corrections_count}/{DAILY_CORRECTION_LIMIT}")

        corrected_in_this_cycle = False
        if corrections_count < DAILY_CORRECTION_LIMIT:
            log.info("Daily limit not reached. Proceeding with scrape and correct.")

            # 1. Scrape
            fetched_tweets = asyncio.run(scrape_tweets(ERROR_PAIRS)) # Run the async scraper

            # 2. Process & Correct (if tweets were found)
            if fetched_tweets:
                log.info(f"Scraper found {len(fetched_tweets)} candidates. Processing...")
                corrected_tweet_id = process_and_correct_tweet(fetched_tweets, corrections_made_today_ids)

                if corrected_tweet_id:
                    # 3. Log Correction Success
                    corrections_made_today_ids.append(corrected_tweet_id)
                    corrections_log_file = f"corrections_{today_str}.json"
                    if not save_json_file(corrections_log_file, corrections_made_today_ids):
                        log.critical(f"CRITICAL: Failed to save updated corrections log '{corrections_log_file}'. Duplicate corrections possible!")
                    else:
                        log.info(f"Successfully logged correction for {corrected_tweet_id}.")
                        corrected_in_this_cycle = True # Mark success for interval calculation
            else:
                log.info("Scraper did not return any candidates.")

            # Calculate sleep time for next cycle
            jitter = random.uniform(-MAX_INTERVAL_JITTER_S, MAX_INTERVAL_JITTER_S)
            # Use the base interval, adjusted by jitter
            sleep_duration_s = max(MIN_SLEEP_BETWEEN_CYCLES_S, base_interval_s + jitter)
            log.info(f"Cycle finished. Base interval sleep: {sleep_duration_s:.0f} seconds.")

        else: # Daily limit reached
            log.info(f"Daily correction limit ({DAILY_CORRECTION_LIMIT}) reached for {today_str}.")
            # Calculate time until midnight UTC + buffer
            try:
                 next_day_start_utc = datetime.combine(today_date_obj + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
                 seconds_until_next_day = (next_day_start_utc - current_time_utc).total_seconds()
                 # Sleep until slightly after midnight (add 1-5 min random buffer)
                 sleep_duration_s = max(MIN_SLEEP_BETWEEN_CYCLES_S, seconds_until_next_day + random.randint(60, 300))
                 log.info(f"Sleeping until after midnight UTC (approx {sleep_duration_s / 3600:.2f} hours).")
            except Exception as e:
                 # Fallback sleep if date calculation fails
                 log.error(f"Error calculating time until midnight: {e}. Sleeping for 1 hour as fallback.")
                 sleep_duration_s = 3600

        # Ensure minimum sleep time even if calculations result in less
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
        log.info("Bot script terminated.")
# --- End Script Entry Point ---