# bot_worker.py
import asyncio
import json
import re
import logging
import urllib.parse
import os
import time
import random
import argparse
import shutil # For safe file saving
from datetime import date, datetime, timedelta, timezone
from pathlib import Path # Using pathlib for easier path handling

# --- Third-Party Libraries ---
import tweepy
from playwright.async_api import async_playwright
from dotenv import load_dotenv

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="Twitter Correction Bot Worker")
parser.add_argument("bot_id", choices=['grammar', 'english'], help="Identifier for the bot type ('grammar' or 'english')")
args = parser.parse_args()
BOT_ID = args.bot_id
# --- End Argument Parsing ---


# --- Configuration Loading ---
load_dotenv()

class Config:
    """Holds bot configuration."""
    def __init__(self, bot_id):
        self.bot_id = bot_id.upper()
        self.debug_mode = os.getenv("DEBUG_MODE", "True").lower() in ("true", "1", "t")
        self.log_level = logging.DEBUG if self.debug_mode else logging.INFO

        # Credentials
        self.api_key = os.getenv(f"API_KEY_{self.bot_id}")
        self.api_secret = os.getenv(f"API_SECRET_{self.bot_id}")
        self.bearer_token = os.getenv(f"BEARER_TOKEN_{self.bot_id}")
        self.access_token = os.getenv(f"ACCESS_TOKEN_{self.bot_id}")
        self.access_token_secret = os.getenv(f"ACCESS_TOKEN_SECRET_{self.bot_id}")

        # Operational Parameters
        self.daily_correction_limit = int(os.getenv(f"DAILY_LIMIT_{self.bot_id}", 15))
        self.min_engagement_query = os.getenv(f"MIN_ENGAGEMENT_{self.bot_id}", "(min_retweets:50 OR min_faves:100)")
        self.max_tweet_age_days = int(os.getenv("MAX_TWEET_AGE_DAYS", 2))
        self.scrape_max_tweets = int(os.getenv("SCRAPE_MAX_TWEETS", 30))
        self.scraper_timeout_s = int(os.getenv("SCRAPER_TIMEOUT_S", 120)) * 1000 # Playwright uses ms
        self.tweeter_timeout_s = int(os.getenv("TWEETER_TIMEOUT_S", 60)) # Not directly used by Tweepy v2 client creation?
        self.max_interval_jitter_s = int(os.getenv("MAX_INTERVAL_JITTER_S", 300))
        self.min_sleep_between_cycles_s = int(os.getenv("MIN_SLEEP_BETWEEN_CYCLES_S", 60))
        self.max_processed_history_size = int(os.getenv("MAX_PROCESSED_QUEUE_SIZE", 200))

        self.nitter_instances = [
            "https://nitter.net", "https://nitter.privacydev.net",
            "https://nitter.poast.org", "https://nitter.cz",
            # Add more reliable instances if needed
        ]

        project_root = Path(__file__).parent  # Gets the directory where bot_worker.py is
        self.state_dir = Path(os.getenv("PERSISTENT_DATA_DIR", project_root / "temp_data"))

        self.state_filename = self.state_dir / f"state_{bot_id.lower()}.json"

        # Error Pairs (Defined later based on bot_id)
        self.error_pairs = []

    def validate_credentials(self):
        """Checks if all necessary Twitter API credentials are present."""
        missing = []
        if not self.api_key: missing.append(f"API_KEY_{self.bot_id}")
        if not self.api_secret: missing.append(f"API_SECRET_{self.bot_id}")
        if not self.bearer_token: missing.append(f"BEARER_TOKEN_{self.bot_id}")
        if not self.access_token: missing.append(f"ACCESS_TOKEN_{self.bot_id}")
        if not self.access_token_secret: missing.append(f"ACCESS_TOKEN_SECRET_{self.bot_id}")
        return missing

# --- Instantiate Configuration ---
config = Config(BOT_ID)

# --- Define Error Pairs Based on BOT_ID ---
ERROR_PAIRS_GRAMMAR = [
    ("انشاء الله", "إن شاء الله"), ("إنشاء الله", "إن شاء الله"), ("لاكن", "لكن"),
    ("ضلم", "ظلم"), ("ضالم", "ظالم"), ("خطاء", "خطأ"), ("هاذا", "هذا"),
]
ERROR_PAIRS_ENGLISH = [
    ("ميتنج", "اجتماع"), ("ميتنغ", "اجتماع"), ("ميتنق", "اجتماع"),
    ("انفايت", "دعوة"), ("إنفايت", "دعوة"), ("انڤايت", "دعوة"), ("إنڤايت", "دعوة"),
    ("إيڤينت", "حدث"), ("ايڤينت", "حدث"), ("اڤينت", "حدث"), ("ايڤنت", "حدث"),
    ("ايفنت", "حدث"), ("إيفنت", "حدث"), ("إيفينت", "حدث"), ("إفينت", "حدث"), ("افينت", "حدث"),
    ("برفكت", "مثالي"), ("بيرفكت", "مثالي"), ("بيرفيكت", "مثالي"), ("برفيكت", "مثالي"),
    ("بروجكت", "مشروع"), ("داتا", "بيانات"), ("الداتا", "البيانات"),

    # إضافات جديدة
    ("اللوقو", "الشعار"), ("اللوجو", "الشعار"), ("اللوغو", "الشعار"),
    ("البانر", "اللافتة"),
    ("فانزات", "معجبين | مشجعين | هواة"),
    ("امبوستر", "مزيف | مُنتحِل | محتال | نصاب"),
    ("الداون تاون", "وسط المدينة | منتصف المدينة"), ("داون تاون", "وسط المدينة | منتصف المدينة"),
    ("ستيكر", "ملصق | لاصق"), ("ستكر", "ملصق | لاصق"),
]


config.error_pairs = ERROR_PAIRS_GRAMMAR if BOT_ID == 'grammar' else ERROR_PAIRS_ENGLISH
# --- End Error Pairs ---

# --- Logging Setup ---
log_filename = f"bot_log_{BOT_ID}_{date.today().strftime('%Y-%m-%d')}.log"
logging.basicConfig(
    level=config.log_level,
    format=f'%(asctime)s - %(levelname)s - [{config.bot_id}] - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(f"bot_worker.{BOT_ID}")
log.info(f"Logging initialized. Level: {logging.getLevelName(config.log_level)}")
# --- End Logging Setup ---


# --- Ensure State Directory Exists ---
try:
    config.state_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"Ensured state directory exists: {config.state_dir}")
except OSError as e:
    log.critical(f"Could not create state directory {config.state_dir}: {e}. Exiting.")
    exit(1)
# --- End Directory Check ---


# --- Bot State Management ---
class BotState:
    """Manages the persistent state of the bot (processed IDs, daily counts)."""
    def __init__(self, config: Config):
        self.config = config
        self.filepath = config.state_filename
        self.max_history = config.max_processed_history_size
        self._processed_ids_list = [] # Order matters for trimming
        self._processed_ids_set = set() # Fast lookups
        self.corrections_today_count = 0
        self.last_reset_date = date.min # Initialize to a very old date

        self.load()

    def _initialize_empty_state(self):
        """Sets default values for a fresh state."""
        self._processed_ids_list = []
        self._processed_ids_set = set()
        self.corrections_today_count = 0
        self.last_reset_date = date.today() # Start fresh today
        log.info("Initialized new empty bot state.")

    def load(self):
        """Loads state from the JSON file."""
        log.debug(f"Attempting to load state from: {self.filepath}")
        try:
            if not self.filepath.exists():
                log.warning(f"State file not found: {self.filepath}. Initializing fresh state.")
                self._initialize_empty_state()
                self.save() # Save the initial state
                return

            with open(self.filepath, "r", encoding="utf-8") as f:
                content = f.read()
                if not content:
                    log.warning(f"State file is empty: {self.filepath}. Initializing fresh state.")
                    self._initialize_empty_state()
                    self.save()
                    return

                data = json.loads(content)

            # Validate loaded data
            if not isinstance(data, dict):
                 raise ValueError("State file root is not a dictionary.")

            loaded_date_str = data.get("last_reset_date")
            loaded_count = data.get("corrections_today_count", 0)
            loaded_ids = data.get("processed_ids", [])

            if not isinstance(loaded_count, int) or loaded_count < 0:
                 log.warning(f"Invalid 'corrections_today_count' in state file. Resetting to 0.")
                 loaded_count = 0
            if not isinstance(loaded_ids, list):
                 log.warning(f"Invalid 'processed_ids' in state file (not a list). Resetting to empty list.")
                 loaded_ids = []

            try:
                 self.last_reset_date = date.fromisoformat(loaded_date_str) if loaded_date_str else date.min
            except (ValueError, TypeError):
                 log.warning(f"Invalid or missing 'last_reset_date' in state file. Resetting daily count.")
                 self.last_reset_date = date.min # Force reset below

            # Check if we need to reset the daily counter
            today = date.today()
            if self.last_reset_date < today:
                log.info(f"Date changed ({self.last_reset_date} -> {today}). Resetting daily correction count.")
                self.corrections_today_count = 0
                self.last_reset_date = today
            else:
                self.corrections_today_count = loaded_count

            # Load processed IDs, ensuring they are strings
            self._processed_ids_list = [str(id_val) for id_val in loaded_ids if id_val] # Ensure strings
            self._processed_ids_set = set(self._processed_ids_list)

            # Trim if loaded list exceeds max size (shouldn't happen with proper saving, but safe)
            self._trim_history()

            log.info(f"State loaded successfully. {self.corrections_today_count} corrections made today ({self.last_reset_date}). {len(self._processed_ids_set)} IDs in recent history.")

        except json.JSONDecodeError:
            log.error(f"Invalid JSON found in state file: {self.filepath}. Backing up and initializing fresh state.")
            self._backup_corrupt_file()
            self._initialize_empty_state()
            self.save()
        except Exception as e:
            log.critical(f"Failed to load state file {self.filepath}: {e}. Initializing fresh state.", exc_info=self.config.debug_mode)
            # Potentially back up file here too
            self._initialize_empty_state()
            # Attempt to save the fresh state, but failure might indicate deeper issues
            self.save()

    def _backup_corrupt_file(self):
        """Creates a timestamped backup of a corrupt state file."""
        if self.filepath.exists():
            try:
                backup_path = self.filepath.with_suffix(f".corrupt_{int(time.time())}.json")
                shutil.move(str(self.filepath), str(backup_path))
                log.info(f"Backed up corrupt state file to: {backup_path}")
            except Exception as backup_e:
                log.error(f"Could not back up corrupt state file {self.filepath}: {backup_e}")

    def save(self) -> bool:
        """Saves the current state to the JSON file atomically."""
        log.debug(f"Attempting to save state to: {self.filepath}")
        state_data = {
            "last_reset_date": self.last_reset_date.isoformat(),
            "corrections_today_count": self.corrections_today_count,
            "processed_ids": self._processed_ids_list # Save the ordered list
        }
        temp_filepath = self.filepath.with_suffix(".tmp")
        try:
            # Write to temporary file first
            with open(temp_filepath, "w", encoding="utf-8") as f:
                json.dump(state_data, f, ensure_ascii=False, indent=4)

            # Atomically replace the original file
            shutil.move(str(temp_filepath), str(self.filepath))
            log.debug(f"State saved successfully to {self.filepath}")
            return True
        except (IOError, OSError, Exception) as e:
            log.error(f"Failed to save state to {self.filepath}: {e}", exc_info=self.config.debug_mode)
            # Clean up temp file if it exists
            if temp_filepath.exists():
                try:
                    temp_filepath.unlink()
                except OSError:
                    pass # Ignore cleanup error
            return False

    def _trim_history(self):
        """Removes oldest entries from history if max size is exceeded."""
        removed_count = 0
        while len(self._processed_ids_list) > self.max_history:
            removed_id = self._processed_ids_list.pop(0) # Remove from the front (oldest)
            self._processed_ids_set.discard(removed_id) # Remove from set too
            removed_count += 1
        if removed_count > 0:
            log.debug(f"Trimmed {removed_count} oldest IDs from processed history (new size: {len(self._processed_ids_list)}).")

    def add_processed(self, tweet_id: str):
        """Marks a tweet ID as processed (attempted). Saves state."""
        tweet_id = str(tweet_id) # Ensure string
        if tweet_id not in self._processed_ids_set:
            log.debug(f"Adding tweet ID {tweet_id} to processed history.")
            self._processed_ids_list.append(tweet_id) # Add to end (most recent)
            self._processed_ids_set.add(tweet_id)
            self._trim_history() # Trim if needed *after* adding
            if not self.save():
                 # Log critical failure, but maybe don't stop the bot?
                 # Or potentially raise an exception if saving state is absolutely critical
                 log.critical(f"CRITICAL: Failed to save state after adding processed ID {tweet_id}!")
        else:
            log.debug(f"Tweet ID {tweet_id} is already in processed history.")

    def increment_daily_count(self):
        """Increments the daily correction count. Saves state."""
        if self.last_reset_date != date.today():
            log.warning("Attempted to increment daily count, but date has changed. Resetting count first.")
            self.corrections_today_count = 0
            self.last_reset_date = date.today()

        self.corrections_today_count += 1
        log.info(f"Daily correction count incremented to {self.corrections_today_count}/{self.config.daily_correction_limit}")
        if not self.save():
             log.critical(f"CRITICAL: Failed to save state after incrementing daily count!")

    def has_processed(self, tweet_id: str) -> bool:
        """Checks if a tweet ID is in the recent processed history."""
        return str(tweet_id) in self._processed_ids_set

    def is_limit_reached(self) -> bool:
        """Checks if the daily correction limit has been reached for today."""
        # Also reset if date changed
        if self.last_reset_date != date.today():
             log.info(f"Date changed during limit check ({self.last_reset_date} -> {date.today()}). Resetting count.")
             self.corrections_today_count = 0
             self.last_reset_date = date.today()
             if not self.save():
                  log.error("Failed to save state after date change check.")
             return False # Limit not reached for the new day

        return self.corrections_today_count >= self.config.daily_correction_limit
# --- End Bot State Management ---


# --- Tweepy Client Initialization ---
missing_creds = config.validate_credentials()
if missing_creds:
    log.critical(f"Twitter API credentials missing! Check .env file for: {', '.join(missing_creds)}. Exiting.")
    exit(1)

tweepy_client = None
try:
    tweepy_client = tweepy.Client(
        bearer_token=config.bearer_token,
        consumer_key=config.api_key,
        consumer_secret=config.api_secret,
        access_token=config.access_token,
        access_token_secret=config.access_token_secret,
        wait_on_rate_limit=True,
    )
    auth_user = tweepy_client.get_me()
    log.info(f"Tweepy Client (v2) initialized successfully for @{auth_user.data.username}")
except tweepy.errors.TweepyException as e:
    log.critical(f"Failed to initialize Tweepy client: {e}", exc_info=config.debug_mode)
    exit(1)
except Exception as e:
    log.critical(f"Unexpected error during Tweepy client initialization: {e}", exc_info=config.debug_mode)
    exit(1)
# --- End Tweepy Client Initialization ---


# --- Helper Functions ---
def extract_number(text):
    """Extracts a number (possibly with K/M suffix) from text."""
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
    """Parses Nitter's timestamp format into a timezone-aware datetime object."""
    try:
        # Example: "May 10, 2024 · 10:30 AM UTC"
        timestamp_str = re.sub(r'\s+', ' ', timestamp_str).strip()
        parts = timestamp_str.split('·')
        if len(parts) != 2: raise ValueError("Timestamp format incorrect: Missing '·' separator")
        date_part = parts[0].strip()
        time_part_full = parts[1].strip() # e.g., "10:30 AM UTC"

        # More robust time parsing
        time_match = re.match(r"(\d{1,2}:\d{2}\s+(?:AM|PM))(?:\s+(.*))?", time_part_full, re.IGNORECASE)
        if not time_match: raise ValueError("Timestamp format incorrect: Could not parse time")

        time_value_str = time_match.group(1)
        timezone_str = (time_match.group(2) or "UTC").upper() # Default to UTC if missing

        dt_str = f"{date_part} {time_value_str}" # e.g., "May 10, 2024 10:30 AM"
        tweet_time_naive = datetime.strptime(dt_str, "%b %d, %Y %I:%M %p") # Use %b for month abbr

        if timezone_str == "UTC":
            return tweet_time_naive.replace(tzinfo=timezone.utc)
        else:
            # Attempt to handle offset like "+0300" if needed in the future
            log.warning(f"Non-UTC timezone '{timezone_str}' detected: '{timestamp_str}'. Assuming UTC.")
            return tweet_time_naive.replace(tzinfo=timezone.utc)

    except (ValueError, TypeError, IndexError) as e:
        log.debug(f"Could not parse timestamp '{timestamp_str}'. Error: {e}")
        return None
# --- End Helper Functions ---


# --- Core Function 1: Scraper ---
async def _extract_tweet_data_async(item, error_pairs, connected_instance_url):
    """Extracts structured data from a single Nitter tweet HTML element."""
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

        username = (await username_element.inner_text()).strip().lstrip('@') # Remove leading @
        timestamp_str = (await timestamp_element.get_attribute("title") or await timestamp_element.inner_text()).strip()
        tweet_text = (await tweet_text_element.inner_text()).strip()

        # Basic Filtering
        if tweet_text.startswith("RT @"):
            log.debug(f"Skipping tweet {tweet_id}: Retweet.")
            return None
        # Add other filters? e.g., minimum length?

        # Find first matching error
        found_error = next(
            (
                {"incorrect": incorrect, "correct": correct}
                for incorrect, correct in error_pairs
                if re.search(r"\b" + re.escape(incorrect) + r"\b", tweet_text, re.IGNORECASE | re.UNICODE)
            ),
            None,
        )
        if not found_error: return None # Skip if no error found in this tweet

        # Extract Engagement Stats
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
                        # Check class names for different Nitter themes/versions
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
        log.warning(f"Error processing a tweet element: {e}", exc_info=config.debug_mode)
        return None

async def scrape_tweets(config: Config):
    """Scrapes Nitter for tweets containing specified errors."""
    log.info("Starting tweet scraping process...")
    incorrect_words_query = " OR ".join([f'"{pair[0]}"' for pair in config.error_pairs])
    base_query = f"({incorrect_words_query}) {config.min_engagement_query} lang:ar -filter:retweets -filter:replies"
    encoded_query = urllib.parse.quote(base_query)
    search_url_template = "/search?f=tweets&q={query}&since=&until=&near="
    log.info(f"Constructed base query: {base_query}")

    fetched_tweets = []
    processed_tweet_ids_this_scrape = set() # Avoid duplicates within a single scrape run
    connected_instance = None

    async with async_playwright() as p:
        browser = None # Define browser outside try block for potential closing in finally
        try:
            browser = await p.firefox.launch(headless=True) # Consider making headless configurable
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36", # Slightly updated UA
                java_script_enabled=True,
            )
            # Anti-bot detection measures
            await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            page = await context.new_page()

            # Try Nitter instances sequentially
            for instance in config.nitter_instances:
                search_url = instance + search_url_template.format(query=encoded_query)
                log.info(f"Trying Nitter instance: {instance}")
                try:
                    await page.goto(search_url, timeout=30000, wait_until="domcontentloaded") # 30s timeout
                    # Wait for either timeline items or an error message
                    await page.wait_for_selector("div.timeline .timeline-item, div.timeline span.error-panel", timeout=20000) # 20s wait

                    # Check for explicit "no results" or error panels
                    no_results = await page.query_selector("div.timeline span.error-panel, div.timeline div:text('No results found')") # More specific check
                    if no_results:
                        error_text = await no_results.inner_text()
                        log.warning(f"Instance {instance} returned no results/error: {error_text.strip()}")
                        await asyncio.sleep(0.5) # Small delay before trying next
                        continue # Try next instance

                    # Check if timeline items actually loaded
                    if not await page.query_selector("div.timeline .timeline-item"):
                         log.warning(f"Instance {instance} loaded but no timeline items found.")
                         await asyncio.sleep(0.5)
                         continue

                    connected_instance = instance
                    log.info(f"Successfully connected to {instance}.")
                    break # Exit loop on successful connection
                except Exception as e:
                    # Catch specific Playwright errors if needed (TimeoutError, etc.)
                    log.warning(f"Failed to load or timed out on {instance}: {e}")
                    await asyncio.sleep(1) # Longer delay on failure

            if not connected_instance:
                log.error("Could not retrieve results from any Nitter instance.")
                return [] # Return empty list

            # Give JS a moment to potentially load more content (optional)
            await page.wait_for_timeout(2000)

            # Select only direct timeline items, excluding 'show more' links etc.
            tweet_elements = await page.query_selector_all("div.timeline > .timeline-item:not(.show-more)")
            log.info(f"Found {len(tweet_elements)} potential tweet elements on {connected_instance}.")

            tasks = [_extract_tweet_data_async(item, config.error_pairs, connected_instance) for item in tweet_elements]
            results = await asyncio.gather(*tasks)

            # Filter out None results and duplicates from this scrape
            for tweet_data in results:
                if tweet_data and tweet_data["tweet_id"] not in processed_tweet_ids_this_scrape:
                    if len(fetched_tweets) < config.scrape_max_tweets:
                        fetched_tweets.append(tweet_data)
                        processed_tweet_ids_this_scrape.add(tweet_data["tweet_id"])
                        log.debug(f"Added candidate tweet {tweet_data['tweet_id']} from @{tweet_data['username']}")
                    else:
                        log.info(f"Reached scrape limit ({config.scrape_max_tweets}).")
                        break # Stop collecting more tweets

        except Exception as e:
            log.error(f"Error during Playwright scraping process: {e}", exc_info=config.debug_mode)
            return [] # Return empty on major scraping error
        finally:
            if browser:
                await browser.close()

    log.info(f"Scraping finished. Found {len(fetched_tweets)} unique candidates matching criteria.")
    return fetched_tweets
# --- End Core Function 1 ---


# --- Core Function 2: Process and Correct ---
def _post_correction_reply_internal(tweet_id, correction_message, tweepy_client):
    """Internal function to post the reply using Tweepy."""
    if not (tweet_id and correction_message and tweepy_client):
        log.error("Cannot post reply: tweet_id, message, or client missing.")
        return False, "internal_error"
    try:
        log.debug(f"Attempting to reply to tweet {tweet_id}")
        # Use V2 method
        response = tweepy_client.create_tweet(text=correction_message, in_reply_to_tweet_id=tweet_id)

        if response and response.data and "id" in response.data:
            log.info(f"Successfully replied to {tweet_id}. New tweet ID: {response.data['id']}")
            return True, None # Success
        else:
            # Log the raw response if debugging is needed
            log.error(f"Failed to reply to {tweet_id}. Unexpected API response format: {response}")
            return False, "api_error"

    except tweepy.errors.Forbidden as e:
        # Check specific error codes/messages if available in e.response or e.api_codes/messages
        error_str = str(e).lower()
        # Add more specific forbidden reasons if encountered
        forbidden_phrases = [
            "you are not allowed to", "cannot reply to protected", "user suspended",
            "unable to perform this action", "not allowed to create a tweet with duplicate content",
            "cannot send replies to users who are not following you", # Less common with V2 replies?
            "403 forbidden" # Generic fallback
        ]
        if any(phrase in error_str for phrase in forbidden_phrases) or (e.response and e.response.status_code == 403):
             log.warning(f"Reply forbidden/duplicate for {tweet_id} (403): {e}")
             return False, "tweet_specific_error" # Treat as skippable for this tweet
        else:
             log.error(f"Unhandled Forbidden error (403) replying to {tweet_id}: {e}", exc_info=config.debug_mode)
             return False, "api_error" # Treat as potentially systemic API issue

    except tweepy.errors.NotFound as e:
        log.warning(f"Failed to reply to {tweet_id} (Tweet Not Found - 404): {e}")
        return False, "tweet_specific_error" # Tweet deleted or ID incorrect

    except tweepy.errors.TweepyException as e:
        # Handle other potential Tweepy/Twitter API errors (e.g., rate limits - though client handles waits)
        log.error(f"Failed to reply (TweepyException) to {tweet_id}: {e}", exc_info=config.debug_mode)
        # Could check for specific error codes here if needed
        return False, "api_error" # Assume potential API issue

    except Exception as e:
        log.error(f"Unexpected error replying to {tweet_id}: {e}", exc_info=True)
        return False, "internal_error" # Bot's internal code error


def _is_valid_candidate(tweet_data, bot_state: BotState, config: Config) -> bool:
    """Checks if a scraped tweet is a valid candidate for correction."""
    tweet_id = tweet_data.get("tweet_id")
    parsed_timestamp = tweet_data.get("parsed_timestamp")
    error_info = tweet_data.get("error_found")

    # Basic data integrity checks
    if not all([tweet_id, parsed_timestamp, error_info]): return False
    if not isinstance(parsed_timestamp, datetime): return False # Must be datetime obj
    if not isinstance(error_info, dict): return False

    # Check 1: Already processed recently? (Using BotState)
    if bot_state.has_processed(tweet_id):
        log.debug(f"Skipping {tweet_id}: Found in recently processed history.")
        return False

    # Check 2: Too old?
    if (datetime.now(timezone.utc) - parsed_timestamp).days > config.max_tweet_age_days:
        log.debug(f"Skipping {tweet_id}: Too old ({parsed_timestamp.date()}, limit {config.max_tweet_age_days} days).")
        return False

    # Check 3: Check against bot's own username? (Prevent self-correction loops)
    # bot_username = os.getenv(f"BOT_USERNAME_{config.bot_id}") # Requires adding bot username to env
    # if bot_username and tweet_data.get("username", "").lower() == bot_username.lower():
    #     log.debug(f"Skipping {tweet_id}: Own tweet by {bot_username}.")
    #     return False

    # Add any other filtering rules here if needed

    return True # Passed all checks

def process_and_correct_tweet(candidate_tweets, bot_state: BotState, tweepy_client, config: Config):
    """
    Filters candidates, selects the best one, attempts correction, and updates state.
    Returns the ID of the corrected tweet if successful, otherwise None.
    """
    if not candidate_tweets:
        log.info("No candidates provided for processing.")
        return None

    # Filter candidates based on validity checks (age, processed history)
    valid_candidates = [
        t for t in candidate_tweets
        if _is_valid_candidate(t, bot_state, config)
    ]
    log.info(f"Processing {len(valid_candidates)} valid candidates (after filtering).")

    if not valid_candidates:
        log.info("No valid, unprocessed candidates found this cycle.")
        return None

    # Prioritize the most recent valid candidate
    valid_candidates.sort(key=lambda t: t["parsed_timestamp"], reverse=True)
    log.debug(f"Top candidate chosen: {valid_candidates[0]['tweet_id']} from @{valid_candidates[0]['username']} ({valid_candidates[0]['timestamp_str']})")

    corrected_tweet_id = None
    # Loop through sorted candidates (try newest first)
    for candidate in valid_candidates:
        tweet_id = candidate["tweet_id"]
        incorrect = candidate["error_found"]["incorrect"]
        correct = candidate["error_found"]["correct"]
        username = candidate["username"]

        log.info(f"Attempting correction for tweet {tweet_id} by @{username}: '{incorrect}' -> '{correct}'")

        # --- Step 1: Mark as processed BEFORE API call ---
        # This ensures we don't retry it immediately if the script crashes during the API call
        # The BotState class handles saving internally.
        bot_state.add_processed(tweet_id)

        # --- Step 2: Construct the reply message ---
        # Consider adding context or being gentler? Optional.
        correction_message = f"❌ {incorrect}\n✅ {correct}"
        log.debug(f"Correction message prepared for {tweet_id}: \"{correction_message.replace(chr(10), ' ')}\"")

        # --- Step 3: Attempt to post the reply ---
        success, error_type = _post_correction_reply_internal(tweet_id, correction_message, tweepy_client)

        # --- Step 4: Update state based on outcome ---
        if success:
            log.info(f"Correction successful for {tweet_id}.")
            bot_state.increment_daily_count() # Handles saving state internally
            corrected_tweet_id = tweet_id
            break # Exit loop on first successful correction

        elif error_type == "tweet_specific_error":
            # Logged in _post_correction_reply_internal
            log.warning(f"Skipping {tweet_id} due to tweet-specific issue ({error_type}). Trying next candidate if available.")
            # No state change needed here, already marked as processed
            continue # Try the next valid candidate

        elif error_type in ["api_error", "internal_error"]:
            # Logged in _post_correction_reply_internal
            log.error(f"Stopping correction attempts this cycle due to non-tweet-specific error ({error_type}) while processing {tweet_id}.")
            # No state change needed here, already marked as processed
            corrected_tweet_id = None # Ensure we don't report success
            break # Stop trying other candidates in this cycle due to systemic issue
        else:
             # Should not happen if error types are handled
             log.error(f"Unknown error type '{error_type}' returned processing {tweet_id}. Stopping cycle.")
             corrected_tweet_id = None
             break

    # --- Cycle Summary ---
    if corrected_tweet_id:
        log.info(f"Correction cycle finished successfully. Corrected Tweet ID: {corrected_tweet_id}")
    else:
        if valid_candidates: # Only log this if there were candidates to try
             log.info("Correction cycle finished. No suitable candidate was successfully corrected this cycle.")
        # If no valid_candidates, message logged earlier.

    return corrected_tweet_id # Return the ID if successful, None otherwise
# --- End Core Function 2 ---


# --- Core Function 3: Main Loop Logic ---
def run_bot_cycle(bot_state: BotState, tweepy_client, config: Config):
    """Runs a single cycle of the bot: check limit, scrape, process."""
    start_time_mono = time.monotonic()
    current_time_utc = datetime.now(timezone.utc)

    log.info(f"--- Cycle Start ({current_time_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}) ---")
    log.info(f"Daily Count: {bot_state.corrections_today_count}/{config.daily_correction_limit}. History Size: {len(bot_state._processed_ids_set)}.")

    corrected_this_cycle = False
    if not bot_state.is_limit_reached():
        log.info("Daily limit not reached. Proceeding with scrape and process.")

        # 1. Scrape for potential candidates
        # We run asyncio scraper in the main thread context here
        fetched_tweets = asyncio.run(scrape_tweets(config))

        # 2. Process candidates and attempt correction
        if fetched_tweets:
            corrected_id = process_and_correct_tweet(fetched_tweets, bot_state, tweepy_client, config)
            if corrected_id:
                corrected_this_cycle = True
                # Daily count and processed state are updated within process_and_correct_tweet
        else:
            log.info("Scraper returned no candidates.")

    else:
        log.info(f"Daily correction limit ({config.daily_correction_limit}) reached for {bot_state.last_reset_date}. Skipping scrape/process.")

    # Calculate time until next cycle
    cycle_duration = time.monotonic() - start_time_mono
    log.info(f"Cycle took {cycle_duration:.2f}s.")

    if bot_state.is_limit_reached():
        # Sleep until after midnight UTC
        try:
            today_date_obj = date.today() # Use current date
            next_day_start_utc = datetime.combine(today_date_obj + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
            # Add a small buffer past midnight (e.g., 1-5 minutes)
            sleep_buffer = random.randint(60, 300)
            seconds_until_next_run = (next_day_start_utc - current_time_utc).total_seconds() + sleep_buffer
            sleep_duration_s = max(config.min_sleep_between_cycles_s, seconds_until_next_run)
            log.info(f"Limit reached. Sleeping until after midnight UTC (~{sleep_duration_s / 3600:.2f}h).")
        except Exception as e:
            log.error(f"Error calculating sleep until midnight: {e}. Sleeping for 1 hour as fallback.", exc_info=config.debug_mode)
            sleep_duration_s = 3600
    else:
        # Calculate interval based on remaining limit (avoid division by zero)
        remaining_limit = config.daily_correction_limit - bot_state.corrections_today_count
        seconds_in_day = 24 * 60 * 60
        time_until_midnight = (datetime.combine(date.today() + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc) - current_time_utc).total_seconds()

        if remaining_limit > 0 and time_until_midnight > 0:
             base_interval_s = max(1.0, time_until_midnight / remaining_limit) # At least 1 second interval
             log.debug(f"Target interval based on remaining {remaining_limit} corrections over {time_until_midnight/3600:.1f}h: ~{base_interval_s / 60:.1f} min")
        else:
             # Fallback if no limit left or past midnight already
             base_interval_s = seconds_in_day / config.daily_correction_limit if config.daily_correction_limit > 0 else seconds_in_day
             log.debug(f"Using default base interval: ~{base_interval_s / 60:.1f} min")

        # Add jitter
        jitter = random.uniform(-config.max_interval_jitter_s, config.max_interval_jitter_s)
        calculated_sleep = base_interval_s + jitter

        # Ensure minimum sleep and prevent negative sleep
        sleep_duration_s = max(config.min_sleep_between_cycles_s, calculated_sleep)
        log.info(f"Calculated sleep duration: {sleep_duration_s:.0f}s (Base: {base_interval_s:.0f}s, Jitter: {jitter:.0f}s)")

    log.info(f"--- Sleeping for {sleep_duration_s:.0f} seconds ---")
    time.sleep(sleep_duration_s)

# --- End Core Function 3 ---


# --- Script Entry Point ---
if __name__ == "__main__":
    log.info(f"================ Starting Bot Worker: {config.bot_id} ================")
    log.info(f"Daily Limit: {config.daily_correction_limit}, Min Engagement: {config.min_engagement_query}")
    log.info(f"Debug Mode: {config.debug_mode}, Max History Size: {config.max_processed_history_size}")
    log.info(f"State File: {config.state_filename}")
    log.info("===========================================================")

    # Initialize state management (will load existing or create new)
    try:
        bot_state = BotState(config)
    except Exception as e:
        log.critical(f"Failed to initialize BotState: {e}. Cannot continue.", exc_info=True)
        exit(1)

    # Main execution loop
    try:
        while True:
            run_bot_cycle(bot_state, tweepy_client, config)
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt received. Shutting down gracefully.")
    except Exception as e:
        # Catch unexpected errors in the main loop itself
        log.critical(f"An uncaught exception occurred in the main loop: {e}", exc_info=True)
    finally:
        log.info(f"Bot worker process [{config.bot_id}] terminated.")
# --- End Script Entry Point ---