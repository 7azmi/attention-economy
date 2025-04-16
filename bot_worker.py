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
import math # For score calculation (exp)
from datetime import date, datetime, timedelta, timezone
from pathlib import Path # Using pathlib for easier path handling
from typing import List, Dict, Tuple, Optional, Set

# --- Third-Party Libraries ---
import tweepy
from playwright.async_api import async_playwright, Page, Browser, PlaywrightContextManager
from dotenv import load_dotenv

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="Twitter Correction Bot Worker")
parser.add_argument("bot_id", choices=['grammar', 'english'], help="Identifier for the bot type ('grammar' or 'english')")
args = parser.parse_args()
BOT_ID = args.bot_id
# --- End Argument Parsing ---


# --- Configuration Loading ---
load_dotenv()

# --- Define Error Pairs (Moved before Config for clarity) ---
ERROR_PAIRS_GRAMMAR: List[Tuple[str, str]] = [
    ("انشاء الله", "إن شاء الله"), ("إنشاء الله", "إن شاء الله"), ("لاكن", "لكن"),
    ("ضلم", "ظلم"), ("ضالم", "ظالم"), ("خطاء", "خطأ"), ("هاذا", "هذا"),
    # Add more grammar pairs here
]
ERROR_PAIRS_ENGLISH: List[Tuple[str, str]] = [
    ("ميتنج", "اجتماع"), ("ميتنغ", "اجتماع"), ("ميتنق", "اجتماع"),
    ("انفايت", "دعوة"), ("إنفايت", "دعوة"), ("انڤايت", "دعوة"), ("إنڤايت", "دعوة"),
    ("إيڤينت", "حدث"), ("ايڤينت", "حدث"), ("اڤينت", "حدث"), ("ايڤنت", "حدث"),
    ("ايفنت", "حدث"), ("إيفنت", "حدث"), ("إيفينت", "حدث"), ("إفينت", "حدث"), ("افينت", "حدث"),
    ("برفكت", "مثالي"), ("بيرفكت", "مثالي"), ("بيرفيكت", "مثالي"), ("برفيكت", "مثالي"),
    ("بروجكت", "مشروع"), ("داتا", "بيانات"), ("الداتا", "البيانات"),
    ("اللوقو", "الشعار"), ("اللوجو", "الشعار"), ("اللوغو", "الشعار"),
    ("البانر", "اللافتة"),
    ("فانزات", "معجبين | مشجعين | هواة"),
    ("امبوستر", "مزيف | مُنتحِل | محتال | نصاب"),
    ("الداون تاون", "وسط المدينة | منتصف المدينة"), ("داون تاون", "وسط المدينة | منتصف المدينة"),
    ("ستيكر", "ملصق | لاصق"), ("ستكر", "ملصق | لاصق"), ("الستكر", "ملصق | لاصق"), ("الستيكر", "ملصق | لاصق"),
    ("البوست", "المنشور"),
    ("فرايز", "بطاطس مقلية"),
    ("اوردر", "طلب"),
    ("البايك", "الدراجة"),
    ("الكارير", "المسيرة المهنية"), ("كارير", "مسيرة مهنية"),
    ("سكانر", "ماسح ضوئي"),
    ("باكيت", "علبة | عبوة | رزمة"),
    ("المانجر", "المدير"),
    ("الكيبورد", "لوحة المفاتيح"),
    ("برافو", "أحسنت"),
    ("الباور", "الطاقة | قوة"),
]
# --- End Error Pairs ---

class Config:
    """Holds bot configuration."""
    def __init__(self, bot_id: str):
        self.bot_id = bot_id.upper()
        self.debug_mode = os.getenv("DEBUG_MODE", "False").lower() in ("true", "1", "t") # Default Debug to False
        self.log_level = logging.DEBUG if self.debug_mode else logging.INFO

        # Credentials (ensure they exist using validate_credentials later)
        self.api_key = os.getenv(f"API_KEY_{self.bot_id}")
        self.api_secret = os.getenv(f"API_SECRET_{self.bot_id}")
        self.bearer_token = os.getenv(f"BEARER_TOKEN_{self.bot_id}")
        self.access_token = os.getenv(f"ACCESS_TOKEN_{self.bot_id}")
        self.access_token_secret = os.getenv(f"ACCESS_TOKEN_SECRET_{self.bot_id}")

        # Operational Parameters
        self.daily_correction_limit = int(os.getenv(f"DAILY_LIMIT_{self.bot_id}", 15))
        self.min_engagement_query = os.getenv(f"MIN_ENGAGEMENT_{self.bot_id}", "(min_retweets:50 OR min_faves:100)")
        self.max_tweet_age_days = int(os.getenv("MAX_TWEET_AGE_DAYS", 2))
        self.scrape_max_tweets_per_cycle = int(os.getenv("SCRAPE_MAX_TWEETS_PER_CYCLE", 300))
        self.scraper_timeout_ms = int(os.getenv("SCRAPER_TIMEOUT_S", 120)) * 1000 # Playwright uses ms
        self.max_interval_jitter_s = int(os.getenv("MAX_INTERVAL_JITTER_S", 300))
        self.min_sleep_between_cycles_s = int(os.getenv("MIN_SLEEP_BETWEEN_CYCLES_S", 60))
        self.max_processed_history_size = int(os.getenv("MAX_PROCESSED_QUEUE_SIZE", 500))

        # Nitter & Scraping Settings
        self.nitter_instances = [
            "https://nitter.net", "https://nitter.privacydev.net",
            "https://nitter.poast.org", "https://nitter.cz",
            # Add more reliable instances if needed
        ]
        self.search_chunk_size = int(os.getenv("SEARCH_CHUNK_SIZE", 7))

        # *** FIX: Ensure score_age_decay_k is initialized ***
        self.score_age_decay_k = float(os.getenv("SCORE_AGE_DECAY_K", 1.5))

        # Paths
        project_root = Path(__file__).parent
        self.state_dir = Path(os.getenv("PERSISTENT_DATA_DIR", project_root / "temp_data"))
        self.state_filename = self.state_dir / f"bot_id_{bot_id.lower()}.json"

        # Error Pairs (Assigned based on bot_id)
        self.error_pairs = ERROR_PAIRS_GRAMMAR if bot_id.lower() == 'grammar' else ERROR_PAIRS_ENGLISH

    def validate_credentials(self) -> List[str]:
        """Checks if all necessary Twitter API credentials are present."""
        missing = []
        creds = {
            f"API_KEY_{self.bot_id}": self.api_key,
            f"API_SECRET_{self.bot_id}": self.api_secret,
            f"BEARER_TOKEN_{self.bot_id}": self.bearer_token,
            f"ACCESS_TOKEN_{self.bot_id}": self.access_token,
            f"ACCESS_TOKEN_SECRET_{self.bot_id}": self.access_token_secret,
        }
        for name, value in creds.items():
            if not value:
                missing.append(name)
        return missing

# --- Instantiate Configuration ---
config = Config(BOT_ID)

# --- Logging Setup ---
log_filename = f"bot_log_{BOT_ID}_{date.today().strftime('%Y-%m-%d')}.log"
# Ensure log directory exists if state_dir is used for logs too (or define a separate log dir)
log_dir = config.state_dir # Assuming logs go in the same persistent data dir
log_dir.mkdir(parents=True, exist_ok=True)
log_filepath = log_dir / log_filename

logging.basicConfig(
    level=config.log_level,
    format=f'%(asctime)s - %(levelname)s - [{config.bot_id}] - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filepath, encoding='utf-8'), # Use full path
        logging.StreamHandler()
    ]
)
log = logging.getLogger(f"bot_worker.{BOT_ID}")
log.info(f"Logging initialized. Level: {logging.getLevelName(config.log_level)}. Log file: {log_filepath}")
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
        self._processed_ids_list: List[str] = [] # Order matters for trimming
        self._processed_ids_set: Set[str] = set() # Fast lookups
        self.corrections_today_count: int = 0
        self.last_reset_date: date = date.min # Initialize to a very old date
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
                content = f.read().strip()
                if not content:
                    log.warning(f"State file is empty: {self.filepath}. Initializing fresh state.")
                    self._initialize_empty_state()
                    self.save()
                    return
                data = json.loads(content)

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

            today = date.today()
            if self.last_reset_date < today:
                log.info(f"Date changed ({self.last_reset_date} -> {today}). Resetting daily correction count.")
                self.corrections_today_count = 0
                self.last_reset_date = today
            else:
                self.corrections_today_count = loaded_count

            self._processed_ids_list = [str(id_val) for id_val in loaded_ids if id_val]
            self._processed_ids_set = set(self._processed_ids_list)
            self._trim_history()

            log.info(f"State loaded. Daily count: {self.corrections_today_count} ({self.last_reset_date}). History size: {len(self._processed_ids_set)}.")

        except json.JSONDecodeError:
            log.error(f"Invalid JSON in state file: {self.filepath}. Backing up and initializing fresh state.")
            self._backup_corrupt_file()
            self._initialize_empty_state()
            self.save()
        except Exception as e:
            log.critical(f"Failed to load state {self.filepath}: {e}. Initializing fresh state.", exc_info=self.config.debug_mode)
            self._backup_corrupt_file() # Attempt backup even on generic load errors
            self._initialize_empty_state()
            self.save()

    def _backup_corrupt_file(self):
        """Creates a timestamped backup of a potentially corrupt state file."""
        if self.filepath.exists():
            try:
                # Ensure state_dir exists before creating backup path
                self.config.state_dir.mkdir(parents=True, exist_ok=True)
                backup_path = self.filepath.with_suffix(f".corrupt_{int(time.time())}.json")
                shutil.move(str(self.filepath), str(backup_path))
                log.info(f"Backed up potentially corrupt state file to: {backup_path}")
            except Exception as backup_e:
                log.error(f"Could not back up state file {self.filepath}: {backup_e}")

    def save(self) -> bool:
        """Saves the current state to the JSON file atomically."""
        log.debug(f"Attempting to save state to: {self.filepath}")
        # Ensure state_dir exists before saving
        try:
            self.config.state_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            log.error(f"Could not ensure state directory exists before saving: {e}. Save will likely fail.")
            return False

        state_data = {
            "last_reset_date": self.last_reset_date.isoformat(),
            "corrections_today_count": self.corrections_today_count,
            "processed_ids": self._processed_ids_list
        }
        temp_filepath = self.filepath.with_suffix(".tmp")
        try:
            with open(temp_filepath, "w", encoding="utf-8") as f:
                json.dump(state_data, f, ensure_ascii=False, indent=4)
            shutil.move(str(temp_filepath), str(self.filepath))
            log.debug(f"State saved successfully ({len(self._processed_ids_list)} IDs).")
            return True
        except Exception as e:
            log.error(f"Failed to save state to {self.filepath}: {e}", exc_info=config.debug_mode)
            if temp_filepath.exists():
                try: temp_filepath.unlink()
                except OSError: pass
            return False

    def _trim_history(self):
        """Removes oldest entries from history if max size is exceeded."""
        removed_count = 0
        while len(self._processed_ids_list) > self.max_history:
            removed_id = self._processed_ids_list.pop(0)
            self._processed_ids_set.discard(removed_id)
            removed_count += 1
        if removed_count > 0:
            log.debug(f"Trimmed {removed_count} oldest IDs from history (new size: {len(self._processed_ids_list)}).")

    def add_processed(self, tweet_id: str):
        """Marks a tweet ID as processed (attempted). Saves state."""
        tweet_id = str(tweet_id)
        if tweet_id not in self._processed_ids_set:
            log.debug(f"Adding tweet ID {tweet_id} to processed history.")
            self._processed_ids_list.append(tweet_id)
            self._processed_ids_set.add(tweet_id)
            self._trim_history()
            if not self.save():
                 log.critical(f"CRITICAL: Failed to save state after adding processed ID {tweet_id}!")
        # else:
            # log.debug(f"Tweet ID {tweet_id} is already in processed history.") # Less verbose

    def increment_daily_count(self):
        """Increments the daily correction count. Saves state."""
        today = date.today()
        if self.last_reset_date != today:
            log.warning("Date changed during increment. Resetting count first.")
            self.corrections_today_count = 0
            self.last_reset_date = today

        self.corrections_today_count += 1
        log.info(f"Daily correction count incremented to {self.corrections_today_count}/{self.config.daily_correction_limit}")
        if not self.save():
             log.critical(f"CRITICAL: Failed to save state after incrementing daily count!")

    def has_processed(self, tweet_id: str) -> bool:
        """Checks if a tweet ID is in the recent processed history."""
        return str(tweet_id) in self._processed_ids_set

    def is_limit_reached(self) -> bool:
        """Checks if the daily correction limit has been reached for today."""
        today = date.today()
        if self.last_reset_date != today:
             log.info(f"Date changed during limit check ({self.last_reset_date} -> {today}). Resetting count.")
             self.corrections_today_count = 0
             self.last_reset_date = today
             if not self.save():
                  log.error("Failed to save state after date change check.")
             return False # Limit not reached for the new day

        return self.corrections_today_count >= self.config.daily_correction_limit
# --- End Bot State Management ---


# --- Tweepy Client Initialization ---
missing_creds = config.validate_credentials()
if missing_creds:
    log.critical(f"Missing Twitter API credentials in .env: {', '.join(missing_creds)}. Exiting.")
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
    log.critical(f"Unexpected error initializing Tweepy client: {e}", exc_info=config.debug_mode)
    exit(1)
# --- End Tweepy Client Initialization ---


# --- Helper Functions ---
def extract_number(text: Optional[str]) -> int:
    """Extracts a number (possibly with K/M suffix) from text."""
    if not text: return 0
    text = text.replace(",", "").strip()
    match = re.search(r"([\d,.]+)([KM]?)", text, re.IGNORECASE)
    if not match: return 0
    try:
        num_str = match.group(1).replace(',', '')
        num = float(num_str)
        suffix = match.group(2).upper() if match.group(2) else ""
        if suffix == "K": return int(num * 1000)
        if suffix == "M": return int(num * 1000000)
        return int(num)
    except ValueError:
        return 0

def parse_tweet_timestamp(timestamp_str: Optional[str]) -> Optional[datetime]:
    """Parses Nitter's timestamp format into a timezone-aware datetime object."""
    if not timestamp_str: return None
    try:
        timestamp_str = re.sub(r'\s+', ' ', timestamp_str).strip()
        parts = timestamp_str.split('·')
        if len(parts) != 2: raise ValueError("Timestamp format incorrect: Missing '·' separator")

        date_part = parts[0].strip()
        time_part_full = parts[1].strip()

        time_match = re.match(r"(\d{1,2}:\d{2}\s+(?:AM|PM))(?:\s+(.*))?", time_part_full, re.IGNORECASE)
        if not time_match: raise ValueError("Timestamp format incorrect: Could not parse time")

        time_value_str = time_match.group(1)
        timezone_str = (time_match.group(2) or "UTC").upper()

        dt_str = f"{date_part} {time_value_str}"
        try:
            tweet_time_naive = datetime.strptime(dt_str, "%b %d, %Y %I:%M %p")
        except ValueError:
            tweet_time_naive = datetime.strptime(dt_str, "%d %b %Y %I:%M %p")

        if timezone_str != "UTC":
             log.warning(f"Non-UTC timezone '{timezone_str}' detected: '{timestamp_str}'. Assuming UTC.")

        return tweet_time_naive.replace(tzinfo=timezone.utc)

    except (ValueError, TypeError, IndexError) as e:
        log.debug(f"Could not parse timestamp '{timestamp_str}'. Error: {e}")
        return None

def chunk_list(data: list, size: int) -> list:
    """Yields successive chunks of a list."""
    if size <= 0:
        yield data
        return
    for i in range(0, len(data), size):
        yield data[i:i + size]

# --- End Helper Functions ---


# --- Core Function 1: Scraper ---
async def _extract_tweet_data_async(item, error_pairs_in_chunk: List[Tuple[str, str]], connected_instance_url: str) -> Optional[Dict]:
    """Extracts structured data from a single Nitter tweet HTML element, matching errors from the specific chunk."""
    tweet_link_element = None # Define outside try for logging context
    tweet_id = "unknown"
    try:
        # Query essential elements first
        tweet_link_element = await item.query_selector("a.tweet-link")
        if not tweet_link_element: return None
        tweet_text_element = await item.query_selector("div.tweet-content")
        if not tweet_text_element: return None

        tweet_text = (await tweet_text_element.inner_text()).strip()
        if tweet_text.startswith("RT @") or not tweet_text:
            return None

        # Check for errors from the current chunk
        found_error = next(
            (
                {"incorrect": incorrect, "correct": correct}
                for incorrect, correct in error_pairs_in_chunk
                if re.search(r"\b" + re.escape(incorrect) + r"\b", tweet_text, re.IGNORECASE | re.UNICODE)
            ),
            None,
        )
        if not found_error:
            return None

        # Extract remaining data only if an error was found
        tweet_link_raw = await tweet_link_element.get_attribute("href")
        tweet_link = urllib.parse.urljoin(connected_instance_url, tweet_link_raw) if tweet_link_raw else None
        tweet_id_match = re.search(r"/(?:status|statuses)/(\d+)", tweet_link or "")
        if tweet_id_match:
            tweet_id = tweet_id_match.group(1)

        username_element = await item.query_selector("a.username")
        timestamp_element = await item.query_selector("span.tweet-date a")
        if not username_element or not timestamp_element:
             log.debug(f"[{tweet_id}] Skipping item: Missing username or timestamp.")
             return None

        username = (await username_element.inner_text()).strip().lstrip('@')
        timestamp_str = (await timestamp_element.get_attribute("title") or await timestamp_element.inner_text()).strip()
        parsed_timestamp = parse_tweet_timestamp(timestamp_str)
        if not parsed_timestamp:
             log.debug(f"Skipping tweet {tweet_id}: Invalid timestamp '{timestamp_str}'.")
             return None

        # --- CORRECTED Engagement Stats Extraction (using old working logic) ---
        replies, retweets, likes, quotes = 0, 0, 0, 0
        # Only log this line once if debugging the start of extraction
        # log.debug(f"[{tweet_id}] Attempting to extract stats using corrected logic...")
        stats_elements = await item.query_selector_all("div.tweet-stats .tweet-stat")
        if not stats_elements and config.debug_mode: # Log only in debug if no stats found
            log.debug(f"[{tweet_id}] No '.tweet-stat' elements found.")

        for stat_element in stats_elements:
            stat_text = ""
            stat_value = 0
            icon_class = ""
            assigned_to = "none"
            try:
                icon_container = await stat_element.query_selector("div.icon-container")
                if icon_container:
                    stat_text = await icon_container.inner_text()
                    stat_value = extract_number(stat_text)

                    icon = await icon_container.query_selector("span[class^='icon-'], i[class^='icon-']")
                    if icon:
                        icon_class = await icon.get_attribute("class") or ""
                        # Assign based on icon class
                        if any(k in icon_class for k in ["comment", "reply", "bubble"]):
                            replies = stat_value
                            assigned_to = "replies"
                        elif any(k in icon_class for k in ["retweet", "recycle"]):
                            retweets = stat_value
                            assigned_to = "retweets"
                        elif any(k in icon_class for k in ["heart", "like", "favorite"]):
                            likes = stat_value
                            assigned_to = "likes"
                        elif "quote" in icon_class:
                            quotes = stat_value
                            assigned_to = "quotes"
                        elif config.debug_mode and stat_value > 0: # Log only if debug and value > 0
                            log.debug(f"[{tweet_id}] Stat value {stat_value} extracted but icon class '{icon_class}' not matched.")

                        if config.debug_mode: # Log details only in debug mode
                             log.debug(f"[{tweet_id}] Stat Raw Text='{stat_text}', Extracted Value={stat_value}, Icon Class='{icon_class}', Assigned: {assigned_to} = {stat_value}")

                    elif config.debug_mode: # Log only if debug
                        # Fallback attempt (less reliable) if icon missing but container exists
                        log.debug(f"[{tweet_id}] Icon container found, but no specific icon element found within. Text was: '{stat_text}'")
                        text_lower = stat_text.lower()
                        if ("comment" in text_lower or "repl" in text_lower) and replies == 0: replies = stat_value
                        elif "retweet" in text_lower and retweets == 0: retweets = stat_value
                        elif ("like" in text_lower or "heart" in text_lower or "favorite" in text_lower) and likes == 0: likes = stat_value
                        elif "quote" in text_lower and quotes == 0: quotes = stat_value
                elif config.debug_mode: # Log only if debug
                     log.debug(f"[{tweet_id}] No 'div.icon-container' found for a stat element.")

            except Exception as e:
                 # Log only in debug mode
                if config.debug_mode:
                    log.debug(f"Minor error extracting individual stat for tweet {tweet_id}: {e}", exc_info=False)
        # --- END CORRECTED Engagement Stats Extraction ---

        # Log final results only if debugging
        if config.debug_mode:
            log.debug(f"[{tweet_id}] Final engagement extracted: R:{replies}, RT:{retweets}, L:{likes}, Q:{quotes}")

        return {
            "username": username, "timestamp_str": timestamp_str, "parsed_timestamp": parsed_timestamp,
            "tweet": tweet_text, "link": tweet_link, "tweet_id": tweet_id,
            "error_found": found_error,
            "engagement": {"replies": replies, "retweets": retweets, "likes": likes, "quotes": quotes},
        }
    except Exception as e:
        current_tweet_id = tweet_id if 'tweet_id' in locals() and tweet_id != "unknown" else 'unknown'
        log.warning(f"Error processing tweet element for ID {current_tweet_id}: {e}", exc_info=config.debug_mode)
        if tweet_link_element:
             try:
                  faulty_link = await tweet_link_element.get_attribute('href')
                  log.warning(f"Faulty element link (approx): {faulty_link}")
             except: pass
        return None

async def scrape_tweets(config: Config) -> List[Dict]:
    """Scrapes Nitter using chunked queries for tweets containing specified errors."""
    log.info(f"Starting chunked tweet scraping process (Chunk Size: {config.search_chunk_size})...")

    all_fetched_tweets: List[Dict] = []
    processed_tweet_ids_this_scrape: Set[str] = set()

    error_pair_chunks = list(chunk_list(config.error_pairs, config.search_chunk_size))
    total_chunks = len(error_pair_chunks)
    log.info(f"Divided {len(config.error_pairs)} error pairs into {total_chunks} chunks.")

    async with async_playwright() as p:
        browser = None
        context = None
        try:
            browser = await p.firefox.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36", # Keep UA reasonably updated
                java_script_enabled=True,
                viewport={'width': 1920, 'height': 1080} # Set a common viewport
            )
            await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            for i, chunk in enumerate(error_pair_chunks):
                if not chunk: continue
                chunk_num = i + 1
                log.info(f"--- Processing Chunk {chunk_num}/{total_chunks} ---")

                incorrect_words_query = " OR ".join([f'"{pair[0]}"' for pair in chunk])
                base_query = f"({incorrect_words_query}) {config.min_engagement_query} lang:ar -filter:retweets -filter:replies"
                encoded_query = urllib.parse.quote(base_query)
                search_url_template = "/search?f=tweets&q={query}&since=&until=&near="
                if config.debug_mode: log.debug(f"Chunk {chunk_num} Query: {base_query}")

                connected_instance = None
                page = None

                try:
                    page = await context.new_page()
                    for instance in config.nitter_instances:
                        search_url = instance + search_url_template.format(query=encoded_query)
                        log.info(f"Chunk {chunk_num}: Trying instance {instance}")
                        try:
                            await page.goto(search_url, timeout=45000, wait_until="domcontentloaded")
                            await page.wait_for_selector("div.timeline .timeline-item, div.error-panel, div.timeline div:text('No results found')", timeout=30000)

                            no_results_or_error = await page.query_selector("div.error-panel, div.timeline div:text('No results found')")
                            if no_results_or_error:
                                error_text = await no_results_or_error.inner_text()
                                log.warning(f"Chunk {chunk_num}: Instance {instance} reported: {error_text.strip()}")
                                await asyncio.sleep(0.5)
                                continue

                            if not await page.query_selector("div.timeline .timeline-item"):
                                log.warning(f"Chunk {chunk_num}: Instance {instance} loaded but no timeline items found (unexpected).")
                                await asyncio.sleep(0.5)
                                continue

                            connected_instance = instance
                            log.info(f"Chunk {chunk_num}: Successfully connected to {instance}.")
                            break

                        except Exception as e:
                            log.warning(f"Chunk {chunk_num}: Failed/timed out on {instance}: {type(e).__name__}") # Less verbose error
                            if config.debug_mode: log.debug(f"Instance {instance} failure details: {e}") # Details only in debug
                            await asyncio.sleep(random.uniform(0.5, 1.5))

                    if not connected_instance:
                        log.error(f"Chunk {chunk_num}: Could not retrieve results from any Nitter instance.")
                        continue

                    await page.wait_for_timeout(random.randint(1500, 3000))
                    await page.evaluate('window.scrollBy(0, document.body.scrollHeight / 4)') # Scroll down a bit
                    await page.wait_for_timeout(random.randint(500, 1500))

                    tweet_elements = await page.query_selector_all("div.timeline > div.timeline-item:not(.show-more)")
                    log.info(f"Chunk {chunk_num}: Found {len(tweet_elements)} potential elements on {connected_instance}.")

                    tasks = [_extract_tweet_data_async(item, chunk, connected_instance) for item in tweet_elements]
                    results = await asyncio.gather(*tasks)

                    chunk_added_count = 0
                    for tweet_data in results:
                        if tweet_data and tweet_data["tweet_id"] not in processed_tweet_ids_this_scrape:
                            if len(all_fetched_tweets) < config.scrape_max_tweets_per_cycle:
                                all_fetched_tweets.append(tweet_data)
                                processed_tweet_ids_this_scrape.add(tweet_data["tweet_id"])
                                chunk_added_count += 1
                            else:
                                log.info(f"Reached scrape cycle limit ({config.scrape_max_tweets_per_cycle}) during chunk {chunk_num}.")
                                break

                    log.info(f"Chunk {chunk_num}: Added {chunk_added_count} new unique candidates.")
                    if len(all_fetched_tweets) >= config.scrape_max_tweets_per_cycle:
                         log.info(f"Total scrape limit reached after chunk {chunk_num}. Stopping scrape.")
                         break

                except Exception as chunk_e:
                    log.error(f"Error during processing of chunk {chunk_num}: {chunk_e}", exc_info=config.debug_mode)
                finally:
                    if page:
                        try: await page.close()
                        except Exception: pass # Ignore errors closing page

        except Exception as e:
            log.error(f"Major error during Playwright setup or execution: {e}", exc_info=config.debug_mode)
        finally:
            if browser:
                try: await browser.close()
                except Exception: pass # Ignore errors closing browser

    log.info(f"Scraping finished. Found {len(all_fetched_tweets)} total unique candidates across all chunks.")
    return all_fetched_tweets
# --- End Core Function 1 ---


# --- Core Function 2: Process and Correct ---
def _post_correction_reply_internal(tweet_id: str, correction_message: str, tweepy_client: tweepy.Client) -> Tuple[bool, str]:
    """Internal function to post the reply using Tweepy."""
    if not (tweet_id and correction_message and tweepy_client):
        log.error("Cannot post reply: tweet_id, message, or client missing.")
        return False, "internal_error"
    try:
        log.debug(f"Attempting Tweepy v2 reply to tweet {tweet_id}")
        response = tweepy_client.create_tweet(text=correction_message, in_reply_to_tweet_id=tweet_id)

        if response and response.data and "id" in response.data:
            log.info(f"Successfully replied to {tweet_id}. Reply tweet ID: {response.data['id']}")
            return True, "success"
        else:
            log.error(f"Failed reply to {tweet_id}. Unexpected API response: {response}")
            return False, "api_error"

    except tweepy.errors.Forbidden as e:
        error_str = str(e).lower()
        if "duplicate content" in error_str:
             log.warning(f"Reply forbidden for {tweet_id} (403 - Duplicate): {e}")
             return False, "tweet_specific_error_duplicate"
        elif any(phrase in error_str for phrase in ["not allowed to", "cannot reply", "user suspended", "protected", "cannot perform this action"]):
             log.warning(f"Reply forbidden for {tweet_id} (403 - Restriction): {e}")
             return False, "tweet_specific_error_restriction"
        else:
             log.error(f"Unhandled Forbidden error (403) replying to {tweet_id}: {e}", exc_info=config.debug_mode)
             return False, "api_error"

    except tweepy.errors.NotFound as e:
        log.warning(f"Failed reply to {tweet_id} (Tweet Not Found - 404): {e}")
        return False, "tweet_specific_error_deleted"

    except tweepy.errors.TweepyException as e:
        log.error(f"TweepyException replying to {tweet_id}: {e}", exc_info=config.debug_mode)
        return False, "api_error"

    except Exception as e:
        log.error(f"Unexpected internal error replying to {tweet_id}: {e}", exc_info=True)
        return False, "internal_error"

def _is_valid_candidate(tweet_data: Dict, bot_state: BotState, config: Config) -> bool:
    """Checks if a scraped tweet is a valid candidate for correction."""
    tweet_id = tweet_data.get("tweet_id")
    parsed_timestamp = tweet_data.get("parsed_timestamp")
    error_info = tweet_data.get("error_found")

    if not all([tweet_id, parsed_timestamp, error_info]):
        log.debug(f"Skipping candidate: Missing essential data.")
        return False
    if not isinstance(parsed_timestamp, datetime):
        log.debug(f"Skipping {tweet_id}: Invalid timestamp type.")
        return False
    if not isinstance(error_info, dict) or "incorrect" not in error_info or "correct" not in error_info:
         log.debug(f"Skipping {tweet_id}: Invalid error_info.")
         return False

    if bot_state.has_processed(tweet_id):
        return False

    now_utc = datetime.now(timezone.utc)
    if (now_utc - parsed_timestamp).days > config.max_tweet_age_days:
        log.debug(f"Skipping {tweet_id}: Too old ({parsed_timestamp.date()}).")
        return False

    return True

def _calculate_score(tweet_data: Dict, config: Config, now_utc: datetime) -> float:
    """
    Calculates an engagement score, factoring in tweet age for prioritization.
    Uses exponential decay: score = raw_engagement * exp(-k * age_in_hours).
    """
    engagement = tweet_data.get("engagement", {})
    likes = engagement.get("likes", 0)
    retweets = engagement.get("retweets", 0)
    quotes = engagement.get("quotes", 0)
    parsed_timestamp = tweet_data.get("parsed_timestamp")
    tweet_id = tweet_data.get("tweet_id", "N/A") # For logging context

    raw_score = float(likes + (retweets * 1.5) + (quotes * 0.5))

    decay_factor = 1.0
    if isinstance(parsed_timestamp, datetime):
        try:
            tweet_age = now_utc - parsed_timestamp
            tweet_age_hours = max(0.0, tweet_age.total_seconds() / 3600.0)

            # Retrieve k from the config object
            decay_k = config.score_age_decay_k
            decay_factor = math.exp(-decay_k * tweet_age_hours)

            if config.debug_mode:
                 log.debug(f"[{tweet_id}] Raw Score: {raw_score:.2f}, Age (hrs): {tweet_age_hours:.2f}, Decay Factor (k={decay_k}): {decay_factor:.4f}")

        except AttributeError:
            # This handles the case where score_age_decay_k might be missing during development/testing
            log.error(f"[{tweet_id}] Config object missing 'score_age_decay_k' attribute! Using decay=0.1", exc_info=False)
            decay_factor = 0.1
        except Exception as e:
            log.warning(f"Error calculating age decay for tweet {tweet_id}: {e}", exc_info=False)
            decay_factor = 0.1 # Penalize if age calculation fails
    else:
         log.warning(f"Missing or invalid timestamp for tweet {tweet_id}. Using decay factor 1.0.")

    final_score = raw_score * decay_factor
    if config.debug_mode:
         log.debug(f"[{tweet_id}] Final Score: {final_score:.2f}")
    return final_score

def process_and_correct_tweet(candidate_tweets: List[Dict], bot_state: BotState, tweepy_client: tweepy.Client, config: Config) -> Optional[str]:
    """
    Filters candidates, scores them, selects the best, attempts correction, and updates state.
    Returns the ID of the corrected tweet if successful, otherwise None.
    """
    if not candidate_tweets:
        log.info("No candidates provided for processing.")
        return None

    # 1. Filter candidates
    valid_candidates = [
        t for t in candidate_tweets
        if _is_valid_candidate(t, bot_state, config)
    ]
    log.info(f"Processing {len(valid_candidates)} valid candidates (after filtering {len(candidate_tweets)} scraped).")

    if not valid_candidates:
        log.info("No valid, unprocessed candidates found this cycle.")
        return None

    # 2. Score and Sort valid candidates
    now_utc = datetime.now(timezone.utc)
    for candidate in valid_candidates:
        candidate['score'] = _calculate_score(candidate, config, now_utc)

    valid_candidates.sort(key=lambda t: t.get('score', 0.0), reverse=True)

    # Check if score_age_decay_k exists before logging it
    decay_k_log = f"k={config.score_age_decay_k}" if hasattr(config, 'score_age_decay_k') else "k=N/A"
    log.info(f"Top candidates by time-weighted score ({decay_k_log}):")
    for i, c in enumerate(valid_candidates[:5]):
        log.info(f"  {i+1}. ID: {c['tweet_id']}, Score: {c['score']:.2f}, User: @{c['username']}, Error: '{c['error_found']['incorrect']}'")
        if config.debug_mode: # Log raw engagement only in debug
            log.info(f"     Raw Engagement: R:{c['engagement']['replies']}, RT:{c['engagement']['retweets']}, L:{c['engagement']['likes']}, Q:{c['engagement']['quotes']}")

    # 3. Attempt correction on the highest-scoring valid candidates
    corrected_tweet_id = None
    for candidate in valid_candidates:
        tweet_id = candidate["tweet_id"]
        incorrect = candidate["error_found"]["incorrect"]
        correct = candidate["error_found"]["correct"]
        username = candidate["username"]
        score = candidate["score"]

        log.info(f"Attempting correction for high-priority tweet {tweet_id} (Score: {score:.2f}) by @{username}: '{incorrect}' -> '{correct}'")

        bot_state.add_processed(tweet_id)

        correction_message = f"❌ {incorrect}\n✅ {correct}"
        if config.debug_mode: log.debug(f"Correction message for {tweet_id}: \"{correction_message.replace(chr(10), ' / ')}\"")

        success, error_type = _post_correction_reply_internal(tweet_id, correction_message, tweepy_client)

        if success:
            log.info(f"Correction successful for {tweet_id}.")
            bot_state.increment_daily_count()
            corrected_tweet_id = tweet_id
            break

        elif error_type.startswith("tweet_specific_error"):
            log.warning(f"Skipping {tweet_id} due to tweet-specific issue ({error_type}). Trying next.")
            continue

        elif error_type in ["api_error", "internal_error"]:
            log.error(f"Stopping correction attempts this cycle due to non-tweet-specific error ({error_type}) on tweet {tweet_id}.")
            corrected_tweet_id = None
            break
        else:
             log.error(f"Unknown error type '{error_type}' from reply function for {tweet_id}. Stopping cycle.")
             corrected_tweet_id = None
             break

    if corrected_tweet_id:
        log.info(f"Correction cycle finished. Successfully corrected tweet ID: {corrected_tweet_id}")
    elif valid_candidates:
         log.info("Correction cycle finished. No candidate was successfully corrected this cycle.")

    return corrected_tweet_id
# --- End Core Function 2 ---


# --- Core Function 3: Main Loop Logic ---
def run_bot_cycle(bot_state: BotState, tweepy_client: tweepy.Client, config: Config):
    """Runs a single cycle of the bot: check limit, scrape (chunked), process (scored)."""
    start_time_mono = time.monotonic()
    current_time_utc = datetime.now(timezone.utc)

    log.info(f"--- Cycle Start ({current_time_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}) ---")
    log.info(f"Daily Count: {bot_state.corrections_today_count}/{config.daily_correction_limit}. History Size: {len(bot_state._processed_ids_set)}.")

    if not bot_state.is_limit_reached():
        log.info("Daily limit OK. Proceeding with scrape and process.")

        try:
            fetched_tweets = asyncio.run(scrape_tweets(config))
        except Exception as scrape_err:
            log.error(f"Error occurred during scrape_tweets execution: {scrape_err}", exc_info=config.debug_mode)
            fetched_tweets = []

        if fetched_tweets:
            process_and_correct_tweet(fetched_tweets, bot_state, tweepy_client, config)
        else:
            log.info("Scraper returned no candidates this cycle.")

    else:
        log.info(f"Daily correction limit ({config.daily_correction_limit}) reached for {bot_state.last_reset_date}. Skipping scrape/process.")

    # --- Calculate Sleep Duration ---
    cycle_duration = time.monotonic() - start_time_mono
    log.info(f"Cycle took {cycle_duration:.2f}s.")

    sleep_duration_s: float
    now_utc = datetime.now(timezone.utc) # Recalculate current time

    if bot_state.is_limit_reached():
        try:
            today_date_obj = now_utc.date()
            next_day_start_utc = datetime.combine(today_date_obj + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
            sleep_buffer_s = random.uniform(60, 300)
            seconds_until_next_run = (next_day_start_utc - now_utc).total_seconds() + sleep_buffer_s
            sleep_duration_s = max(config.min_sleep_between_cycles_s, seconds_until_next_run)
            log.info(f"Limit reached. Sleeping until after midnight UTC (~{(sleep_duration_s / 3600):.2f}h).")
        except Exception as e:
            log.error(f"Error calculating sleep until midnight: {e}. Sleeping for 1 hour fallback.", exc_info=config.debug_mode)
            sleep_duration_s = 3600.0
    else:
        remaining_limit = max(1, config.daily_correction_limit - bot_state.corrections_today_count)
        today_date_obj = now_utc.date()
        next_day_start_utc = datetime.combine(today_date_obj + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
        time_until_midnight_s = max(1.0, (next_day_start_utc - now_utc).total_seconds())

        base_interval_s = time_until_midnight_s / remaining_limit
        log.debug(f"Target interval: ~{base_interval_s / 60:.1f} min ({remaining_limit} left over {time_until_midnight_s / 3600:.1f}h)")

        jitter = random.uniform(-config.max_interval_jitter_s, config.max_interval_jitter_s)
        calculated_sleep = base_interval_s + jitter

        sleep_duration_s = max(config.min_sleep_between_cycles_s, calculated_sleep)
        log.info(f"Calculated sleep: {sleep_duration_s:.0f}s (Base: {base_interval_s:.0f}s, Jitter: {jitter:.0f}s)")

    log.info(f"--- Sleeping for {sleep_duration_s:.0f} seconds ---")
    time.sleep(sleep_duration_s)

# --- End Core Function 3 ---


# --- Script Entry Point ---
if __name__ == "__main__":
    log.info(f"================ Starting Bot Worker: {config.bot_id.upper()} ================")
    log.info(f"Daily Limit: {config.daily_correction_limit}, Min Engagement: {config.min_engagement_query}")
    log.info(f"Max Tweet Age: {config.max_tweet_age_days} days, Search Chunk Size: {config.search_chunk_size}")
    # Check attribute exists before logging
    decay_k_info = f"{config.score_age_decay_k}" if hasattr(config, 'score_age_decay_k') else "N/A (Check Config)"
    log.info(f"Score Age Decay K: {decay_k_info}")
    log.info(f"Debug Mode: {config.debug_mode}, Max History Size: {config.max_processed_history_size}")
    log.info(f"State File: {config.state_filename}")
    log.info(f"Loaded {len(config.error_pairs)} error pairs for mode '{config.bot_id.lower()}'.")
    log.info("===========================================================")

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
        # Add a small delay before exiting to allow logs to flush?
        time.sleep(2)
    finally:
        log.info(f"Bot worker process [{config.bot_id.upper()}] terminated.")
# --- End Script Entry Point ---