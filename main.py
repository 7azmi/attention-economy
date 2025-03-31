import json
import sys # Used for sys.executable
import time
from datetime import date, datetime, timedelta, timezone
import subprocess
import random
import logging
from dotenv import load_dotenv

# --- Logging Setup ---
# (Setup logging as before - ensures logs go to file and console)
log_filename = f"bot_log_{date.today().strftime('%Y-%m-%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)
# --- End Logging Setup ---

# --- Configuration Constants ---
# Define core operational parameters clearly
DAILY_CORRECTION_LIMIT = 10     # Target number of corrections per day
SCRAPER_TIMEOUT_S = 120       # Max time for scraper script (seconds)
TWEETER_TIMEOUT_S = 60        # Max time for tweeter script (seconds)
MAX_INTERVAL_JITTER_S = 300   # Max random variation +/- for cycle interval (seconds)
MIN_SLEEP_BETWEEN_CYCLES_S = 60 # Minimum time to wait before next cycle (seconds)
SECONDS_IN_DAY = 24 * 60 * 60 # For interval calculation
# --- End Configuration Constants ---

# --- Helper Functions ---

def run_script(script_name, timeout_seconds):
    """Runs a python script using subprocess with a timeout and logs output."""
    log.info(f"Running script: {script_name} (Timeout: {timeout_seconds}s)...")
    try:
        process = subprocess.run(
            [sys.executable, script_name], # Use the same python interpreter
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8',
            timeout=timeout_seconds
        )
        log.debug(f"Script {script_name} finished successfully.")
        # Log output only if present
        stdout = process.stdout.strip()
        stderr = process.stderr.strip()
        if stdout: log.debug(f"{script_name} STDOUT:\n{stdout}")
        if stderr: log.warning(f"{script_name} STDERR:\n{stderr}") # Log stderr as warning
        return True
    except subprocess.CalledProcessError as e:
        log.error(f"Error running {script_name}. Return code: {e.returncode}")
        stdout = e.stdout.strip() if e.stdout else ""
        stderr = e.stderr.strip() if e.stderr else ""
        if stdout: log.error(f"{script_name} STDOUT:\n{stdout}")
        if stderr: log.error(f"{script_name} STDERR:\n{stderr}")
        return False
    except subprocess.TimeoutExpired as e:
        log.error(f"Error running {script_name}: Script timed out after {timeout_seconds} seconds.")
        # Log output captured before timeout if available
        stdout = e.stdout.strip() if isinstance(e.stdout, str) else ""
        stderr = e.stderr.strip() if isinstance(e.stderr, str) else ""
        if stdout: log.error(f"{script_name} STDOUT (before timeout):\n{stdout}")
        if stderr: log.error(f"{script_name} STDERR (before timeout):\n{stderr}")
        return False
    except FileNotFoundError:
         log.error(f"Error running {script_name}: Script file not found.")
         return False
    except Exception as e:
        log.error(f"Unexpected error running {script_name}: {e}", exc_info=True)
        return False

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
        log.error(f"Error loading {filename}: {e}. Returning default.", exc_info=True)
        return default

def get_corrections_made_today():
    """Gets the count of corrections made today from the log file."""
    today_str = date.today().strftime("%Y-%m-%d")
    corrections_file = f"corrections_{today_str}.json"
    corrections_list = load_json_file(corrections_file, default=[])

    if isinstance(corrections_list, list):
        return len(corrections_list)
    else:
        log.warning(f"Corrections file {corrections_file} contained invalid data. Assuming 0 corrections and attempting to reset.")
        # Try to reset the file to an empty list
        try:
            with open(corrections_file, "w", encoding="utf-8") as f:
                json.dump([], f)
        except IOError as e:
            log.error(f"Failed to reset corrupt corrections file {corrections_file}: {e}")
        return 0

# --- Main Application Logic ---

def main_loop():
    """Main control loop for the scraper and tweeter bot."""
    log.info("===========================================")
    log.info("Starting Main Application Loop")
    log.info(f"Daily Correction Limit: {DAILY_CORRECTION_LIMIT}")
    log.info("===========================================")

    # Calculate base interval aiming to spread checks across the day
    try:
        base_interval_s = SECONDS_IN_DAY / DAILY_CORRECTION_LIMIT if DAILY_CORRECTION_LIMIT > 0 else SECONDS_IN_DAY
        log.info(f"Target interval between cycles: ~{base_interval_s / 60:.1f} minutes")
    except ZeroDivisionError:
        base_interval_s = SECONDS_IN_DAY # Default to 24h if limit is 0
        log.warning("DAILY_CORRECTION_LIMIT is 0, setting interval to 24 hours.")


    while True:
        current_time_utc = datetime.now(timezone.utc)
        today_date = current_time_utc.date()

        corrections_count = get_corrections_made_today()
        log.info(f"Starting cycle check. Corrections made on {today_date.strftime('%Y-%m-%d')}: {corrections_count}/{DAILY_CORRECTION_LIMIT}")

        if corrections_count < DAILY_CORRECTION_LIMIT:
            log.info("Daily limit not reached. Running scraper...")
            scraper_success = run_script("scraper.py", timeout_seconds=SCRAPER_TIMEOUT_S)

            if scraper_success:
                tweets_found = load_json_file("tweets_to_correct.json", default=[])
                if isinstance(tweets_found, list) and tweets_found:
                    log.info(f"Scraper found {len(tweets_found)} candidates. Running tweeter...")
                    run_script("tweeter.py", timeout_seconds=TWEETER_TIMEOUT_S)
                else:
                    log.info("Scraper ran successfully but found no tweet candidates.")
            else:
                log.error("Scraper script failed. Skipping tweeter for this cycle.")

            # Calculate sleep time: base interval +/- random jitter
            jitter = random.uniform(-MAX_INTERVAL_JITTER_S, MAX_INTERVAL_JITTER_S)
            sleep_duration_s = max(MIN_SLEEP_BETWEEN_CYCLES_S, base_interval_s + jitter)
            log.info(f"Cycle finished. Sleeping for {sleep_duration_s:.0f} seconds.")

        else: # Daily limit reached
            log.info(f"Daily correction limit ({DAILY_CORRECTION_LIMIT}) reached for {today_date.strftime('%Y-%m-%d')}.")
            # Calculate time until midnight UTC + small buffer
            midnight_utc = datetime.combine(today_date + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
            seconds_until_midnight = (midnight_utc - current_time_utc).total_seconds()

            # Sleep until slightly after midnight (add 1-5 min random buffer)
            sleep_duration_s = max(MIN_SLEEP_BETWEEN_CYCLES_S, seconds_until_midnight + random.randint(60, 300))
            log.info(f"Sleeping until after midnight UTC (approx {sleep_duration_s / 3600:.2f} hours).")

        time.sleep(sleep_duration_s)

# --- Script Entry Point ---

if __name__ == "__main__":
    load_dotenv() # Load .env variables (e.g., for tweeter API keys)
    try:
        main_loop()
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt received. Shutting down gracefully.")
    except Exception as e:
        log.critical(f"An uncaught exception occurred in the main loop: {e}", exc_info=True)
    finally:
        log.info("Main application loop terminated.")