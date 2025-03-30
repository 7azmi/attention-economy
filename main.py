import asyncio
import json
import os
import time
from datetime import date, datetime, timedelta  # Added datetime
import subprocess
import random
import logging # Import logging

# --- Logging Setup ---
# Configure logging for the entire application
log_filename = f"bot_log_{date.today().strftime('%Y-%m-%d')}.log"
logging.basicConfig(
    level=logging.INFO, # Set default level (INFO, DEBUG, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', # Include logger name
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'), # Log to a daily file
        logging.StreamHandler() # Also log to console
    ]
)
log = logging.getLogger(__name__) # Get logger for this module
# --- End Logging Setup ---


# --- Constants ---
DAILY_TWEET_LIMIT = 10
HOURS_IN_DAY = 24
SECONDS_IN_HOUR = 3600
# Calculate base interval, ensure it's not zero
BASE_REPLY_INTERVAL = (HOURS_IN_DAY * SECONDS_IN_HOUR) // DAILY_TWEET_LIMIT if DAILY_TWEET_LIMIT > 0 else (HOURS_IN_DAY * SECONDS_IN_HOUR)
MAX_JITTER_SECONDS = 600 # Max random seconds to add/subtract
# --- End Constants ---


def run_script(script_name):
    """Runs a python script using subprocess and logs output."""
    log.info(f"Attempting to run script: {script_name}")
    try:
        process = subprocess.run(
            ["python", script_name],
            check=True, # Raise CalledProcessError on non-zero exit code
            capture_output=True, # Capture stdout/stderr
            text=True, # Decode output as text
            encoding='utf-8' # Ensure correct encoding
        )
        log.info(f"Script {script_name} finished successfully.")
        log.debug(f"{script_name} STDOUT:\n{process.stdout}")
        if process.stderr:
             log.warning(f"{script_name} STDERR:\n{process.stderr}")
        return True
    except subprocess.CalledProcessError as e:
        log.error(f"Error running {script_name}. Return code: {e.returncode}")
        log.error(f"{script_name} STDOUT:\n{e.stdout}")
        log.error(f"{script_name} STDERR:\n{e.stderr}")
        return False
    except FileNotFoundError:
         log.error(f"Error running {script_name}: Script file not found.")
         return False
    except Exception as e:
        log.error(f"Unexpected error running {script_name}: {e}", exc_info=True)
        return False


def get_corrections_made_count():
    """Gets the count of corrections made today from the log file."""
    today_str = date.today().strftime("%Y-%m-%d")
    corrections_file = f"corrections_{today_str}.json"
    try:
        with open(corrections_file, "r", encoding='utf-8') as f:
            corrections_list = json.load(f)
            if isinstance(corrections_list, list):
                return len(corrections_list)
            else:
                log.warning(f"{corrections_file} does not contain a list. Assuming 0 corrections.")
                return 0
    except FileNotFoundError:
        log.info(f"Corrections file {corrections_file} not found. Assuming 0 corrections today.")
        return 0
    except json.JSONDecodeError:
         log.error(f"Error decoding JSON from {corrections_file}. Assuming 0 corrections.")
         # Optional: backup/rename the corrupt file
         return 0
    except Exception as e:
         log.error(f"Error reading {corrections_file}: {e}. Assuming 0 corrections.", exc_info=True)
         return 0


# Note: save_corrections function removed as tweeter.py handles saving its own log.

def main():
    log.info("===========================================")
    log.info("Starting Main Application Loop")
    log.info(f"Daily Tweet Limit: {DAILY_TWEET_LIMIT}")
    log.info(f"Base Reply Interval: {BASE_REPLY_INTERVAL} seconds")
    log.info("===========================================")

    last_correction_check_day = date.today() # Track day for resetting count

    while True:
        current_day = date.today()
        # Check if the day has changed to reset the counter logic if needed
        if current_day != last_correction_check_day:
            log.info(f"Date changed to {current_day.strftime('%Y-%m-%d')}. Resetting daily logic if applicable.")
            last_correction_check_day = current_day
            # Update log file name for basicConfig (though handlers might need re-adding for complex setups)
            # For basicConfig, new file handler might be needed if process runs over midnight.
            # Simpler approach: Rely on the date check for counting.

        corrections_count = get_corrections_made_count()
        log.info(f"Corrections made today ({current_day.strftime('%Y-%m-%d')}): {corrections_count}/{DAILY_TWEET_LIMIT}")

        if corrections_count < DAILY_TWEET_LIMIT:
            log.info("Daily limit not reached. Proceeding with scrape and tweet cycle.")

            scraper_success = run_script("scraper.py")

            if scraper_success:
                # Optional short delay between scraper and tweeter
                time.sleep(5)
                tweeter_success = run_script("tweeter.py")

                if not tweeter_success:
                    log.error("Tweeter script failed.")
                # No need to update corrections_count here, tweeter logs its own success/failure
                # and the count will be re-checked at the start of the next loop.
            else:
                log.error("Scraper script failed. Skipping tweeter for this cycle.")

            # Calculate sleep time *after* a cycle attempt
            corrections_count_after = get_corrections_made_count() # Check count again in case tweeter succeeded
            if corrections_count_after < DAILY_TWEET_LIMIT:
                # Apply jitter: random number between -MAX_JITTER/2 and +MAX_JITTER/2
                jitter = random.randint(-MAX_JITTER_SECONDS // 2, MAX_JITTER_SECONDS // 2)
                sleep_duration = max(30, BASE_REPLY_INTERVAL + jitter) # Ensure minimum sleep of 30s
                log.info(f"Sleeping for {sleep_duration:.0f} seconds (interval: {BASE_REPLY_INTERVAL}s, jitter: {jitter}s)...")
                time.sleep(sleep_duration)
            else:
                # Limit reached *during* this cycle, proceed to daily sleep immediately
                log.info(f"Daily tweet limit ({DAILY_TWEET_LIMIT}) reached. Proceeding to 24-hour sleep.")
                # Fall through to the else block below

        else: # corrections_count >= DAILY_TWEET_LIMIT
            now = datetime.now()
            # Calculate seconds until midnight
            midnight = datetime.combine(now.date() + timedelta(days=1), datetime.min.time())
            seconds_until_midnight = (midnight - now).total_seconds()

            sleep_duration = max(60, seconds_until_midnight + random.randint(60, 300)) # Sleep until after midnight + buffer
            log.info(f"Daily tweet limit ({DAILY_TWEET_LIMIT}) reached. Sleeping for approximately {sleep_duration / 3600:.2f} hours (until after midnight).")
            time.sleep(sleep_duration)
            # No need to reset corrections_made list here, get_corrections_made_count reads the file for the *new* day.

if __name__ == "__main__":
    # Ensure environment variables are loaded if needed directly by main (usually not)
    # load_dotenv()
    main()