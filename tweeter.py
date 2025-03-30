import tweepy
import os
import json
from dotenv import load_dotenv
from datetime import date, datetime # Import datetime
import logging # Import logging
import time # Import time for potential waits
from gemini_utils import select_best_tweet, generate_correction_message

# --- Logging Setup ---
# Basic configuration will be handled by main.py, but get the logger
log = logging.getLogger(__name__)
# --- End Logging Setup ---


# --- Environment & API Setup ---
load_dotenv()

# Check for essential keys
api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")
bearer_token = os.getenv("BEARER_TOKEN") # Needed for Client v2
access_token = os.getenv("ACCESS_TOKEN")
access_token_secret = os.getenv("ACCESS_TOKEN_SECRET")

if not all([api_key, api_secret, bearer_token, access_token, access_token_secret]):
    log.critical("Twitter API credentials missing in environment variables! Exiting tweeter.")
    exit(1) # Exit if keys are missing

try:
    client = tweepy.Client(
        bearer_token=bearer_token, # Use bearer_token here
        consumer_key=api_key,
        consumer_secret=api_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
        wait_on_rate_limit=True # Automatically wait if rate limited
    )
    log.info("Tweepy Client (v2) initialized successfully.")
    # You might not need API v1 if Client v2 does everything
    # auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)
    # api = tweepy.API(auth, wait_on_rate_limit=True)
    # log.info("Tweepy API (v1) initialized successfully.")

except tweepy.errors.TweepyException as e:
     log.critical(f"Failed to initialize Tweepy client: {e}", exc_info=True)
     exit(1)
# --- End Environment & API Setup ---


def post_correction_reply(tweet_id, correction_message):
    """Posts the correction message as a reply to the given tweet ID."""
    log.info(f"Attempting to reply to tweet ID: {tweet_id}")
    try:
        # Using Client v2 for creating tweets/replies
        response = client.create_tweet(text=correction_message, in_reply_to_tweet_id=tweet_id)
        log.info(f"Successfully posted reply to {tweet_id}. New tweet ID: {response.data['id']}")
        return True
    except tweepy.errors.Forbidden as e:
        # Handle specific errors like "You are not allowed to create a Tweet with duplicate content" (403 Forbidden)
        if "duplicate content" in str(e).lower():
             log.warning(f"Failed to reply to {tweet_id}: Duplicate content detected. {e}")
        # Handle errors like replying to protected accounts or accounts that blocked you
        elif "cannot reply to users who protect their Tweets" in str(e).lower():
             log.warning(f"Failed to reply to {tweet_id}: Cannot reply to protected tweet. {e}")
        elif "User is suspended" in str(e).lower():
             log.warning(f"Failed to reply to {tweet_id}: Original tweet author is suspended. {e}")
        else:
             log.error(f"Failed to reply to {tweet_id} (Forbidden - 403): {e}", exc_info=True)
        return False
    except tweepy.errors.NotFound as e:
        # Handle case where original tweet was deleted (404 Not Found)
        log.warning(f"Failed to reply to {tweet_id}: Original tweet likely deleted (Not Found - 404). {e}")
        return False
    except tweepy.errors.TweepyException as e:
        # Catch other potential Tweepy/Twitter API errors
        log.error(f"Error replying to tweet {tweet_id}: {e}", exc_info=True)
        return False
    except Exception as e:
        # Catch unexpected errors
        log.error(f"Unexpected error during reply to {tweet_id}: {e}", exc_info=True)
        return False


def load_json_file(filename, default=[]):
    """Safely loads a JSON file."""
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        log.warning(f"{filename} not found. Returning default: {default}")
        return default
    except json.JSONDecodeError as e:
        log.error(f"Error decoding JSON from {filename}: {e}. Returning default: {default}")
        return default
    except Exception as e:
        log.error(f"Error reading {filename}: {e}. Returning default: {default}", exc_info=True)
        return default


def save_json_file(filename, data):
    """Safely saves data to a JSON file."""
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        log.debug(f"Successfully saved data to {filename}")
        return True
    except IOError as e:
        log.error(f"Error writing to {filename}: {e}", exc_info=True)
        return False
    except Exception as e:
        log.error(f"Unexpected error writing to {filename}: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    log.info("----- Tweeter script started -----")

    today_str = date.today().strftime("%Y-%m-%d")
    corrections_log_file = f"corrections_{today_str}.json"
    tweets_to_correct_file = "tweets_to_correct.json"

    # Load tweets scraped by scraper.py
    tweets_to_correct = load_json_file(tweets_to_correct_file, [])
    if not tweets_to_correct:
        log.info("No tweets found in tweets_to_correct.json. Exiting tweeter.")
        exit(0) # Exit gracefully if no tweets to process

    # Load tweets already corrected today
    corrections_made_today_ids = load_json_file(corrections_log_file, [])
    # Ensure it's a list, even if file had null or wrong type
    if not isinstance(corrections_made_today_ids, list):
         log.warning(f"{corrections_log_file} did not contain a list. Resetting.")
         corrections_made_today_ids = []

    log.info(f"Loaded {len(tweets_to_correct)} potential tweets. Already corrected {len(corrections_made_today_ids)} today.")

    # --- Daily Limit Check (redundant if main.py handles it, but good safety) ---
    # DAILY_TWEET_LIMIT = 10 # Or get from main.py if refactored
    # if len(corrections_made_today_ids) >= DAILY_TWEET_LIMIT:
    #     log.info(f"Daily correction limit ({DAILY_TWEET_LIMIT}) already reached based on log file. Exiting.")
    #     exit(0)
    # --- End Daily Limit Check ---

    # Filter out tweets already corrected today
    candidate_tweets = [
        tweet for tweet in tweets_to_correct
        if tweet.get("tweet_id") not in corrections_made_today_ids and tweet.get("error_found")
    ]

    if not candidate_tweets:
        log.info("No new candidate tweets available to correct (all scraped tweets already processed today or lack error info).")
        exit(0)

    log.info(f"Selecting best tweet from {len(candidate_tweets)} candidates.")
    best_tweet = select_best_tweet(candidate_tweets) # Use Gemini to select

    if best_tweet and "tweet_id" in best_tweet and "error_found" in best_tweet:
        tweet_id = best_tweet["tweet_id"]
        incorrect = best_tweet["error_found"]["incorrect"]
        correct = best_tweet["error_found"]["correct"]
        log.info(f"Best tweet selected: ID {tweet_id}, Error: '{incorrect}' -> '{correct}'")

        # Generate the dynamic correction message using Gemini
        log.info("Generating correction message via Gemini...")
        correction_message = generate_correction_message(incorrect, correct)

        if correction_message:
            # Post the reply
            if post_correction_reply(tweet_id, correction_message):
                # Add to today's log if successful
                corrections_made_today_ids.append(tweet_id)
                if not save_json_file(corrections_log_file, corrections_made_today_ids):
                     log.error(f"CRITICAL: Failed to save updated corrections log to {corrections_log_file} after successful tweet!")
                else:
                     log.info(f"Successfully processed and logged tweet {tweet_id}.")
            else:
                log.error(f"Failed to post correction reply for tweet {tweet_id}.")
                # Optional: Add failed ID to a separate log? Or just rely on logs.
        else:
            log.error(f"Failed to generate correction message for tweet {tweet_id}. Skipping.")

    elif best_tweet:
        log.error(f"Selected tweet object is missing 'tweet_id' or 'error_found'. Data: {best_tweet}")
    else:
        log.warning("Gemini did not select a suitable tweet from the candidates.")

    # Cleanup: Optionally remove the processed tweet file or clear it?
    # For now, main.py handles re-running scraper, so stale tweets will be overwritten/ignored.
    # os.remove(tweets_to_correct_file) # Use with caution

    log.info("----- Tweeter script finished -----")