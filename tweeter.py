import tweepy
import os
import json
from dotenv import load_dotenv
from datetime import date, datetime
import logging
import time
from gemini_utils import select_best_tweet, generate_correction_message

# --- Logging Setup ---
log = logging.getLogger(__name__)


# --- Environment & API Setup ---
load_dotenv()

api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")
bearer_token = os.getenv("BEARER_TOKEN")
access_token = os.getenv("ACCESS_TOKEN")
access_token_secret = os.getenv("ACCESS_TOKEN_SECRET")

if not all([api_key, api_secret, bearer_token, access_token, access_token_secret]):
    log.critical("Twitter API credentials missing! Exiting tweeter.")
    exit(1)

try:
    client = tweepy.Client(
        bearer_token=bearer_token,
        consumer_key=api_key,
        consumer_secret=api_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
        wait_on_rate_limit=True
    )
    log.info("Tweepy Client (v2) initialized successfully.")

except tweepy.errors.TweepyException as e:
     log.critical(f"Failed to initialize Tweepy client: {e}", exc_info=True)
     exit(1)


def post_correction_reply(tweet_id, correction_message):
    """Posts the correction message as a reply to the given tweet ID."""
    log.info(f"Attempting to reply to tweet ID: {tweet_id}")
    try:
        response = client.create_tweet(text=correction_message, in_reply_to_tweet_id=tweet_id)
        log.info(f"Successfully posted reply to {tweet_id}. New tweet ID: {response.data['id']}")
        return True
    except tweepy.errors.Forbidden as e:
        error_message = str(e).lower()
        if "you are not allowed to reply to this tweet" in error_message:
            log.warning(f"Cannot reply to tweet {tweet_id}: Reply restrictions in place. Skipping.")
            return False
        elif "duplicate content" in error_message:
             log.warning(f"Failed to reply to {tweet_id}: Duplicate content detected. {e}")
        elif "cannot reply to users who protect their Tweets" in str(e).lower():
             log.warning(f"Failed to reply to {tweet_id}: Cannot reply to protected tweet. {e}")
        elif "user is suspended" in str(e).lower():  # Corrected this line for accuracy
             log.warning(f"Failed to reply to {tweet_id}: Original tweet author is suspended. {e}")
        else:
             log.error(f"Failed to reply to {tweet_id} (Forbidden - 403): {e}", exc_info=True)
        return False
    except tweepy.errors.NotFound as e:
        log.warning(f"Failed to reply to {tweet_id}: Original tweet likely deleted (Not Found - 404). {e}")
        return False
    except tweepy.errors.TweepyException as e:
        log.error(f"Error replying to tweet {tweet_id}: {e}", exc_info=True)
        return False
    except Exception as e:
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

    tweets_to_correct = load_json_file(tweets_to_correct_file, [])
    if not tweets_to_correct:
        log.info("No tweets found in tweets_to_correct.json. Exiting tweeter.")
        exit(0)

    corrections_made_today_ids = load_json_file(corrections_log_file, [])
    if not isinstance(corrections_made_today_ids, list):
         log.warning(f"{corrections_log_file} did not contain a list. Resetting.")
         corrections_made_today_ids = []

    log.info(f"Loaded {len(tweets_to_correct)} potential tweets. Already corrected {len(corrections_made_today_ids)} today.")

    candidate_tweets = [
        tweet for tweet in tweets_to_correct
        if tweet.get("tweet_id") not in corrections_made_today_ids and tweet.get("error_found")
    ]

    if not candidate_tweets:
        log.info("No new candidate tweets available to correct.")
        exit(0)

    log.info(f"Selecting best tweet from {len(candidate_tweets)} candidates.")
    best_tweet = select_best_tweet(candidate_tweets)

    if best_tweet and "tweet_id" in best_tweet and "error_found" in best_tweet:
        tweet_id = best_tweet["tweet_id"]
        incorrect = best_tweet["error_found"]["incorrect"]
        correct = best_tweet["error_found"]["correct"]
        log.info(f"Best tweet selected: ID {tweet_id}, Error: '{incorrect}' -> '{correct}'")

        log.info("Generating correction message via Gemini...")
        correction_message = generate_correction_message(incorrect, correct)

        if correction_message:
            if post_correction_reply(tweet_id, correction_message):
                corrections_made_today_ids.append(tweet_id)
                if not save_json_file(corrections_log_file, corrections_made_today_ids):
                     log.error(f"CRITICAL: Failed to save corrections log to {corrections_log_file} after successful tweet!")
                else:
                     log.info(f"Successfully processed and logged tweet {tweet_id}.")
            else:
                log.error(f"Failed to post correction reply for tweet {tweet_id}.")
        else:
            log.error(f"Failed to generate correction message for tweet {tweet_id}. Skipping.")

    elif best_tweet:
        log.error(f"Selected tweet missing 'tweet_id' or 'error_found': {best_tweet}")
    else:
        log.warning("Gemini did not select a suitable tweet.")

    log.info("----- Tweeter script finished -----")