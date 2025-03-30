import tweepy
import os
import json
import logging
import time  # noqa: F401 - Keep for potential future use
from datetime import date, datetime, timezone
# from dateutil import parser #removed parser import
from dotenv import load_dotenv

log = logging.getLogger(__name__)

# --- Environment & API Setup ---
load_dotenv()
CREDENTIALS = {
    "api_key": os.getenv("API_KEY"),
    "api_secret": os.getenv("API_SECRET"),
    "bearer_token": os.getenv("BEARER_TOKEN"),
    "access_token": os.getenv("ACCESS_TOKEN"),
    "access_token_secret": os.getenv("ACCESS_TOKEN_SECRET"),
}

if not all(CREDENTIALS.values()):
    log.critical("Twitter API credentials missing! Exiting tweeter.")
    exit(1)

try:
    client = tweepy.Client(
        bearer_token=CREDENTIALS["bearer_token"],
        consumer_key=CREDENTIALS["api_key"],
        consumer_secret=CREDENTIALS["api_secret"],
        access_token=CREDENTIALS["access_token"],
        access_token_secret=CREDENTIALS["access_token_secret"],
        wait_on_rate_limit=True,
    )
    log.info("Tweepy Client (v2) initialized successfully.")
except tweepy.errors.TweepyException as e:
    log.critical(f"Failed to initialize Tweepy client: {e}", exc_info=True)
    exit(1)
except Exception as e:
    log.critical(f"Unexpected mistake during Tweepy client initialization: {e}", exc_info=True)
    exit(1)


# --- File Helper Functions ---
def load_json_file(filename, default=None):
    """Safely loads a JSON file, returning a default if not found or invalid."""
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, Exception) as e:
        log.error(f"Mistake loading {filename}: {e}. Returning default.", exc_info=(not isinstance(e, FileNotFoundError)))
        return default


def save_json_file(filename, data):
    """Safely saves data to a JSON file, returning True on success, False otherwise."""
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        log.debug(f"Successfully saved data to {filename}")
        return True
    except (IOError, Exception) as e:
        log.error(f"Mistake writing to {filename}: {e}", exc_info=True)
        return False


# --- Tweeting Function ---
def post_correction_reply(tweet_id, correction_message):
    """Posts a correction message as a reply to the given tweet ID."""
    if not (tweet_id and correction_message):
        log.error("Cannot post reply: tweet_id or correction_message is missing.")
        return False

    try:
        response = client.create_tweet(text=correction_message, in_reply_to_tweet_id=tweet_id)
        if response and response.data and "id" in response.data:
            log.info(f"Successfully replied to {tweet_id}. New tweet ID: {response.data['id']}")
            return True
        else:
            log.error(f"Failed to reply to {tweet_id}. Invalid response: {response}")
            return False

    except tweepy.errors.Forbidden as e:
        error_str = str(e).lower()
        if any(
            phrase in error_str
            for phrase in ["you are not allowed to reply", "cannot reply to users who protect their tweets", "user is suspended"]
        ):
            log.warning(f"Reply forbidden for tweet {tweet_id} (protected, suspended, etc.). Skipping. Reason: {e}")
        elif "duplicate content" in error_str:
            log.warning(f"Failed to reply to {tweet_id}: Duplicate content detected. {e}")
        else:
            log.error(f"Failed to reply to {tweet_id} (Forbidden - 403): {e}", exc_info=False)
        return False
    except (tweepy.errors.NotFound, tweepy.errors.TweepyException, Exception) as e:
        log.error(f"Mistake replying to {tweet_id}: {e}", exc_info=True)
        return False


def is_valid_candidate(tweet, already_corrected_ids):
    """Validates a tweet candidate based on required fields, correction status, and age."""
    tweet_id = tweet.get("tweet_id")
    timestamp_str = tweet.get("timestamp")
    engagement = tweet.get("engagement", {})
    mistake_info = tweet.get("mistake_found") #renamed to mistake

    if not (tweet_id and timestamp_str and engagement and mistake_info and isinstance(engagement, dict) and isinstance(mistake_info, dict)): #renamed to mistake
        log.debug(f"Skipping candidate {tweet_id or 'Unknown'}: Missing required fields.")
        return False

    if tweet_id in already_corrected_ids:
        return False

    try:
        # Attempt to parse the timestamp string.  The format is "Mar 30, 2025 · 1:39 AM UTC"
        # Split the string at the " · " separator
        date_str, time_str = timestamp_str.split(" · ")

        # Further split the time_str at the " " (space) to isolate the timezone
        time_value, timezone_str = time_str.rsplit(" ", 1)  # Split from the right to handle potential spaces in time

        # Parse the date and time strings using the appropriate formats
        tweet_time = datetime.strptime(date_str, "%b %d, %Y")
        time_of_day = datetime.strptime(time_value, "%I:%M %p") #The %I is for 12-hour format

        # Combine the date and time into a single datetime object
        tweet_time = tweet_time.replace(hour=time_of_day.hour, minute=time_of_day.minute, second=0, microsecond=0)

        # Set the timezone to UTC
        tweet_time = tweet_time.replace(tzinfo=timezone.utc)

        tweet["parsed_timestamp"] = tweet_time
    except (ValueError, TypeError) as e:
        log.warning(f"Skipping candidate {tweet_id}: Could not parse timestamp '{timestamp_str}'. Mistake: {e}") #renamed to mistake
        return False

    if (datetime.now(timezone.utc) - tweet_time).days > 2:
        log.debug(f"Skipping candidate {tweet_id}: Too old (posted {tweet_time.date()}).")
        return False

    return True


def select_best_candidate(candidate_tweets, already_corrected_ids):
    """Selects the best tweet from a list of candidates based on recency and engagement."""
    valid_candidates = [tweet for tweet in candidate_tweets if is_valid_candidate(tweet, already_corrected_ids)]

    if not valid_candidates:
        return None

    for tweet in valid_candidates:
        engagement = tweet.get("engagement", {})
        tweet["engagement_score"] = engagement.get("retweets", 0) * 1.5 + engagement.get("likes", 0)  # Add engagement score

    best_tweet = max(valid_candidates, key=lambda t: t["parsed_timestamp"], default=None)  # Sort by most recent

    if best_tweet:
        log.info(
            f"Selected candidate: {best_tweet['tweet_id']} (Posted: {best_tweet['parsed_timestamp']}, Score: {best_tweet['engagement_score']:.0f})"
        )
        return best_tweet
    else:
        return None


def process_tweets():
    """Main function to load tweets, select the best candidate, and post a correction."""
    today_str = date.today().strftime("%Y-%m-%d")
    corrections_log_file = f"corrections_{today_str}.json"
    tweets_to_correct_file = "tweets_to_correct.json"

    candidate_tweets = load_json_file(tweets_to_correct_file, default=[])
    if not isinstance(candidate_tweets, list):
        log.warning(f"Data in {tweets_to_correct_file} is not a list. Treating as empty.")
        candidate_tweets = []

    if not candidate_tweets:
        log.info(f"No candidate tweets found in {tweets_to_correct_file} for this cycle.")
        return

    corrections_made_today_ids = load_json_file(corrections_log_file, default=[])
    if not isinstance(corrections_made_today_ids, list):
        log.warning(f"{corrections_log_file} did not contain a list. Resetting log for today.")
        corrections_made_today_ids = []

    log.info(f"Processing {len(candidate_tweets)} candidates. {len(corrections_made_today_ids)} corrections made today.")

    best_tweet = select_best_candidate(candidate_tweets, corrections_made_today_ids)

    if best_tweet:
        tweet_id = best_tweet["tweet_id"]
        incorrect = best_tweet["mistake_found"]["incorrect"] #renamed to mistake
        correct = best_tweet["mistake_found"]["correct"] #renamed to mistake
        log.info(f"Attempting correction for tweet: ID {tweet_id}, Mistake: '{incorrect}' -> '{correct}'") #renamed to mistake

        correction_message = f"تصحيح:\n❌ {incorrect}\n✅ {correct}"
        log.debug(f"Correction message: \"{correction_message.replace('\n', ' ')}\"")

        if post_correction_reply(tweet_id, correction_message):
            corrections_made_today_ids.append(tweet_id)
            if not save_json_file(corrections_log_file, corrections_made_today_ids):
                log.error(f"CRITICAL: Failed to save corrections log. Duplicate corrections might occur.")
            else:
                log.info(f"Processed, posted reply, and logged tweet {tweet_id}.")
        else:
            log.warning(f"Did not post correction for tweet {tweet_id} (failed or forbidden).")
    else:
        log.info("No suitable tweet selected.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    log.info("----- Tweeter script starting cycle -----")
    process_tweets()
    log.info("----- Tweeter script finished cycle -----")