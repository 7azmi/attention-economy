import tweepy
import os
import json
from dotenv import load_dotenv
from datetime import date
from gemini_utils import select_best_tweet  # Import the Gemini module

load_dotenv()

api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")
bearer_token = os.getenv("BEARER_TOKEN")
access_token = os.getenv("ACCESS_TOKEN")
access_token_secret = os.getenv("ACCESS_TOKEN_SECRET")

client = tweepy.Client(bearer_token, api_key, api_secret, access_token, access_token_secret)
auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)
api = tweepy.API(auth)

def correct_tweet(tweet_id):
    correction_message = "❌ انشاء الله\n✅ إن شاء الله"
    try:
        client.create_tweet(text=correction_message, in_reply_to_tweet_id=tweet_id)
        return True
    except tweepy.TweepyException as e:
        print(f"Error replying to tweet {tweet_id}: {e}")
        return False

if __name__ == "__main__":
    today = date.today().strftime("%Y-%m-%d")
    corrections_file = f"corrections_{today}.json"

    try:
        with open("tweets_to_correct.json", "r", encoding="utf-8") as f:
            tweets_to_correct = json.load(f)
    except FileNotFoundError:
        print("tweets_to_correct.json not found. Exiting.")
        exit()

    try:
        with open(corrections_file, "r") as f:
            corrections_made = json.load(f)
    except FileNotFoundError:
        corrections_made = []

    if len(corrections_made) >= 10:
        print("Daily correction limit reached.")
        exit()

    best_tweet = select_best_tweet(tweets_to_correct)

    if best_tweet:
        if correct_tweet(best_tweet["tweet_id"]):
            corrections_made.append(best_tweet["tweet_id"])
            with open(corrections_file, "w") as f:
                json.dump(corrections_made, f)
        else:
            print("Error posting correction.")
    else:
        print("No suitable tweet found.")