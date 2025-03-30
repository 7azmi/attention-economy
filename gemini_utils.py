# gemini_utils.py
import os
from google import genai
from google.genai import types
import json
import re # Import regex
from dotenv import load_dotenv
import logging # Import logging

load_dotenv()

gemini_api_key = os.getenv("GEMINI_API_KEY")
if not gemini_api_key:
    logging.error("GEMINI_API_KEY not found in environment variables.")
    # Decide how to handle this: raise error, exit, etc.
    # For now, functions will likely fail later.

# Configure logging for this module
log = logging.getLogger(__name__)
# Note: Basic config should be set in the main script (main.py or tweeter.py)


def _call_gemini_api(prompt, model="gemini-1.5-flash", temperature=0.5): # Keep temperature in signature for now
    """ Helper function to call the Gemini API and handle potential errors """
    if not gemini_api_key:
        log.error("Cannot call Gemini API: API key is missing.")
        return None

    log.debug(f"Calling Gemini API. Model: {model}") # Removed Temp from log as it might not be applied
    log.debug(f"Prompt Start:\n------\n{prompt[:200]}...\n------")

    try:
        client = genai.Client(api_key=gemini_api_key)

        # *** FIX: Remove generation_config argument ***
        # Call generate_content without the configuration object.
        # NOTE: This might ignore the 'temperature' setting if this method doesn't support it directly.
        response = client.models.generate_content(
            model=model,  # Pass the model name string here
            contents=[prompt] # Pass the prompt within a list
            # Removed: generation_config=...
        )
        # *** END FIX ***

        # Handle potential safety blocks or empty responses
        if not hasattr(response, 'text') or not response.text:
             if hasattr(response, 'prompt_feedback') and response.prompt_feedback.block_reason:
                 log.warning(f"Gemini API call blocked. Reason: {response.prompt_feedback.block_reason_message}")
             elif not hasattr(response, 'candidates') or not response.candidates: # Check candidates attribute exists
                 log.warning("Gemini API returned no candidates.")
             else:
                log.warning("Gemini API returned an empty or unexpected response structure.")
             return None

        log.debug(f"Gemini Raw Response Text:\n------\n{response.text}\n------")
        return response.text.strip()

    except TypeError as e:
         # Catch TypeErrors specifically, as they often relate to arguments
         log.error(f"Gemini API TypeError (check arguments/API usage): {e}", exc_info=True)
         return None
    except AttributeError as e:
         log.error(f"Gemini API Attribute Error (potential library mismatch or change?): {e}", exc_info=True)
         return None
    except Exception as e:
        log.error(f"Gemini API Error: {e}", exc_info=True) # Log traceback
        return None


def select_best_tweet(tweets):
    """
    Selects the best tweet ID from a list using Gemini, focusing on clarity and robustness.
    """
    if not tweets:
        log.warning("select_best_tweet called with empty tweet list.")
        return None

    # Prepare a simpler representation for the prompt if tweets are complex
    simplified_tweets = [
        {
            "tweet_id": t.get("tweet_id"),
            "text_snippet": t.get("tweet", "")[:100] + "...", # Show only beginning of tweet
            "retweets": t.get("engagement", {}).get("retweets", 0),
            "likes": t.get("engagement", {}).get("likes", 0),
            "error": t.get("error_found", {}).get("incorrect", "N/A")
        }
        for t in tweets if t.get("tweet_id") # Ensure tweet has an ID
    ]

    if not simplified_tweets:
         log.warning("No tweets with valid IDs found to send to Gemini for selection.")
         return None

    # Enhanced prompt asking specifically for the ID only
    prompt = f"""Analyze the following list of tweets. Each tweet contains a common Arabic grammatical error that needs correction. Select the single BEST tweet to reply to with a correction.

Consider these factors for selection:
1.  **Engagement:** Higher retweets and likes generally indicate wider visibility, but avoid extremely viral tweets that might attract negativity. Aim for significant but not overwhelming engagement.
2.  **Relevance:** Ensure the tweet's context is suitable for a helpful correction (e.g., avoid arguments, sensitive topics if possible).
3.  **Clarity:** The error should be clear in the snippet provided.

Tweets List:
{json.dumps(simplified_tweets, ensure_ascii=False, indent=2)}

Based on your analysis, which tweet is the most suitable candidate for receiving a helpful correction reply?

**Your Response:** Provide **ONLY** the numeric `tweet_id` of the selected tweet and nothing else. For example: 17384756789123456
"""

    response_text = _call_gemini_api(prompt, model="gemini-1.5-flash", temperature=0.2) # Low temp for deterministic ID

    if not response_text:
        log.error("Failed to get response from Gemini for tweet selection.")
        return None

    # Use regex to find the first sequence of digits (more robust)
    match = re.search(r'\b(\d{10,25})\b', response_text) # Look for typical tweet ID lengths
    if match:
        best_tweet_id = match.group(1)
        log.info(f"Gemini selected tweet ID: {best_tweet_id}")
        # Find the original full tweet object matching the ID
        for tweet in tweets:
            if tweet.get("tweet_id") == best_tweet_id:
                return tweet
        log.warning(f"Selected tweet ID {best_tweet_id} not found in the original list.")
        return None # ID found in response, but not in our list
    else:
        log.error(f"Could not extract a valid numeric tweet ID from Gemini response: '{response_text}'")
        return None


def generate_correction_message(incorrect_word, correct_word):
    """
    Generates a two-line correction message with emojis using Gemini.
    """
    log.info(f"Requesting correction message for: '{incorrect_word}' -> '{correct_word}'")

    prompt = f"""Generate a friendly and concise two-line correction message for an Arabic tweet.

The message should:
1.  Clearly show the incorrect word and the correct word.
2.  Use relevant emojis (like ❌ and ✅ or similar).
3.  Be polite and helpful in tone.
4.  Fit within Twitter's reply character limits.

Incorrect word: "{incorrect_word}"
Correct word: "{correct_word}"

**Example Format:**
❌ [Incorrect Word]
✅ [Correct Word]

Please provide only the two-line correction message.
"""

    correction_message = _call_gemini_api(prompt, model="gemini-1.5-flash", temperature=0.7) # Allow some creativity

    if not correction_message:
        log.error("Failed to generate correction message from Gemini.")
        # Fallback message
        return f"تصحيح إملائي:\n❌ {incorrect_word}\n✅ {correct_word}"
    else:
        # Basic validation: ensure it's roughly two lines
        if '\n' not in correction_message or len(correction_message.split('\n')) > 3:
             log.warning(f"Generated message format might be unexpected: '{correction_message}'. Using anyway.")
        log.info(f"Generated correction message:\n{correction_message}")
        return correction_message