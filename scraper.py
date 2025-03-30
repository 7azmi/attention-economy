import asyncio
import json
from playwright.async_api import async_playwright
import urllib.parse
import re # Import regex module

# Define error pairs (incorrect, correct)
# You can easily add more pairs here
ERROR_PAIRS = [
    # ("انشاء الله", "إن شاء الله"),
    ("لاكن", "لكن"),
    # Add more pairs as needed, e.g., ("مثلا", "مثلاً")
]

# --- Helper function to extract numbers ---
def extract_number(text):
    """Extracts the first number found in a string, handling K/M suffixes."""
    if not text:
        return 0
    # Remove commas
    text = text.replace(',', '')
    match = re.search(r'([\d.]+)([KM]?)', text, re.IGNORECASE)
    if match:
        num = float(match.group(1))
        suffix = match.group(2).upper()
        if suffix == 'K':
            return int(num * 1000)
        elif suffix == 'M':
            return int(num * 1000000)
        else:
            return int(num)
    return 0
# --- End Helper ---

async def scrape_nitter_search_async(error_pairs, max_tweets=10, min_engagement_query="(min_retweets:500 OR min_faves:1000)"):
    tweets_to_correct = []
    # Build the query part for incorrect words
    incorrect_words_query = " OR ".join([f'"{pair[0]}"' for pair in error_pairs])

    query = f'({incorrect_words_query}) {min_engagement_query} lang:ar'
    print(f"Scraper: Using query: {query}") # Basic print logging

    try:
        async with async_playwright() as p:
            browser = await p.firefox.launch(headless=True)
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            ) # Set a common user agent
            page = await context.new_page()

            # Try different Nitter instances if one fails, or use a known working one
            nitter_instances = ["https://nitter.net", "https://nitter.privacydev.net", "https://nitter.poast.org"] # Add more if needed
            encoded_query = urllib.parse.quote_plus(query)
            search_url_base = f"/search?f=tweets&q={encoded_query}&since=&until=&near="
            tweets_found = False

            for instance in nitter_instances:
                search_url = instance + search_url_base
                print(f"Scraper: Trying Nitter instance: {instance}")
                try:
                    await page.goto(search_url, timeout=45000, wait_until='domcontentloaded') # Increased timeout, wait condition
                    await page.wait_for_selector("div.timeline-item", timeout=15000) # Wait for results to appear
                    tweets_found = True
                    print(f"Scraper: Successfully connected to {instance}")
                    break # Stop trying instances if one works
                except Exception as e:
                    print(f"Scraper: Failed to connect or find results on {instance}: {e}")
                    continue # Try next instance

            if not tweets_found:
                print("Scraper: Could not retrieve results from any Nitter instance.")
                await browser.close()
                return []

            await asyncio.sleep(5) # Wait for dynamic content like stats

            tweet_elements = await page.query_selector_all("div.timeline-item")
            print(f"Scraper: Found {len(tweet_elements)} potential tweet elements on page.")

            count = 0
            for item in tweet_elements:
                if count >= max_tweets:
                    break

                try:
                    username_element = await item.query_selector("a.username")
                    timestamp_element = await item.query_selector("span.tweet-date a") # Check if 'title' attr has full date
                    tweet_text_element = await item.query_selector("div.tweet-content")
                    tweet_link_element = await item.query_selector("a.tweet-link")

                    # --- Extract Engagement Stats ---
                    # Selectors might change based on Nitter instance/updates. Inspect HTML if needed.
                    stats_elements = await item.query_selector_all("div.tweet-stats span.tweet-stat")
                    replies = 0
                    retweets = 0
                    likes = 0
                    for stat_element in stats_elements:
                        # Check class names or icons within the span to identify stat type
                        icon = await stat_element.query_selector("span.icon-comment, span.icon-retweet, span.icon-heart")
                        if icon:
                            icon_class = await icon.get_attribute("class")
                            stat_text = await stat_element.inner_text()
                            stat_value = extract_number(stat_text) # Use helper to parse number

                            if "icon-comment" in icon_class:
                                replies = stat_value
                            elif "icon-retweet" in icon_class:
                                retweets = stat_value
                            elif "icon-heart" in icon_class: # Nitter often uses heart for likes/faves
                                likes = stat_value
                    # --- End Engagement Stats ---


                    if username_element and timestamp_element and tweet_text_element and tweet_link_element:
                        username = await username_element.inner_text()
                        # Get full timestamp from title attribute if available
                        timestamp_title = await timestamp_element.get_attribute("title")
                        timestamp = timestamp_title if timestamp_title else await timestamp_element.inner_text()
                        tweet_text = await tweet_text_element.inner_text()
                        tweet_link_raw = await tweet_link_element.get_attribute("href")
                        # Ensure link is absolute
                        tweet_link = urllib.parse.urljoin(page.url, tweet_link_raw)
                        tweet_id_match = re.search(r'/status/(\d+)', tweet_link) # More robust ID extraction
                        tweet_id = tweet_id_match.group(1) if tweet_id_match else tweet_link.split('/')[-1].split('#')[0] # Fallback


                        # Check which error is present and store both incorrect/correct forms
                        found_error = None
                        for incorrect, correct in error_pairs:
                            if incorrect in tweet_text:
                                found_error = {"incorrect": incorrect, "correct": correct}
                                break # Found the error for this tweet

                        if found_error:
                            tweets_to_correct.append({
                                "username": username.strip(),
                                "timestamp": timestamp.strip(),
                                "tweet": tweet_text.strip(),
                                "link": tweet_link,
                                "tweet_id": tweet_id,
                                "error_found": found_error, # Add the specific error pair
                                "engagement": { # Add engagement numbers
                                    "replies": replies,
                                    "retweets": retweets,
                                    "likes": likes
                                }
                            })
                            count += 1
                            print(f"Scraper: Added tweet {tweet_id} for correction ({found_error['incorrect']}). Engagement: R:{replies}, RT:{retweets}, L:{likes}")

                except Exception as e:
                    print(f"Scraper: Error processing one tweet element: {e}")
                    # Attempt to get link even if other parts fail for debugging
                    try:
                        link_el = await item.query_selector("a.tweet-link")
                        if link_el:
                             print(f"Scraper: Faulty element link: {await link_el.get_attribute('href')}")
                    except:
                        pass # Ignore errors here

            await browser.close()
            print(f"Scraper: Found {len(tweets_to_correct)} tweets containing specified errors.")
            return tweets_to_correct

    except Exception as e:
        print(f"Scraper: Critical error during scraping process: {e}")
        # Ensure browser is closed if it exists and hasn't been closed
        if 'browser' in locals() and browser.is_connected():
           await browser.close()
        return []

if __name__ == "__main__":
    # Use the defined ERROR_PAIRS list
    tweets_to_correct = asyncio.run(scrape_nitter_search_async(ERROR_PAIRS, max_tweets=20)) # Increase max_tweets slightly

    if tweets_to_correct:
        output_filename = "tweets_to_correct.json"
        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                json.dump(tweets_to_correct, f, ensure_ascii=False, indent=4)
            print(f"Scraper: Successfully saved {len(tweets_to_correct)} tweets to {output_filename}")
        except IOError as e:
            print(f"Scraper: Error writing to {output_filename}: {e}")
    else:
        print("Scraper: No tweets found matching the criteria or an error occurred.")
        # Optional: Create an empty file to prevent tweeter.py from crashing if it expects the file
        output_filename = "tweets_to_correct.json"
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump([], f)
        print(f"Scraper: Created empty {output_filename}.")