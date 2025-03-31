# scraper.py
import asyncio
import json
import re
import logging
import urllib.parse

from playwright.async_api import async_playwright

log = logging.getLogger(__name__)

ERROR_PAIRS = [
    ("انشاء الله", "إن شاء الله"),
    ("إنشاء الله", "إن شاء الله"),
    ("لاكن", "لكن"),
    ("ضلم", "ظلم"),
    ("ضالم", "ظالم"),
    ("خطاء", "خطأ"),
    ("هاذا", "هذا"),
]


def extract_number(text):
    """Extracts the first number from a string, handling K/M suffixes."""
    if not text:
        return 0

    text = text.replace(",", "")
    match = re.search(r"([\d.]+)([KM]?)", text, re.IGNORECASE)
    if not match:
        return 0

    num = float(match.group(1))
    suffix = match.group(2).upper() if match.group(2) else ""
    if suffix == "K":
        return int(num * 1000)
    elif suffix == "M":
        return int(num * 1000000)
    else:
        return int(num)


async def extract_tweet_data(item, error_pairs, connected_instance):
    """Extracts relevant data from a tweet element."""
    try:
        tweet_link_element = await item.query_selector("a.tweet-link")
        if not tweet_link_element:
            return None

        tweet_link_raw = await tweet_link_element.get_attribute("href")
        tweet_link = urllib.parse.urljoin(connected_instance, tweet_link_raw)
        tweet_id_match = re.search(r"/(?:status|statuses)/(\d+)", tweet_link)
        tweet_id = tweet_id_match.group(1) if tweet_id_match else None

        username_element = await item.query_selector("a.username")
        timestamp_element = await item.query_selector("span.tweet-date a")
        tweet_text_element = await item.query_selector("div.tweet-content")

        if not all([username_element, timestamp_element, tweet_text_element]):
            log.debug(f"Skipping tweet {tweet_id}: Missing elements.")
            return None

        username = await username_element.inner_text()
        timestamp_str = await timestamp_element.get_attribute("title") or await timestamp_element.inner_text()
        tweet_text = await tweet_text_element.inner_text()

        if tweet_text.startswith("RT @"):
            log.debug(f"Skipping tweet {tweet_id}: Looks like a Retweet.")
            return None

        found_error = next(
            (
                {"incorrect": incorrect, "correct": correct}
                for incorrect, correct in error_pairs
                if re.search(r"(?<!\w)" + re.escape(incorrect) + r"(\W|$)", tweet_text, re.IGNORECASE)
            ),
            None,
        )

        if not found_error:
            return None

        stats_elements = await item.query_selector_all("div.tweet-stats .tweet-stat")
        replies, retweets, likes, quotes = 0, 0, 0, 0 #added quote
        for stat_element in stats_elements:
            try:
                icon_container = await stat_element.query_selector("div.icon-container")
                if icon_container:
                    stat_text = await icon_container.inner_text()  # Get text from icon_container
                    stat_value = extract_number(stat_text)

                    icon = await icon_container.query_selector("span[class^='icon-']")
                    if icon:
                        icon_class = await icon.get_attribute("class") or ""
                        if "comment" in icon_class or "reply" in icon_class:  # Added 'reply'
                            replies = stat_value
                        elif "retweet" in icon_class:
                            retweets = stat_value
                        elif "heart" in icon_class or "like" in icon_class or "favorite" in icon_class:
                            likes = stat_value
                        elif "quote" in icon_class: #added quote
                            quotes = stat_value
                    else:
                        log.warning("No icon found within icon container")

                else:
                    log.warning("No icon container found for stat element.")

            except Exception as e:
                log.warning(f"Error extracting stat: {e}")

        return {
            "username": username.strip(),
            "timestamp": timestamp_str.strip(),
            "tweet": tweet_text.strip(),
            "link": tweet_link,
            "tweet_id": tweet_id,
            "error_found": found_error,
            "engagement": {"replies": replies, "retweets": retweets, "likes": likes, "quotes": quotes}, #added quote
        }

    except Exception as e:
        log.warning(f"Error processing tweet element: {e}", exc_info=False)
        try:
            link_el = await item.query_selector("a.tweet-link")
            if link_el:
                log.debug(f"Faulty element link: {await link_el.get_attribute('href')}")
        except:
            pass
        return None


async def scrape_nitter_search_async(
    error_pairs, max_tweets=30, min_engagement_query="(min_retweets:100 OR min_faves:200)"
):
    """Scrapes Nitter for tweets containing specific errors."""
    incorrect_words_query = " OR ".join([f'"{pair[0]}"' for pair in error_pairs])
    query = f"({incorrect_words_query}) {min_engagement_query} lang:ar -filter:retweets -filter:replies"
    log.info(f"Using query: {query}")

    tweets_to_correct = []
    processed_tweet_ids = set()

    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
        )
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page = await context.new_page()

        nitter_instances = [
            "https://nitter.net",
            "https://nitter.privacydev.net",
            "https://nitter.poast.org",
            "https://nitter.cz",
        ]
        encoded_query = urllib.parse.quote_plus(query)
        search_url_base = f"/search?q={encoded_query}&since=&until=&near="
        connected_instance = None

        for instance in nitter_instances:
            search_url = instance + search_url_base
            log.info(f"Trying Nitter instance: {instance}")
            try:
                await page.goto(search_url, timeout=60000, wait_until="domcontentloaded")
                await page.wait_for_selector("div.timeline .timeline-item", timeout=20000)
                connected_instance = instance
                log.info(f"Successfully connected to {instance}")
                break
            except Exception as e:
                log.warning(f"Failed connection/result wait on {instance}: {e}")
                await asyncio.sleep(1)

        if not connected_instance:
            log.error("Could not retrieve results from any Nitter instance.")
            await browser.close()
            return []

        await page.wait_for_timeout(5000)

        tweet_elements = await page.query_selector_all("div.timeline-item:not(.show-more)")
        log.info(f"Found {len(tweet_elements)} potential tweet elements.")

        count = 0
        for item in tweet_elements:
            if count >= max_tweets:
                break

            tweet_data = await extract_tweet_data(item, error_pairs, connected_instance)

            if tweet_data and tweet_data["tweet_id"] not in processed_tweet_ids:
                tweets_to_correct.append(tweet_data)
                processed_tweet_ids.add(tweet_data["tweet_id"])
                count += 1
                log.debug(
                    f"Added tweet {tweet_data['tweet_id']} ({tweet_data['error_found']['incorrect']}). Engagement: R:{tweet_data['engagement']['replies']}, RT:{tweet_data['engagement']['retweets']}, L:{tweet_data['engagement']['likes']}"
                )

        await browser.close()
        log.info(f"Finished scraping. Found {len(tweets_to_correct)} potential tweets containing specified errors.")
        return tweets_to_correct


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    fetched_tweets = asyncio.run(scrape_nitter_search_async(ERROR_PAIRS, max_tweets=30))

    output_filename = "tweets_to_correct.json"
    try:
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(fetched_tweets, f, ensure_ascii=False, indent=4)
        log.info(
            f"Successfully saved {len(fetched_tweets)} tweets to {output_filename}"
            if fetched_tweets
            else f"No matching tweets found. Saved empty list to {output_filename}"
        )
    except IOError as e:
        log.error(f"Error writing to {output_filename}: {e}")