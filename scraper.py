from pprint import pprint

from playwright.async_api import async_playwright
import asyncio
import urllib.parse  # Import urllib.parse for URL encoding

async def scrape_nitter_search_async(query, max_tweets=10):
    nitter_url = "https://nitter.net"
    encoded_query = urllib.parse.quote_plus(query)  # URL-encode the query
    search_url = f"{nitter_url}/search?f=tweets&q={encoded_query}&since=&until=&near="
    tweets = []

    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            await page.goto(search_url, timeout=30000)
            await asyncio.sleep(3)  # Slightly shorter wait

            tweet_elements = await page.query_selector_all("div.timeline-item")

            for item in tweet_elements[:max_tweets]:
                username_element = await item.query_selector("div.tweet-name-row a.username")
                timestamp_element = await item.query_selector("span.tweet-date a")
                tweet_text_element = await item.query_selector("div.tweet-content")
                tweet_link_element = await item.query_selector("a.tweet-link")

                username = await username_element.inner_text() if username_element else "Unknown"
                timestamp = await timestamp_element.inner_text() if timestamp_element else "Unknown"
                tweet_text = await tweet_text_element.inner_text() if tweet_text_element else ""
                tweet_text = tweet_text[:4093] + '...' if len(tweet_text) > 4096 else tweet_text #handles long text
                tweet_link = nitter_url + await tweet_link_element.get_attribute("href") if tweet_link_element else ""

                tweets.append({
                    "username": username,
                    "timestamp": timestamp,
                    "tweet": tweet_text,
                    "link": tweet_link
                })

        except Exception as e:
            print(f"Error fetching tweets: {e}")

        await browser.close()
        pprint(tweets)
        return tweets

if __name__ == "__main__":
    query = '"انشاء الله" (min_retweets:500 OR min_faves:1000) lang:ar'  # Filtered Query
    asyncio.run(scrape_nitter_search_async(query))