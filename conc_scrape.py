import sys
import re
import hashlib
import time
import redis.client
import requests
import argparse
import redis
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

visited_key = "visited_urls"
emails_key = "scraped_emails"
to_visit_key = "to_visit_urls"
domain_count_key = "domain_count"

def get_emails_from_text(text):
    email_pattern = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
    return re.findall(email_pattern, text)

def scrape_page(url):
    try:
        response = requests.get(
            url,
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
            },
            timeout=3
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        emails = get_emails_from_text(soup.get_text())

        links = [urljoin(url, a.get('href')) for a in soup.find_all('a', href=True)]
        links = [link for link in links if is_valid_url(link)]

        return emails, links
    except requests.RequestException as e:
        print(f"Error scraping {url}: {e}")
        return [], []

def is_valid_url(url):
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)

def hash_url(url):
    return hashlib.sha256(url.encode('utf-8')).hexdigest()

def is_valid_domain(url, domain):
    parsed = urlparse(url)
    return parsed.hostname.endswith(domain)

def get_domain(url):
    parsed = urlparse(url)
    return parsed.hostname

def scrape_emails(args, redis_client):
    with ThreadPoolExecutor(args.threads) as executor:
        while True:
            to_visit = redis_client.zpopmin(to_visit_key, args.threads)

            if not to_visit:
                print("No more URLs to visit. Checking again in 30s.")
                time.sleep(30)
                continue

            futures = {executor.submit(scrape_page, url.split(',', 1)[1]): url for url, _ in to_visit}
            zadd_mapping = {}

            for future in as_completed(futures):
                url = futures[future]
                depth, url = url.split(',', 1)
                depth = int(depth)
                hashed_url = hash_url(url)

                try:
                    page_emails, links = future.result()
                    links = [link for link in links if is_valid_domain(link, args.domain)]
                    ismembers = redis_client.smismember(visited_key, map(hash_url, links)) if len(links) > 0 else []
                    unvisited_links = [link for link, ismember in zip(links, ismembers) if not ismember]

                    if len(page_emails) > 0:
                        redis_client.sadd(emails_key, *page_emails)
                    if len(unvisited_links) > 0 and depth < args.depth:
                        for link in unvisited_links:
                            domain = get_domain(link)
                            domain_count = redis_client.hincrby(domain_count_key, domain, 1)
                            zadd_mapping[f"{depth + 1},{link}"] = int(domain_count)
                    redis_client.sadd(visited_key, hashed_url)

                    print(f"Depth {depth} added {len(page_emails)} email(s) and {len(unvisited_links)}/{len(links)} URL(s) processing {url}")
                except Exception as e:
                    print(f"Error processing {url}: {e}")

            redis_client.zadd(to_visit_key, zadd_mapping)

def main(args):
    redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)

    if args.url:
        if is_valid_url(args.url):
            domain = get_domain(args.url)
            redis_client.zadd(to_visit_key, {f"0,{args.url}": 0})
            redis_client.hincrby(domain_count_key, domain, 1)
        else:
            print("Invalid URL. Please provide a valid URL.")
            sys.exit(1)

    print(f"Starting email scraping from: {args.url}")
    scrape_emails(args, redis_client)
    print(f"Email scraping completed for: {args.url}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape email addresses with a starting url"
    )

    parser.add_argument(
        "--url",
        type=str,
        default="",
        help="Starting URL (default: None)"
    )

    parser.add_argument(
        "--threads",
        type=int,
        default=8,
        help="Number of concurrent threads (default: 8)."
    )

    parser.add_argument(
        "--domain",
        type=str,
        default="",
        help="Only scrape URLs whose hostname ends with the specified domain."
    )

    parser.add_argument(
        "--depth",
        type=int,
        default=10,
        help="Maximum depth to search (default: 10)."
    )

    args = parser.parse_args()

    main(args)