import sys
import re
import hashlib
import requests
import argparse
import redis
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse

def get_base_domain(url, level):
    parsed_url = urlparse(url)
    hostname = parsed_url.hostname
    if not hostname:
        return None
    parts = hostname.split('.')
    return '.'.join(parts[-level:]) if len(parts) >= level else hostname

def is_same_hostname(url1, url2, level):
    if level == 0:
        return True

    domain1 = get_base_domain(url1, level)
    domain2 = get_base_domain(url2, level)
    return domain1 == domain2

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
            timeout=5
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

def scrape_emails(args, redis_client):
    visited_key = "visited_urls"
    emails_key = "scraped_emails"

    to_visit = {args.url}
    depth = 0

    with ThreadPoolExecutor(args.threads) as executor:
        while to_visit and depth < args.depth:
            remaining_depth = args.depth - depth
            futures = {executor.submit(scrape_page, url): url for url in to_visit}
            to_visit = set()

            for future in futures:
                url = futures[future]
                hashed_url = hash_url(url)

                try:
                    if redis_client.hexists(visited_key, hashed_url):
                        continue

                    page_emails, links = future.result()

                    for email in page_emails:
                        redis_client.hset(emails_key, email, 1)

                    to_visit.update(
                        link for link in links
                        if is_same_hostname(args.url, link, args.hostnameLvl) and not redis_client.hexists(visited_key, hash_url(link))
                    )

                    redis_client.hset(visited_key, hashed_url, remaining_depth)
                    print(f"Total email(s) {redis_client.hlen(emails_key)}. Done processing {url}")
                except Exception as e:
                    print(f"Error processing {url}: {e}")

            depth += 1

def main(args):
    if not is_valid_url(args.url):
        print("Invalid URL. Please provide a valid URL.")
        sys.exit(1)

    redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)
    previous_emails = redis_client.hlen('scraped_emails')

    print(f"Starting email scraping from: {args.url}")
    scrape_emails(args, redis_client)

    new_emails = redis_client.hlen('scraped_emails') - previous_emails
    print(f"Email scraping completed. New unique emails: {new_emails}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape email addresses from a given website"
    )

    parser.add_argument(
        "url",
        type=str,
        help="Starting URL"
    )

    parser.add_argument(
        "--hostnameLvl",
        type=int,
        default=0,
        help="Scrape only links from hostnames with matching specified levels of hostname (default: 0)."
    )

    parser.add_argument(
        "--depth",
        type=int,
        default=5,
        help="Maximum depth to search (default: 5)."
    )

    parser.add_argument(
        "--threads",
        type=int,
        default=8,
        help="Number of concurrent threads (default: 8)."
    )

    args = parser.parse_args()

    main(args)