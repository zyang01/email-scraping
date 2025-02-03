import math
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
from multiprocessing import Process
import os
import socket

visited_key = "visited_urls"
emails_key = "scraped_emails"
to_visit_key = "to_visit_urls"
domain_count_key = "domain_count"
shutdown_key = "shutdown"
register_key = "register"

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

def scrape_emails(args, redis_client, hostname):
    end_time = time.time() + 3600  # Run for one hour

    with ThreadPoolExecutor(args.threads) as executor:
        while time.time() < end_time:
            if redis_client.get(shutdown_key) == "yes" or \
                redis_client.hget(register_key, hostname) == "shutdown":
                print("Shutdown signal received. Exiting child process...")
                break

            to_visit = redis_client.zpopmin(to_visit_key, args.threads)

            if not to_visit:
                print("No more URLs to visit. Checking again in 10s.")
                time.sleep(10)
                continue

            futures = {executor.submit(scrape_page, url): (url, score) for url, score in to_visit}
            zadd_mapping = {}
            email_count = 0

            for future in as_completed(futures):
                url, score = futures[future]
                hashed_url = hash_url(url)
                penalty = math.sqrt(score)

                try:
                    page_emails, links = future.result()
                    links = [link for link in links if is_valid_domain(link, args.domain)]
                    ismembers = redis_client.smismember(visited_key, map(hash_url, links)) if len(links) > 0 else []
                    unvisited_links = [link for link, ismember in zip(links, ismembers) if not ismember]

                    if len(page_emails) > 0:
                        redis_client.sadd(emails_key, *page_emails)
                    if len(unvisited_links) > 0:
                        for link in unvisited_links:
                            domain = get_domain(link)
                            domain_count = redis_client.hincrby(domain_count_key, domain, 1)
                            zadd_mapping[link] = math.sqrt(int(domain_count)) + penalty
                    redis_client.sadd(visited_key, hashed_url)
                    email_count += len(page_emails)
                except Exception as e:
                    print(f"Error processing {url}: {e}")

            if zadd_mapping:
                redis_client.zadd(to_visit_key, zadd_mapping, nx=True)
            print(f"Added {email_count} email(s) and {len(zadd_mapping)} URL(s)")

def main(args):
    redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
    redis_client = redis.StrictRedis.from_url(redis_url, decode_responses=True)
    hostname = socket.gethostname()
    redis_client.hset(register_key, hostname, "online")

    if args.url:
        if is_valid_url(args.url):
            domain = get_domain(args.url)
            domain_count = redis_client.hincrby(domain_count_key, domain, 1)
            redis_client.zadd(to_visit_key, {args.url: math.sqrt(int(domain_count))})
        else:
            print("Invalid URL. Please provide a valid URL.")
            sys.exit(1)

    print(f"Starting email scraping from: {args.url}")

    processes = []
    for _ in range(args.processes):
        p = Process(target=scrape_emails, args=(args, redis_client, hostname))
        p.start()
        processes.append(p)

    while processes:
        if redis_client.get(shutdown_key) == "yes" or \
            redis_client.hget(register_key, hostname) == "shutdown":
            print("Shutdown signal received. Exiting main loop...")
            break

        for p in processes[:]:
            p.join(timeout=0)
            if not p.is_alive():
                processes.remove(p)
                if redis_client.get(shutdown_key) != "yes":
                    new_p = Process(target=scrape_emails, args=(args, redis_client, hostname))
                    new_p.start()
                    processes.append(new_p)

        time.sleep(1)

    for p in processes:
        p.join()

    redis_client.hset(register_key, hostname, "offline")
    print("All child processes have terminated. Exiting main process.")

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
        "--processes",
        type=int,
        default=1,
        help="Number of concurrent processes (default: 1)."
    )

    args = parser.parse_args()

    main(args)