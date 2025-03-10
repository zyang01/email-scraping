import sys
import re
import hashlib
import time
import redis.client
import requests
import argparse
import redis
import json
import os
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Pool
from urllib.parse import urljoin, urlparse

visited_key = "visited_urls"
emails_key = "scraped_emails"
to_visit_key = "to_visit_urls_set"
domain_count_key = "domain_count"
hostname_to_name_key = "hostname_to_name"
failed_key = "failed_urls"
hostname_count_key = "hostname_count"
processing_key = "processing_urls"
processed_key = "processed_urls"

def get_emails_from_text(text):
    email_pattern = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
    return re.findall(email_pattern, text)

def scrape_page(url):
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
    hostname = parsed.hostname
    if (hostname.startswith('www.')):
        hostname = hostname[4:]
    return hostname

def process_json_file(json_file, redis_client):
    with open(json_file, 'r') as file:
        data = json.load(file)
        for entry in data:
            name = entry['name']
            links = entry['links']
            for link in links:
                if is_valid_url(link):
                    hostname = get_domain(link)
                    redis_client.hset(hostname_to_name_key, hostname, name)
                    if not redis_client.sismember(visited_key, hash_url(link)):
                        count = redis_client.hincrby(hostname_count_key, hostname, 1)
                        val = f"{hostname}:{link}"
                        if not redis_client.zscore(to_visit_key, val):
                            redis_client.zadd(to_visit_key, {val: count})
                            print(f"Added {val} to the list of URLs to visit with score {count}")

def process_txt_file(txt_file, redis_client):
    with open(txt_file, 'r') as file:
        for line in file:
            url = line.strip()
            if is_valid_url(url):
                hostname = get_domain(url)
                redis_client.hset(hostname_to_name_key, hostname, hostname)
                if not redis_client.sismember(visited_key, hash_url(url)):
                    count = redis_client.hincrby(hostname_count_key, hostname, 1)
                    val = f"{hostname}:{url}"
                    if not redis_client.zscore(to_visit_key, val):
                        redis_client.zadd(to_visit_key, {val: count})
                        print(f"Added {val} to the list of URLs to visit with score {count}")

def get_redis_client():
    redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
    return redis.StrictRedis.from_url(redis_url, decode_responses=True)

def scrape_emails(args):
    redis_client = get_redis_client()
    with ThreadPoolExecutor(args.threads) as executor:
        while True:
            to_visit = redis_client.zpopmin(to_visit_key, args.threads)

            if not to_visit:
                print("No more URLs to visit. Checking again in 30s.")
                time.sleep(30)
                continue

            for url, score in to_visit:
                redis_client.zadd(processing_key, {url: score})

            futures = {executor.submit(scrape_page, url.split(':', 1)[1]): (url, score) for url, score in to_visit}

            for future in as_completed(futures):
                url, score = futures[future]
                hostname, url = url.split(':', 1)
                hashed_url = hash_url(url)

                try:
                    page_emails, links = future.result()
                    links = [link for link in links if get_domain(link).endswith(hostname)]
                    ismembers = redis_client.smismember(visited_key, map(hash_url, links)) if len(links) > 0 else []
                    unvisited_links = [link for link, ismember in zip(links, ismembers) if not ismember]

                    if len(page_emails) > 0:
                        formatted_emails = [f"{hostname}:{email}" for email in page_emails]
                        redis_client.sadd(emails_key, *formatted_emails)
                    if len(unvisited_links) > 0:
                        for link in unvisited_links:
                            count = redis_client.hincrby(hostname_count_key, hostname, 1)
                            val = f"{hostname}:{link}"
                            if not redis_client.zscore(to_visit_key, val):
                                redis_client.zadd(to_visit_key, {val: count})
                    redis_client.sadd(visited_key, hashed_url)
                    redis_client.zrem(processing_key, f"{hostname}:{url}")
                    redis_client.zadd(processed_key, {f"{hostname}:{url}": score})

                    print(f"Added {len(page_emails)} email(s) and {len(unvisited_links)}/{len(links)} URL(s) processing {url}")
                except Exception as e:
                    print(f"Error processing {url}: {e}")
                    redis_client.zrem(processing_key, f"{hostname}:{url}")
                    redis_client.zadd(failed_key, {f"{hostname}:{url}": score})

def main(args):
    redis_client = get_redis_client()
    
    if args.json_file:
        process_json_file(args.json_file, redis_client)
    
    if args.txt_file:
        process_txt_file(args.txt_file, redis_client)
        print("Processed txt file and added URLs to visit. Exiting.")
        return

    print(f"Starting email scraping")

    with Pool(args.processes) as pool:
        pool.map(scrape_emails, [args] * args.processes)
        pool.close()
        pool.join()

    print(f"Email scraping completed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape email addresses"
    )

    parser.add_argument(
        "--threads",
        type=int,
        default=8,
        help="Number of concurrent threads (default: 8)."
    )

    parser.add_argument(
        "--processes",
        type=int,
        default=4,
        help="Number of concurrent processes (default: 4)."
    )

    parser.add_argument(
        "--json_file",
        type=str,
        help="Path to a JSON file containing names and links."
    )

    parser.add_argument(
        "--txt_file",
        type=str,
        help="Path to a txt file containing URLs."
    )

    args = parser.parse_args()

    main(args)