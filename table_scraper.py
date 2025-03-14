import json
import pprint
import requests
from bs4 import BeautifulSoup
import csv
import time
import concurrent.futures
import redis

BASE_URL = "https://www.einpresswire.com/world-media-directory/detail/"
OUTPUT_FILE = "einpresswire.csv"
HEADERS = {"User-Agent": "Mozilla/5.0"}  # To avoid getting blocked


def scrape_table(url):
    """Fetches the table with class 'simple full' from the given URL."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to retrieve {url}: {e}")
        return None
    
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", class_="simple full")
    
    if not table:
        print(f"No table found on {url}")
        return None
    
    result = {}
    for row in table.find_all("tr"):
        cells = row.find_all(["th", "td"])
        
        # Skip section headers (e.g., "Website") that have colspan
        if len(cells) == 1 and "colspan" in cells[0].attrs:
            continue
        
        # Extract text and handle links inside <td>
        row_data = []
        for cell in cells:
            link = cell.find("a", class_="verbatim")
            if link:
                row_data.append(link["href"])  # Extract the URL
            else:
                row_data.append(cell.get_text(strip=True))
        
        if row_data:
            if len(row_data) != 2:
                print(f"Unexpected row data: {row_data}")
            else:
                result.update({row_data[0]: row_data[1]})
    
    return result


def main():
    all_data = []
    csv_headers = set()
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
    
    def process_url(number):
        url = f"{BASE_URL}{number}"
        print(f"Scraping {url}")
        table = scrape_table(url)
        if table:
            table["uid"] = number
            return table
        return None
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        futures = [executor.submit(process_url, number) for number in range(1, 100001)]
        for future in concurrent.futures.as_completed(futures):
            table = future.result()
            if table:
                csv_headers.update(table.keys())
                all_data.append(table)
                redis_client.hset("ein", table["uid"], json.dumps(table))
    
    if all_data:
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=sorted(csv_headers))
            writer.writeheader()
            writer.writerows(all_data)
        print(f"Data successfully saved to {OUTPUT_FILE}")
    else:
        print("No data scraped.")


if __name__ == "__main__":
    main()
