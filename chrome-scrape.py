import time
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

# Set up Selenium WebDriver (Chrome example)
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run headless (without opening browser window)
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")

# Path to ChromeDriver (adjust the path if needed)
service = Service("/usr/bin/chromedriver")

driver = webdriver.Chrome(service=service, options=chrome_options)

# Function to extract email from a webpage
def extract_email(url):
    driver.get(url)
    
    # Wait for the JavaScript content to render (adjust if necessary)
    time.sleep(3)  # Adjust sleep time if necessary to wait for JS rendering

    # Get the page source and parse it with BeautifulSoup
    soup = BeautifulSoup(driver.page_source, "html.parser")
    
    # Look for email addresses using regex (basic email extraction pattern)
    email = None
    email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"

    email = re.search(email_pattern, soup.get_text())
    print(soup.get_text())
    
    if email:
        return email.group(0)
    return None

# Loop through 10,000 URLs
base_url = "https://members.parliament.uk/member/{}/contact"
emails = []

for i in range(3453, 10000):
    url = base_url.format(i)
    print(f"Processing {url}...")
    
    email = extract_email(url)
    
    if email:
        print(f"Found email: {email}")
        emails.append(email)
    else:
        print("No email found.")

# After looping, close the WebDriver
driver.quit()

# Optionally, save emails to a file
with open("emails.txt", "w") as f:
    for email in emails:
        f.write(f"{email}\n")

print("Finished processing all URLs.")
