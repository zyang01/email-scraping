import smtplib
import random
import time
import json
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Configure logging
logging.basicConfig(filename='email_log.txt', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def log_message(message):
    print(message)
    logging.info(message)

# Load SMTP credentials from a JSON file
SMTP_CREDENTIALS_FILE = "smtp_credentials.json"
EMAIL_ADDRESSES_FILE = "email_addresses.txt"
EMAIL_TEMPLATE_FILE = "legislature_email.html"

# Load SMTP credentials
with open(SMTP_CREDENTIALS_FILE, "r") as f:
    smtp_accounts = json.load(f)

# Load recipient email addresses
with open(EMAIL_ADDRESSES_FILE, "r") as f:
    email_addresses = [line.strip() for line in f.readlines() if line.strip()]

# Load email template
with open(EMAIL_TEMPLATE_FILE, "r") as f:
    email_template = f.read()

def send_email(smtp_info, recipient):
    try:
        msg = MIMEMultipart()
        msg['From'] = smtp_info['email']
        msg['To'] = recipient
        msg['Subject'] = "On Love, Artificial Intelligence and Narcissistic Personality Disorder"
        msg.attach(MIMEText(email_template, 'html'))
        
        with smtplib.SMTP(smtp_info['server'], smtp_info['port']) as server:
            server.starttls()
            server.login(smtp_info['email'], smtp_info['password'])
            server.sendmail(smtp_info['email'], recipient, msg.as_string())
            log_message(f"Email sent to {recipient} using {smtp_info['email']}")
    except Exception as e:
        log_message(f"Failed to send email to {recipient} using {smtp_info['email']}: {e}")

# Iterate over email addresses and send emails with rotating SMTP accounts
for i, recipient in enumerate(email_addresses):
    smtp_info = smtp_accounts[i % len(smtp_accounts)]
    send_email(smtp_info, recipient)
    
    # Random delay between emails (1 to 30 seconds)
    delay = random.randint(1, 30)
    log_message(f"Waiting {delay} seconds before sending the next email...")
    time.sleep(delay)

log_message("Email sending completed.")
