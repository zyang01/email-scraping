import smtplib
import random
import time
import json
import logging
import redis
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
EMAIL_TEMPLATE_FILE = "legislature_email.html"

# Load SMTP credentials
with open(SMTP_CREDENTIALS_FILE, "r") as f:
    smtp_accounts = json.load(f)

# Load email template
with open(EMAIL_TEMPLATE_FILE, "r") as f:
    email_template = f.read()

# Initialize Redis connection
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

def get_random_email():
    available_emails = redis_client.sdiff('media:emails', 'media:sent', 'media:failed')
    if available_emails:
        return random.choice(list(available_emails)).decode('utf-8')
    else:
        log_message("No more emails to send.")
        return None

def mark_email_as_sent(email):
    redis_client.sadd('media:sent', email)

def mark_email_as_failed(email):
    redis_client.sadd('media:failed', email)

def send_email(smtp_info, recipient, retry_delay=60):
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
            mark_email_as_sent(recipient)
            return True  # Email sent successfully
    except Exception as e:
        log_message(f"Failed to send email to {recipient} using {smtp_info['email']}: {e}")
        mark_email_as_failed(recipient)
        log_message(f"Waiting {retry_delay} seconds before trying the next SMTP account...")
        time.sleep(retry_delay)
        return False  # Email sending failed

# Send emails with rotating SMTP accounts
i = 0
retry_delay = 60  # Initial retry delay
while True:
    recipient = get_random_email()
    if recipient is None:
        break
    smtp_info = smtp_accounts[i % len(smtp_accounts)]
    if send_email(smtp_info, recipient, retry_delay):
        retry_delay = 60  # Reset retry delay on success
    else:
        retry_delay *= 2  # Exponential backoff on failure
    
    # Random delay between emails (1 to 30 seconds)
    delay = random.randint(1, 30)
    log_message(f"Waiting {delay} seconds before sending the next email...")
    time.sleep(delay)
    i += 1

log_message("Email sending completed.")
