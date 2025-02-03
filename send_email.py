import redis
import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import asyncio

# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
SCRAPED_EMAILS_SET = 'scraped_emails'  # Redis set with scraped email addresses
SENT_EMAILS_SET = 'sent_emails'  # Redis set to store sent email addresses
FAILED_EMAILS_SET = 'failed_emails'  # Redis set to store failed email addresses
EMAIL_TEMPLATE_FILE = 'template.html'
SMTP_SERVER = '10.96.0.132'
SMTP_PORT = 587
SMTP_USERNAME = 'jonah@thoushaltsendit.cloud'
SMTP_PASSWORD = 'thoushaltsendit'
BATCH_SIZE = 10  # Number of emails to send in each batch

# Connect to Redis
try:
    redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    print("Connected to Redis")
except Exception as e:
    print(f"Error connecting to Redis: {e}")
    exit(1)

# Read the email template from a file
def load_email_template(template_file):
    try:
        with open(template_file, 'r') as file:
            return file.read()
    except Exception as e:
        print(f"Error reading email template: {e}")
        exit(1)

# Send email using SMTP
async def send_email(to_email, subject, body):
    try:
        # Create the MIME email message
        msg = MIMEMultipart()
        msg['From'] = SMTP_USERNAME
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))  # Use 'html' for HTML content

        # Send the email
        await aiosmtplib.send(
            msg, 
            hostname=SMTP_SERVER, 
            port=SMTP_PORT, 
            start_tls=True, 
            username=SMTP_USERNAME, 
            password=SMTP_PASSWORD, 
            validate_certs=False
        )
        print(f"Email sent to {to_email}")

        # Store the email in the Redis set
        redis_client.sadd(SENT_EMAILS_SET, to_email)
    except Exception as e:
        print(f"Failed to send email to {to_email}: {e}")
        redis_client.sadd(FAILED_EMAILS_SET, to_email)

# Main script logic
async def main():
    email_template = load_email_template(EMAIL_TEMPLATE_FILE)
    subject = "Knowledge of Good and Evil"  # Customize as needed

    try:
        # Retrieve email addresses that are in scraped_emails but not in sent_emails or failed_emails
        email_keys = list(redis_client.sdiff(SCRAPED_EMAILS_SET, SENT_EMAILS_SET, FAILED_EMAILS_SET))

        if not email_keys:
            print("No valid email keys found in the Redis set.")
        else:
            print(f"Found {len(email_keys)} email addresses.")
            for i in range(0, len(email_keys), BATCH_SIZE):
                batch = email_keys[i:i + BATCH_SIZE]
                tasks = [asyncio.create_task(send_email(email, subject, email_template)) for email in batch]
                await asyncio.gather(*tasks)
                print(f"Batch {i // BATCH_SIZE + 1} sent.")
    except Exception as e:
        print(f"Error processing email addresses: {e}")

if __name__ == '__main__':
    asyncio.run(main())
