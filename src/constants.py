import logging
import os

from dotenv import load_dotenv

load_dotenv()


DATABASE_CONNECTION_STRING = os.environ["DATABASE_CONNECTION_STRING"]
LOGGING_LEVEL = logging.DEBUG

# Twilio settings
TWILIO_ACCOUNT_SID = os.environ["TWILIO_ACCOUNT_SID"]
TWILIO_AUTH_TOKEN = os.environ["TWILIO_AUTH_TOKEN"]
TWILIO_PHONE_NUMBER = os.environ["TWILIO_PHONE_NUMBER"]
