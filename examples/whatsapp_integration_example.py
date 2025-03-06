"""Example usage of WhatsApp integration via Twilio."""

import asyncio
import xml.etree.ElementTree as ET
from fastapi.testclient import TestClient

from api.main import app
from api.models import WhatsAppWebhookRequest
from api.config import settings


async def main():
    """Run example demonstrating WhatsApp integration."""
    # Create a test client
    client = TestClient(app)
    
    print("\n=== WhatsApp Integration Example ===")
    
    # Create a mock WhatsApp request
    whatsapp_request = WhatsAppWebhookRequest(
        From="+1234567890",  # Sender's WhatsApp number
        Body="Hello, can you help me with something?",  # Message content
        ProfileName="Test User",  # Sender's profile name
        WaId="1234567890",  # WhatsApp ID
        SmsMessageSid="SM123456789012345678901234567890"  # Twilio message SID
    )
    
    print(f"Sending WhatsApp message: {whatsapp_request.Body}")
    
    # In a real Twilio request, Twilio would include the X-Twilio-Signature header
    # For testing purposes, we'll bypass the signature validation by not including it
    # In production, Twilio will validate the request using the auth token
    
    # Send the request to the WhatsApp endpoint
    headers = {
        # In a real scenario, Twilio would add these headers
        # "X-Twilio-Signature": "...",
        # "X-Original-URL": "https://your-domain.com/chat/whatsapp"
    }
    response = client.post("/chat/whatsapp", json=whatsapp_request.model_dump(), headers=headers)
    
    # Print response details
    print(f"\nStatus Code: {response.status_code}")
    print(f"Content-Type: {response.headers.get('content-type', 'Not specified')}")
    
    # Parse the TwiML response
    if response.headers.get('content-type') == 'application/xml':
        try:
            root = ET.fromstring(response.text)
            message_element = root.find('.//Message')
            if message_element is not None:
                print("\nAI Response:")
                print(f"{message_element.text}")
            else:
                print("\nNo message found in TwiML response")
        except Exception as e:
            print(f"Error parsing XML: {e}")
            print(f"Raw response: {response.text}")
    else:
        print(f"Response: {response.text}")
    
    # Send a follow-up message
    follow_up_request = WhatsAppWebhookRequest(
        From="+1234567890",  # Same sender
        Body="What can you tell me about Python?",  # Follow-up question
        ProfileName="Test User",
        WaId="1234567890",
        SmsMessageSid="SM123456789012345678901234567891"  # Different SID
    )
    
    print(f"\nSending follow-up message: {follow_up_request.Body}")
    
    # Send the follow-up request
    follow_up_response = client.post("/chat/whatsapp", json=follow_up_request.model_dump(), headers=headers)
    
    # Print response details
    print(f"\nStatus Code: {follow_up_response.status_code}")
    print(f"Content-Type: {follow_up_response.headers.get('content-type', 'Not specified')}")
    
    # Parse the TwiML response
    if follow_up_response.headers.get('content-type') == 'application/xml':
        try:
            root = ET.fromstring(follow_up_response.text)
            message_element = root.find('.//Message')
            if message_element is not None:
                print("\nAI Response:")
                print(f"{message_element.text}")
            else:
                print("\nNo message found in TwiML response")
        except Exception as e:
            print(f"Error parsing XML: {e}")
            print(f"Raw response: {follow_up_response.text}")
    else:
        print(f"Response: {follow_up_response.text}")


if __name__ == "__main__":
    asyncio.run(main())
