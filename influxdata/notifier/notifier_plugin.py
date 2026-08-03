"""
{
    "plugin_type": ["http"]
}
"""

import asyncio
import base64
import json
import os
import random
import time
import uuid
from json import JSONDecodeError

import httpx
from twilio.base.exceptions import TwilioRestException
from twilio.rest import Client


def send_sms_via_twilio(influxdb3_local, params: dict, task_id: str) -> bool:
    """
    Sends an SMS via the Twilio API.

    Args:
        influxdb3_local: InfluxDB client instance.
        params (dict): A dictionary containing the following keys:
            - twilio_sid (str): Your Twilio Account SID.
            - twilio_token (str): Your Twilio Auth Token.
            - twilio_from_number (str): Twilio phone number in E.164 format (e.g., "+1234567890").
            - twilio_to_number (str): Recipient phone number in E.164 format.
            - notification_text (str): Text content of the message.
        task_id (str): Unique task identifier.

    Raises:
        Exception: If there is a network error or authentication failure.
    """

    # Extract required parameters
    account_sid: str | None = params.get("twilio_sid") or os.getenv("TWILIO_SID")
    auth_token: str | None = params.get("twilio_token") or os.getenv("TWILIO_TOKEN")
    if not account_sid or not auth_token:
        influxdb3_local.error(f"[{task_id}] SMS notification failed - Missing Twilio credentials (sid_provided={bool(params.get('twilio_sid'))}, token_provided={bool(params.get('twilio_token'))}, env_sid_set={bool(os.getenv('TWILIO_SID'))}, env_token_set={bool(os.getenv('TWILIO_TOKEN'))})")
        return False

    from_number: str | None = params.get("twilio_from_number", None)
    to_number: str | None = params.get("twilio_to_number", None)
    if not from_number or not to_number:
        influxdb3_local.error(f"[{task_id}] SMS notification failed - Missing phone numbers (from_provided={bool(from_number)}, to_provided={bool(to_number)})")
        return False

    notification_text: str = params["notification_text"]
    message_length: int = len(notification_text)
    max_retries: int = 3

    influxdb3_local.info(f"[{task_id}] Starting SMS notification (from={from_number}, to={to_number[:5]}***{to_number[-3:] if len(to_number) > 8 else '***'}, message_length={message_length})")

    for attempt in range(1, max_retries + 1):
        try:
            client = Client(account_sid, auth_token)
            message = client.messages.create(
                to=to_number, from_=from_number, body=notification_text
            )
            influxdb3_local.info(f"[{task_id}] SMS notification success (sid={message.sid}, attempt={attempt})")
            return True
        except TwilioRestException as e:
            influxdb3_local.warn(f"[{task_id}] Twilio error (attempt {attempt}): {e}")
        except Exception as e:
            influxdb3_local.warn(
                f"[{task_id}] Unexpected error while sending SMS message (attempt {attempt}): {e}"
            )

        if attempt < max_retries:
            wait: float = random.uniform(1, 4)
            time.sleep(wait)

    influxdb3_local.error(f"[{task_id}] SMS notification failed permanently (max_attempts={max_retries}, from={from_number}, to={to_number[:5]}***{to_number[-3:] if len(to_number) > 8 else '***'})")
    return False


def send_whatsapp_via_twilio(influxdb3_local, params: dict, task_id: str) -> bool:
    """
    Sends a WhatsApp message via the Twilio API.

    Args:
        influxdb3_local: InfluxDB client instance.
        params (dict): A dictionary containing the following keys:
            - twilio_sid (str): Your Twilio Account SID.
            - twilio_token (str): Your Twilio Auth Token.
            - twilio_from_number (str): Twilio sandbox or approved WhatsApp number (e.g., "+1234567890").
            - twilio_to_number (str): Recipient WhatsApp number in E.164 format.
            - notification_text (str): Text content of the message.
        task_id (str): Unique task identifier.

    Returns:
        bool: True if message was sent successfully, False otherwise.
    """

    # Extract required parameters
    account_sid: str | None = params.get("twilio_sid") or os.getenv("TWILIO_SID")
    auth_token: str | None = params.get("twilio_token") or os.getenv("TWILIO_TOKEN")
    if not account_sid or not auth_token:
        influxdb3_local.error(f"[{task_id}] WhatsApp notification failed - Missing Twilio credentials (sid_provided={bool(params.get('twilio_sid'))}, token_provided={bool(params.get('twilio_token'))}, env_sid_set={bool(os.getenv('TWILIO_SID'))}, env_token_set={bool(os.getenv('TWILIO_TOKEN'))})")
        return False

    from_number: str | None = params.get("twilio_from_number", None)
    to_number: str | None = params.get("twilio_to_number", None)
    if not from_number or not to_number:
        influxdb3_local.error(f"[{task_id}] WhatsApp notification failed - Missing phone numbers (from_provided={bool(from_number)}, to_provided={bool(to_number)})")
        return False

    body: str = params["notification_text"]
    message_length: int = len(body)
    max_retries: int = 3

    influxdb3_local.info(f"[{task_id}] Starting WhatsApp notification (from=whatsapp:{from_number}, to=whatsapp:{to_number[:5]}***{to_number[-3:] if len(to_number) > 8 else '***'}, message_length={message_length})")

    for attempt in range(1, max_retries + 1):
        try:
            client = Client(account_sid, auth_token)
            message = client.messages.create(
                to=f"whatsapp:{to_number}", from_=f"whatsapp:{from_number}", body=body
            )
            influxdb3_local.info(f"[{task_id}] WhatsApp notification success (sid={message.sid}, attempt={attempt})")
            return True
        except TwilioRestException as e:
            influxdb3_local.warn(f"[{task_id}] Twilio error (attempt {attempt}): {e}")
        except Exception as e:
            influxdb3_local.warn(
                f"[{task_id}] Unexpected error while sending WhatsApp message (attempt {attempt}): {e}"
            )

        if attempt < max_retries:
            wait: float = random.uniform(1, 4)
            time.sleep(wait)

    influxdb3_local.error(f"[{task_id}] WhatsApp notification failed permanently (max_attempts={max_retries}, from=whatsapp:{from_number}, to=whatsapp:{to_number[:5]}***{to_number[-3:] if len(to_number) > 8 else '***'})")
    return False


async def alert_async(
    influxdb3_local, endpoint_type: str, args: dict, task_id: str
) -> bool:
    """
    Send asynchronous alerts with retry logic.

    Args:
        influxdb3_local: InfluxDB client instance
        endpoint_type (str): Type of endpoint to send alert to
        args (dict): configuration arguments
        task_id (str: Unique task identifier
    """
    influxdb3_local.info(f"[{task_id}] Sending notification via {endpoint_type}")
    webhook_url: str = args.get(f"{endpoint_type}_webhook_url")
    if not webhook_url:
        influxdb3_local.error(f"[{task_id}] Webhook URL not found for {endpoint_type}")
        return False

    payload: dict = build_payload(endpoint_type, args)
    headers: dict = parse_headers(influxdb3_local, args, endpoint_type, task_id)
    max_retries: int = 3

    async with httpx.AsyncClient() as client:
        for attempt in range(max_retries):
            try:
                response = await client.post(
                    webhook_url,
                    json=payload,
                    headers=headers,
                    timeout=10,
                )
                if response.status_code in [200, 204]:  # Discord returns 204 on success
                    influxdb3_local.info(
                        f"[{task_id}] Notification sent successfully via {endpoint_type}"
                    )
                    return True

                try:
                    response_data = await response.json()
                except Exception:
                    response_data = response.text

                influxdb3_local.info(
                    f"[{task_id}] Failed to send notification via {endpoint_type}, \
                    attempt {attempt + 1}/{max_retries}, \
                    response status code: {response.status_code}, \
                    response_data: {response_data}"
                )
            except Exception as e:
                influxdb3_local.error(f"[{task_id}] Request error: {str(e)}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2**attempt)
        influxdb3_local.error(
            f"[{task_id}] Max retries reached. Alert via {endpoint_type} not sent."
        )
        return False


def build_payload(endpoint_type: str, args: dict) -> dict:
    """
    Build notification payload based on endpoint type.

    Args:
        endpoint_type: Type of endpoint ('slack', 'discord', or 'http')
        args: Additional arguments for payload construction

    Returns:
        dict: Formatted payload for the specified endpoint
    """
    if endpoint_type == "http":
        return {"message": args["notification_text"]}

    payloads: dict = {
        "slack": {"text": args["notification_text"]},
        "discord": {"content": args["notification_text"]},
    }
    return payloads.get(endpoint_type, {"message": args["notification_text"]})


def parse_headers(influxdb3_local, args: dict, key: str, task_id: str) -> dict:
    """
    Parse and validate headers from base64 encoded string.

    Args:
        influxdb3_local: InfluxDB client instance
        args (dict): Dictionary containing base64 encoded headers
        key (str): Key in args where headers are stored
        task_id (str): Unique task identifier

    Returns:
        dict: Decoded headers or empty dict if invalid
    """
    try:
        headers_b64: str = args.get(f"{key}_headers", "")
        if not headers_b64:
            return {}

        padding = len(headers_b64) % 4
        if padding:
            headers_b64 += "=" * (4 - padding)
        headers_json = base64.b64decode(headers_b64).decode("utf-8")
        decoded_headers = json.loads(headers_json)
        return decoded_headers
    except Exception:
        influxdb3_local.warn(f"[{task_id}] Failed to parse headers for {key}")
        return {}


def process_request(
    influxdb3_local, query_parameters, request_headers, request_body, args=None
):
    """
    Process an incoming HTTP request to trigger notifications via configured senders.
    """
    task_id: str = str(uuid.uuid4())
    influxdb3_local.info(f"[{task_id}] Starting request process")

    # Process the request body
    if request_body:
        try:
            data: dict = json.loads(request_body)
        except JSONDecodeError:
            influxdb3_local.error(f"[{task_id}] Invalid JSON in request body.")
            return {"status": "failed", "message": "Invalid JSON in request body."}
    else:
        influxdb3_local.error(f"[{task_id}] No request body provided.")
        return {"status": "failed", "message": "No request body provided."}

    if "senders_config" not in data or "notification_text" not in data:
        influxdb3_local.error(f"[{task_id}] Missing required fields in request body: 'senders_config' or 'notification_text'.")
        return {
            "status": "failed",
            "message": "Missing required fields: 'senders_config' or 'notification_text'."
        }

    senders_functions: dict = {
        "slack": alert_async,
        "discord": alert_async,
        "http": alert_async,
        "whatsapp": send_whatsapp_via_twilio,
        "sms": send_sms_via_twilio,
    }

    results: dict = {}
    for sender, configs in data["senders_config"].items():
        configs["notification_text"] = data["notification_text"]

        if sender in senders_functions and sender in ["slack", "discord", "http"]:
            result: bool = asyncio.run(
                senders_functions[sender](influxdb3_local, sender, configs, task_id)
            )
        elif sender in senders_functions and sender in ["whatsapp", "sms"]:
            result: bool = senders_functions[sender](influxdb3_local, configs, task_id)
        else:
            influxdb3_local.warn(f"[{task_id}] Invalid sender: {sender}")
            result: str = f"Invalid sender"

        results[sender] = result
    influxdb3_local.info(f"[{task_id}] Finished processing all senders.")
    return {"status": "success", "message": "Request processed", "results": results}
