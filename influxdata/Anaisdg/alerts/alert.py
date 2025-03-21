
from dataclasses import dataclass
from typing import Dict, Any, Optional
import httpx
import json
import asyncio
import base64
from string import Template
from urllib.parse import urlparse
import os
import datetime

@dataclass
class WebhookConfig:
    """Configuration class for webhook validation and settings."""
    url: str
    service: str
    domain: str
    path_contains: str

    def validate(self, logger) -> bool:
        """
        Validate webhook URL format based on service type.
        
        Args:
            logger: Logger instance for error reporting
            
        Returns:
            bool: True if URL is valid for the service, False otherwise
        """
        if not self.url:
            logger.error(f"Please provide a {self.service} webhook URL.")
            return False
        
        if self.service == 'http':
            return True
            
        try:
            result = urlparse(self.url)
            issues = []
            if result.scheme not in ('http', 'https'):
                issues.append("URL must start with 'https://'")
            if not result.netloc.endswith(self.domain):
                issues.append(f"Domain must end with '{self.domain}'")
            if self.path_contains not in result.path:
                issues.append(f"URL must contain '{self.path_contains}'")

            if issues:
                logger.error(f"{self.service} webhook URL needs fixes:")
                for issue in issues:
                    logger.error(f"- {issue}")
                return False
            return True
        except Exception as e:
            logger.error(f"Unable to parse the webhook URL: {str(e)}")
            return False

def get_webhook_config(service: str) -> WebhookConfig:
    """
    Get webhook configuration for specified service.
    
    Args:
        service: Service identifier ('slack', 'discord', or 'http')
        
    Returns:
        WebhookConfig: Configuration object for the specified service
    """
    configs = {
        'slack': WebhookConfig('', 'Slack', 'slack.com', '/services/'),
        'discord': WebhookConfig('', 'Discord', 'discord.com', 'api/webhooks'),
        'http': WebhookConfig('', 'HTTP', '', ''),
    }
    return configs.get(service, WebhookConfig('', 'Generic', '', ''))

def interpolate_notification_text(text: str, row_data: Dict[str, Any]) -> str:
    """
    Replace variables in notification text with actual values from row data.
    
    Args:
        text: Template string with variables
        row_data: Dictionary containing values to interpolate
        
    Returns:
        str: Interpolated text with variables replaced
    """
    return Template(text).safe_substitute(row_data)

def build_payload(endpoint_type: str, notification_text: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build notification payload based on endpoint type.
    
    Args:
        endpoint_type: Type of endpoint ('slack', 'discord', or 'http')
        notification_text: Message to send
        args: Additional arguments for payload construction
        
    Returns:
        dict: Formatted payload for the specified endpoint
    """
    if endpoint_type == 'http':
        return {"message": notification_text}
        
    payloads = {
        'slack': {
            "channel": args.get('channel', '#notifications-testing'),
            "username": args.get('username', 'InfluxDB Alert Bot'),
            "text": notification_text
        },
        'discord': {"content": notification_text}
    }
    return payloads.get(endpoint_type, {"message": notification_text})

async def alert_async(logger, endpoint_type: str, notification_text: str, 
                     args: Optional[Dict[str, Any]] = None, 
                     table_batches: Optional[list] = None, 
                     max_retries: int = 3) -> None:
    """
    Send asynchronous alerts with retry logic.
    
    Args:
        logger: Logger instance for status reporting
        endpoint_type: Type of endpoint to send alert to
        notification_text: Alert message
        args: Optional configuration arguments
        table_batches: Optional batch data
        max_retries: Maximum number of retry attempts
    """
    webhook_url = args.get('webhook_url') if args else None
    if not webhook_url:
        logger.error("Notification endpoint is not provided.")
        return

    payload = build_payload(endpoint_type, notification_text, args or {})
    headers = parse_headers(args) if args else {}

    async with httpx.AsyncClient() as client:
        for attempt in range(max_retries):
            try:
                response = await client.post(webhook_url, json=payload, headers=headers, timeout=10)
                if response.status_code in [200, 204]:  # Discord returns 204 on success
                    logger.info("Alert sent successfully")
                    return
                logger.info(f"Failed to send alert, attempt {attempt + 1}/{max_retries}")
            except httpx.RequestError as e:
                logger.error(f"Request error: {str(e)}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
        logger.error("Max retries reached. Alert not sent.")

def parse_headers(args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse and validate headers from base64 encoded string.
    
    Args:
        args: Dictionary containing base64 encoded headers
        
    Returns:
        dict: Decoded headers or empty dict if invalid
    """
    try:
        headers_b64 = args.get('headers', '')
        if not headers_b64:
            return {}
        padding = len(headers_b64) % 4
        if padding:
            headers_b64 += '=' * (4 - padding)
        headers_json = base64.b64decode(headers_b64).decode('utf-8')
        decoded_headers = json.loads(headers_json)
        logger.info(f"Decoded headers: {decoded_headers}")
        return decoded_headers
    except Exception:
        return {}

def process_writes(logger, table_batches: list, args: Optional[Dict[str, Any]] = None) -> None:
    """
    Process writes and trigger alerts based on conditions.
    
    Args:
        logger: Logger instance for status reporting
        table_batches: List of data batches to process
        args: Optional configuration arguments
    """
    try:
        if not args:
            logger.error("No arguments provided")
            return

        # Validate required parameters
        required_params = ['name', 'endpoint_type', 'threshold', 'field_name']
        missing_params = [param for param in required_params if param not in args]
        if missing_params:
            logger.error(f"Missing required parameters: {', '.join(missing_params)}")
            return

        try:
            threshold = float(args['threshold'])
        except ValueError:
            logger.error("Threshold must be a valid number")
            return

        endpoint_type = args['endpoint_type']
        field_name = args['field_name']
        notification_text = args.get('notification_text', 'InfluxDB 3 alert triggered.')

        # Get webhook URL from args or environment variables
        webhook_url = args.get('webhook_url')
        if not webhook_url:
            if endpoint_type == 'slack':
                webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            elif endpoint_type == 'discord':
                webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
            elif endpoint_type == 'http':
                webhook_url = os.getenv('HTTP_WEBHOOK_URL')
        if not webhook_url:
            logger.error(f"No webhook URL provided for {endpoint_type}")
            return

        webhook_config = get_webhook_config(endpoint_type)
        webhook_config.url = webhook_url
        
        # Validate URL before proceeding
        if not webhook_config.validate(logger):
            logger.error(f"Invalid {endpoint_type} webhook URL format")
            return
            
        logger.info(f"Received webhook URL: {webhook_config.url}")

        # Update args with the webhook URL to ensure it's passed to alert_async
        args['webhook_url'] = webhook_url

        alert_db = args.get('alert_db')
        
        for batch in table_batches:
            if isinstance(batch, dict) and 'rows' in batch:
                table_name = batch.get('table_name', 'alerts')
                for row in batch['rows']:
                    field_value = row.get(field_name, 0)
                    if field_value > threshold:
                        template_data = {field_name: field_value}
                        interpolated_text = interpolate_notification_text(notification_text, template_data)
                        asyncio.run(alert_async(logger, endpoint_type, interpolated_text, args, table_batches))
                        
                        # Write alert to database if specified
                        if alert_db:
                            try:
                                line = LineBuilder(table_name)
                                # Add plugin name
                                line.string_field("plugin_name", args['name'])
                                # Add all fields from original row
                                for key, value in row.items():
                                    if key == 'time':
                                        line.timestamp(int(value))
                                    elif isinstance(value, (int, float)):
                                        line.float64_field(key, value)
                                    else:
                                        line.string_field(key, str(value))
                                
                                # Add alert specific fields
                                line.string_field("alert_message", interpolated_text)
                                line.string_field("processed_at", datetime.datetime.utcnow().isoformat())
                                
                                logger.info(f"Writing alert to database: {alert_db}")
                                logger.write_to_db(alert_db, line)
                            except Exception as e:
                                logger.error(f"Failed to write alert to database: {str(e)}")
                        return

    except Exception as e:
        logger.error(f"Error in plugin: {str(e)}")
