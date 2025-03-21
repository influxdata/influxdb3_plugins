"""Unit tests for the alert plugin functionality."""
import unittest
import base64
import json
import asyncio
import httpx
from unittest.mock import Mock

from alert import WebhookConfig, build_payload, parse_headers, alert_async

class TestAlertPlugin(unittest.TestCase):
    def test_webhook_config_validation(self):
        """Test webhook URL validation for different services."""
        logger = Mock()

        # Test valid Slack webhook
        slack_config = WebhookConfig(
            'https://hooks.slack.com/services/ABC123',
            'Slack', 'slack.com', '/services/'
        )
        self.assertTrue(slack_config.validate(logger))

        # Test valid Discord webhook
        discord_config = WebhookConfig(
            'https://discord.com/api/webhooks/123/abc',
            'Discord', 'discord.com', 'api/webhooks'
        )
        self.assertTrue(discord_config.validate(logger))

        # Test HTTP generic endpoint
        http_config = WebhookConfig(
            'https://api.example.com/webhook',
            'http', '', ''
        )
        self.assertTrue(http_config.validate(logger))

    def test_build_payload(self):
        """Test payload construction for different services."""
        self.assertEqual(build_payload('discord', 'Test message', {})['content'], 'Test message')
        self.assertEqual(build_payload('slack', 'Test message', {'channel': '#test'})['text'], 'Test message')
        self.assertEqual(build_payload('http', 'Test message', {})['message'], 'Test message')

    def test_parse_headers(self):
        """Test header parsing functionality."""
        self.assertEqual(parse_headers({}), {})
        headers = {'Authorization': 'Bearer token123'}
        encoded_headers = base64.b64encode(json.dumps(headers).encode()).decode()
        self.assertEqual(parse_headers({'headers': encoded_headers}), headers)

    def test_alert_async(self):
        """Test alert sending with retry mechanism."""
        logger = Mock()
        with unittest.mock.patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value.status_code = 200
            asyncio.run(alert_async(logger, 'discord', 'test message', {'webhook_url': 'http://test'}))
            self.assertTrue(mock_post.called)

if __name__ == '__main__':
    unittest.main()