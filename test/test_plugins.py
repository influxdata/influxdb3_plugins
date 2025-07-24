"""
Test script for InfluxDB 3 plugins using HTTP API
Supports testing individual plugins or entire organizations

This module provides both a command-line interface and reusable PluginTester class
for testing InfluxDB 3 plugins with TOML configuration support.
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import requests

# Import shared modules
try:
    from .influxdb_api_client import InfluxDBApiClient, print_status, print_warning, print_error
    from .plugin_config import PluginConfig
    from .trigger_manager import TriggerManager
    from .container_manager import ContainerManager
except ImportError:
    # When running as a script, use absolute imports
    from influxdb_api_client import InfluxDBApiClient, print_status, print_warning, print_error
    from plugin_config import PluginConfig
    from trigger_manager import TriggerManager
    from container_manager import ContainerManager

# Configuration defaults (can be overridden with environment variables)
DEFAULT_DATABASE_NAME = os.getenv("INFLUXDB3_DATABASE_NAME", "testdb")
DEFAULT_HOST_URL = os.getenv("INFLUXDB3_HOST_URL", f"http://localhost:{os.getenv('INFLUXDB3_HOST_PORT', '8181')}")

class BasePluginTester:
    """
    Base class providing common plugin testing operations.
    
    Contains shared functionality for:
    - Docker container management
    - Test environment setup
    - Simplified API access through composition
    """
    
    def __init__(self, database_name: str = DEFAULT_DATABASE_NAME,
                 host_url: str = DEFAULT_HOST_URL,
                 skip_container: bool = False):
        self.database_name = database_name
        self.host_url = host_url
        self.skip_container = skip_container or os.getenv('SKIP_CONTAINER_MANAGEMENT', '').lower() in ('true', '1')
        
        # Initialize API client, trigger manager, and container manager
        self.api_client = InfluxDBApiClient(host_url)
        self.trigger_manager = TriggerManager(self.api_client, database_name, skip_container)
        self.container_manager = ContainerManager(self.api_client, getattr(self, 'service_type', 'core'), skip_container)
        
        # Set appropriate plugin directory for container context
        plugin_dir = "/plugins" if self.skip_container else "/host"
        os.environ["PLUGIN_DIR"] = plugin_dir
        
        if self.skip_container:
            print_status("Container management disabled (running inside container)")
    
    def run_command(self, command: List[str], capture_output: bool = False) -> subprocess.CompletedProcess:
        """Run a command (delegates to container manager)"""
        return self.container_manager.run_command(command, capture_output)
    
    def make_api_request(self, method: str, endpoint: str, data: Optional[Dict] = None, 
                        params: Optional[Dict] = None) -> Tuple[bool, Dict]:
        """Make an API request to InfluxDB 3 (delegates to API client)"""
        return self.api_client.make_request(method, endpoint, data, params)
    
    def start_influxdb(self) -> None:
        """Start InfluxDB container (delegates to container manager)"""
        self.container_manager.start_influxdb()
    
    def wait_for_influxdb(self) -> None:
        """Wait for InfluxDB to be ready (delegates to container manager)"""
        self.container_manager.wait_for_influxdb()
    
    def delete_database(self, db_name: str) -> bool:
        """Delete a database (delegates to API client)"""
        return self.api_client.delete_database(db_name)
    
    def create_database(self, db_name: str) -> bool:
        """Create a database (delegates to API client)"""
        return self.api_client.create_database(db_name)
    
    def install_packages(self, packages: List[str]) -> bool:
        """Install Python packages (delegates to API client)"""
        return self.api_client.install_packages(packages)
    
    def install_package(self, package_name: str) -> bool:
        """Install a single Python package"""
        return self.install_packages([package_name])
    
    def query_internal_db(self, query: str) -> List[Dict]:
        """Query the _internal database (delegates to API client)"""
        return self.api_client.query_internal_db(query)
    
    def disable_trigger(self, trigger_name: str) -> bool:
        """Disable a trigger (delegates to trigger manager)"""
        return self.trigger_manager.disable_trigger(trigger_name)
    
    def delete_trigger(self, trigger_name: str, force: bool = True) -> bool:
        """Delete a trigger (delegates to trigger manager)"""
        return self.trigger_manager.delete_trigger(trigger_name, force)
    
    def write_test_data(self, test_data: str = "temperature,sensor=room1 value=25.5") -> bool:
        """Write test data (delegates to API client)"""
        return self.api_client.write_test_data(self.database_name, test_data)
    
    def test_plugin_dir_env(self) -> bool:
        """Test: Check PLUGIN_DIR environment variable (delegates to container manager)"""
        return self.container_manager.test_plugin_dir_env()
    
    def cleanup(self) -> None:
        """Cleanup created triggers (delegates to trigger manager)"""
        self.trigger_manager.cleanup_created_triggers()
    
    def list_all_triggers(self) -> List[str]:
        """List all triggers (delegates to trigger manager)"""
        return self.trigger_manager.list_all_triggers()
    
    def cleanup_all_test_triggers(self) -> None:
        """Cleanup all test triggers (delegates to trigger manager)"""
        self.trigger_manager.cleanup_all_test_triggers()
    
    def cleanup_test_environment(self) -> None:
        """Complete cleanup of test environment (triggers and database)"""
        print_status("Performing complete test environment cleanup...")
        
        try:
            # Clean up triggers using trigger manager
            self.trigger_manager.cleanup_all()
        except Exception as e:
            print_warning(f"Error during trigger cleanup: {e}")
        
        try:
            # Delete test databases
            self.delete_database(self.database_name)
            if self.database_name != "default":
                self.delete_database("default")  # Also cleanup default db if it's different
        except Exception as e:
            print_warning(f"Error during database cleanup: {e}")

class PluginTestRunner(BasePluginTester):
    """Plugin test runner using HTTP API for organization-level testing"""
    
    def __init__(self, database_name: str = DEFAULT_DATABASE_NAME, 
                 host_url: str = DEFAULT_HOST_URL,
                 service_type: str = "core",
                 skip_container: bool = False):
        self.service_type = service_type
        self.container_name = f"influxdb3-{service_type}"
        super().__init__(database_name, host_url, skip_container)
        
        print_status(f"Set PLUGIN_DIR={os.environ.get('PLUGIN_DIR')} for TOML configuration support")
    
    def start_influxdb_container(self) -> None:
        """Start InfluxDB 3 container with service-specific logic"""
        self.container_manager.start_influxdb()
    def create_trigger(self, trigger_name: str, plugin_path: str, plugin_type: str, 
                      trigger_args: Dict) -> bool:
        """Create a trigger (delegates to trigger manager)"""
        return self.trigger_manager.create_trigger(trigger_name, plugin_path, plugin_type, trigger_args)
    
    def test_plugin(self, plugin_path: str) -> bool:
        """Test a specific plugin with both API and CLI modes"""
        plugin_name = Path(plugin_path).name
        print_status(f"Testing {plugin_path} plugin...")
        
        # Check if plugin file exists
        plugin_file = Path(plugin_path) / f"{plugin_name}.py"
        if not plugin_file.exists():
            print_warning(f"{plugin_name}.py not found at {plugin_path}, skipping test")
            return False
        
        try:
            # Create plugin test configuration
            config = PluginConfig(plugin_path, f"{plugin_name}.py")
            supported_types = config.supported_types
            
            print_status(f"Plugin {plugin_name} supports: {supported_types}")
            
            # Install plugin-specific packages
            required_packages = config.get_required_packages()
            if required_packages:
                self.install_packages(required_packages)
            
            # No setup data needed for plugin_test endpoints
            # The test data is provided directly in the API request
            
            # Test each supported plugin type
            all_passed = True
            for plugin_type in supported_types:
                # For scheduled and onwrite plugins, use plugin_test endpoints
                if plugin_type in ["scheduled", "onwrite"]:
                    if not self._test_plugin_api_endpoints(config, plugin_path, plugin_type):
                        all_passed = False
                # For HTTP plugins, create triggers since no plugin_test endpoint exists
                elif plugin_type == "http":
                    if not self._test_plugin_api_mode(config, plugin_path, plugin_type):
                        all_passed = False
                else:
                    print_warning(f"Unknown plugin type: {plugin_type}")
                    all_passed = False
            
            return all_passed
            
        except Exception as e:
            print_error(f"Error testing {plugin_name}: {e}")
            return False
    
    def _test_plugin_api_mode(self, config: PluginConfig, plugin_path: str, plugin_type: str) -> bool:
        """Test plugin using API mode with inline arguments"""
        plugin_name = Path(plugin_path).name
        trigger_name = f"test_{plugin_name}_{plugin_type}_api"
        
        print_status(f"Testing {plugin_name} ({plugin_type}) with API mode...")
        
        # Get appropriate arguments for this plugin type
        trigger_args = config.get_test_args(plugin_type)
        
        # Create and test the trigger
        if self.create_trigger(trigger_name, plugin_path, plugin_type, trigger_args):
            print_status(f"✓ {plugin_name} ({plugin_type}) API test completed successfully")
            return True
        else:
            print_error(f"✗ {plugin_name} ({plugin_type}) API test failed")
            return False
    
    def _test_plugin_api_endpoints(self, config: PluginConfig, plugin_path: str, plugin_type: str) -> bool:
        """Test plugin using InfluxDB 3 API test endpoints"""
        plugin_name = Path(plugin_path).name
        
        print_status(f"Testing {plugin_name} ({plugin_type}) with API test endpoints...")
        
        # Map plugin types to test endpoints
        test_endpoints = {
            "scheduled": "/plugin_test/schedule",
            "onwrite": "/plugin_test/wal",
            "http": None  # HTTP plugins need a trigger to test
        }
        
        endpoint = test_endpoints.get(plugin_type)
        if endpoint is None and plugin_type == "http":
            # HTTP plugins need a trigger to be tested - test via trigger creation and HTTP call
            return self._test_http_plugin_via_trigger(config, plugin_path, plugin_type)
        
        if endpoint is None:
            print_warning(f"Unknown plugin type: {plugin_type}")
            return False
        
        try:
            # Get test arguments
            test_args = config.get_test_args(plugin_type)
            
            # Construct plugin filename path
            plugin_filename = f"/host/{plugin_path}/{plugin_name}.py"
            
            # Prepare test request data
            test_data = {
                "database": self.database_name,
                "filename": plugin_filename,
                "arguments": test_args
            }
            
            # Add input_lp parameter for onwrite tests
            if plugin_type == "onwrite":
                test_data["input_lp"] = config.get_test_data()
            
            # Test with inline arguments
            print_status(f"Testing with inline arguments: {test_args}")
            success, response = self.make_api_request("POST", endpoint, test_data)
            
            if success:
                print_status(f"✓ {plugin_name} ({plugin_type}) inline args test completed successfully")
                inline_success = True
            else:
                print_error(f"✗ {plugin_name} ({plugin_type}) inline args test failed")
                print_error(f"Response: {response}")
                inline_success = False
            
            # Test with TOML config if supported
            toml_success = True
            if config.has_toml_support():
                toml_file = config.create_test_toml_config(plugin_type)
                if toml_file:
                    print_status(f"Testing {plugin_name} ({plugin_type}) with TOML config...")
                    
                    # For TOML testing, pass config_file_path as argument
                    toml_args = {"config_file_path": toml_file, "dry_run": "true"}
                    toml_test_data = {
                        "database": self.database_name,
                        "filename": plugin_filename,
                        "arguments": toml_args
                    }
                    
                    # Add input_lp parameter for onwrite tests
                    if plugin_type == "onwrite":
                        toml_test_data["input_lp"] = config.get_test_data()
                    
                    success, response = self.make_api_request("POST", endpoint, toml_test_data)
                    
                    if success:
                        print_status(f"✓ {plugin_name} ({plugin_type}) TOML config test completed successfully")
                    else:
                        print_error(f"✗ {plugin_name} ({plugin_type}) TOML config test failed")
                        print_error(f"Response: {response}")
                        toml_success = False
            
            return inline_success and toml_success
            
        except Exception as e:
            print_error(f"Error in test API mode: {e}")
            return False
    
    def _test_http_plugin_via_trigger(self, config: PluginConfig, plugin_path: str, plugin_type: str) -> bool:
        """Test HTTP plugin by creating a trigger and making HTTP requests"""
        plugin_name = Path(plugin_path).name
        trigger_name = f"test_{plugin_name}_{plugin_type}_http"
        
        print_status(f"Testing {plugin_name} ({plugin_type}) via HTTP trigger...")
        
        try:
            # Get test arguments
            trigger_args = config.get_test_args(plugin_type)
            
            # Create trigger for HTTP plugin
            if not self.create_trigger(trigger_name, plugin_path, plugin_type, trigger_args):
                print_error(f"Failed to create HTTP trigger for {plugin_name}")
                return False
            
            # Give trigger a moment to start
            time.sleep(2)
            
            # Test the HTTP endpoint (assumed to be at /test based on trigger spec)
            test_endpoint = "/test"  # This matches the "request:test" trigger spec
            test_data = {"test": "data"}
            
            print_status(f"Making test HTTP request to {test_endpoint}")
            success, response = self.make_api_request("POST", test_endpoint, test_data)
            
            if success:
                print_status(f"✓ {plugin_name} ({plugin_type}) HTTP test completed successfully")
                return True
            else:
                print_error(f"✗ {plugin_name} ({plugin_type}) HTTP test failed")
                print_error(f"Response: {response}")
                return False
                
        except Exception as e:
            print_error(f"Error testing HTTP plugin {plugin_name}: {e}")
            return False
    
    def test_organization_plugins(self, organization: str) -> Tuple[int, int]:
        """Test all plugins in an organization"""
        print_status(f"Testing all plugins in {organization} organization...")
        
        org_path = Path(organization)
        if not org_path.exists() or not org_path.is_dir():
            print_error(f"Organization directory '{organization}' not found")
            return 0, 0
        
        tested_count = 0
        passed_count = 0
        
        # Find all plugin directories in the organization
        for plugin_dir in org_path.iterdir():
            if plugin_dir.is_dir():
                plugin_name = plugin_dir.name
                plugin_file = plugin_dir / f"{plugin_name}.py"
                
                # Skip if no Python file with matching name exists
                if plugin_file.exists():
                    tested_count += 1
                    if self.test_plugin(str(plugin_dir)):
                        passed_count += 1
        
        if tested_count == 0:
            print_warning(f"No plugins found in {organization} organization")
        else:
            print_status(f"Organization {organization}: {passed_count}/{tested_count} plugins passed")
        
        return tested_count, passed_count
    
    def run_tests(self, target: str) -> bool:
        """Run tests on the specified target"""
        try:
            # Start InfluxDB container (or wait for it if already running)
            self.start_influxdb_container()
            
            # Clean up any existing test environment first
            print_status("Cleaning up any existing test environment...")
            self.cleanup_test_environment()
            
            # Create test database
            self.create_database(self.database_name)
            
            # Install common packages that plugins might need
            common_packages = ["pint", "requests", "pandas", "numpy"]
            self.install_packages(common_packages)
            
            # Determine if target is a specific plugin or organization
            if '/' in target:
                # Specific plugin (organization/plugin_name)
                success = self.test_plugin(target)
                return success
            else:
                # Organization directory
                tested_count, passed_count = self.test_organization_plugins(target)
                return tested_count > 0 and passed_count == tested_count
                
        except Exception as e:
            print_error(f"Test execution failed: {e}")
            return False
        finally:
            # Complete cleanup at the end
            try:
                print_status("Performing final cleanup...")
                self.cleanup_test_environment()
            except Exception as e:
                print_warning(f"Error during final cleanup: {e}")

def list_available_plugins():
    """List available organizations and plugins"""
    print("Available organizations and plugins:\n")
    
    current_dir = Path(".")
    for org_dir in current_dir.iterdir():
        if org_dir.is_dir() and not org_dir.name.startswith('.'):
            org_name = org_dir.name
            print(f"  {org_name}/")
            
            for plugin_dir in org_dir.iterdir():
                if plugin_dir.is_dir():
                    plugin_name = plugin_dir.name
                    plugin_file = plugin_dir / f"{plugin_name}.py"
                    if plugin_file.exists():
                        print(f"    └── {plugin_name}")
            print()

def show_usage():
    """Display usage information"""
    print("Usage: python test/test_plugins.py [organization[/plugin_name]] [options]")
    print("")
    print("Test plugins by organization or specific plugin:")
    print("  python test/test_plugins.py <organization> --core                - Test all plugins in organization with Core")
    print("  python test/test_plugins.py <organization>/<plugin_name>         - Test specific plugin (defaults to Core)")
    print("  python test/test_plugins.py <organization> --enterprise          - Test all plugins in organization with Enterprise")
    print("  python test/test_plugins.py --list                               - List all available organizations and plugins")
    print("")
    print("Options:")
    print("  --core            Start influxdb3 (default)")
    print("  --enterprise      Start InfluxDB 3 Enterprise")
    print("  --list            List available plugins")
    print("  --skip-container  Skip Docker container management (for running inside containers)")
    print("  -h, --help        Show this help message")
    print("")
    print("Features:")
    print("  - Plugin configurations are automatically derived from docstring metadata")
    print("  - TOML configuration files are auto-generated and tested when supported")
    print("  - Required Python packages are automatically installed per plugin")
    print("  - Each plugin type (scheduled, onwrite, http) is tested if supported")
    print(f"  - Default host URL: {DEFAULT_HOST_URL}")
    print(f"  - Default database: {DEFAULT_DATABASE_NAME}")
    print("")
    print("Environment variables:")
    print("  INFLUXDB3_HOST_PORT     - Host port for InfluxDB (default: 8181)")
    print("  INFLUXDB3_HOST_URL      - Full host URL for InfluxDB")
    print("  INFLUXDB3_DATABASE_NAME - Database name for testing")
    print("  SKIP_CONTAINER_MANAGEMENT - Skip Docker operations when running in containers")
    print("")
    print("Examples:")
    print("  python test/test_plugins.py influxdata --core")
    print("  python test/test_plugins.py influxdata/basic_transformation --enterprise")
    print("  python test/test_plugins.py --list")

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='InfluxDB 3 plugin testing framework')
    parser.add_argument('target', nargs='?', help='Organization or plugin path to test')
    parser.add_argument('--core', action='store_const', const='core', dest='service_type',
                       help='Use influxdb3 (default)')
    parser.add_argument('--enterprise', action='store_const', const='enterprise', dest='service_type',
                       help='Use InfluxDB 3 Enterprise')
    parser.add_argument('--list', action='store_true', help='List available plugins')
    parser.add_argument('--database', default=DEFAULT_DATABASE_NAME, help='Database name for testing')
    parser.add_argument('--host', default=DEFAULT_HOST_URL, help='InfluxDB host URL')
    parser.add_argument('--skip-container', action='store_true', help='Skip Docker container management (for running inside containers)')
    
    # Set default service type
    parser.set_defaults(service_type='core')
    
    args = parser.parse_args()
    
    # Handle list command
    if args.list:
        list_available_plugins()
        return 0
    
    # Handle help
    if not args.target:
        show_usage()
        return 1
    
    # Default target
    target = args.target or "influxdata"
    
    print_status(f"Testing target: {target} with InfluxDB 3 {args.service_type}")
    
    # Initialize test runner
    runner = PluginTestRunner(
        database_name=args.database,
        host_url=args.host,
        service_type=args.service_type,
        skip_container=args.skip_container
    )
    
    # Run tests
    success = runner.run_tests(target)
    
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())