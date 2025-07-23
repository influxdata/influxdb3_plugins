"""
Test script for InfluxDB 3 plugins using HTTP API
Supports testing individual plugins or entire organizations

This module provides both a command-line interface and reusable PluginTester class
for testing InfluxDB 3 plugins with TOML configuration support.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import requests
import tomllib

# Configuration defaults (can be overridden with environment variables)
DEFAULT_DATABASE_NAME = os.getenv("INFLUXDB3_DATABASE_NAME", "testdb")
DEFAULT_HOST_URL = os.getenv("INFLUXDB3_HOST_URL", f"http://localhost:{os.getenv('INFLUXDB3_HOST_PORT', '8181')}")

# Color output for better readability
class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    NC = '\033[0m'  # No Color

def print_status(message: str) -> None:
    """Print status message in green"""
    print(f"{Colors.GREEN}[INFO]{Colors.NC} {message}")

def print_warning(message: str) -> None:
    """Print warning message in yellow"""
    print(f"{Colors.YELLOW}[WARN]{Colors.NC} {message}")

def print_error(message: str) -> None:
    """Print error message in red"""
    print(f"{Colors.RED}[ERROR]{Colors.NC} {message}")

def parse_plugin_metadata(plugin_path: str) -> Dict:
    """Parse plugin metadata from the JSON schema in the docstring"""
    plugin_file = Path(plugin_path)
    
    if not plugin_file.exists():
        raise FileNotFoundError(f"Plugin file not found: {plugin_path}")
    
    with open(plugin_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Look for JSON schema in docstring
    json_pattern = r'"""[\s\n]*(\{.*?\})[\s\n]*"""'
    match = re.search(json_pattern, content, re.DOTALL)
    
    if not match:
        json_pattern = r"'''[\s\n]*(\{.*?\})[\s\n]*'''"
        match = re.search(json_pattern, content, re.DOTALL)
    
    if not match:
        return {"plugin_type": ["onwrite"]}  # Default fallback
    
    try:
        json_str = match.group(1)
        metadata = json.loads(json_str)
        return metadata
    except json.JSONDecodeError:
        return {"plugin_type": ["onwrite"]}  # Default fallback

def get_trigger_spec_for_type(plugin_type: str) -> str:
    """Get appropriate trigger specification for plugin type"""
    trigger_specs = {
        "scheduled": "every:5m",
        "onwrite": "all_tables", 
        "http": "request:test"
    }
    return trigger_specs.get(plugin_type, "all_tables")

class PluginTestConfig:
    """Plugin test configuration manager that integrates with existing docstring parsing"""
    
    def __init__(self, plugin_path: str, plugin_file: str):
        self.plugin_path = Path(plugin_path)
        self.plugin_file = plugin_file
        self.full_plugin_path = self.plugin_path / plugin_file
        
        # Parse plugin metadata from docstring
        try:
            self.metadata = parse_plugin_metadata(str(self.full_plugin_path))
        except Exception as e:
            print_warning(f"Could not parse plugin metadata from {self.full_plugin_path}: {e}")
            self.metadata = {"plugin_type": ["onwrite"]}  # Default fallback
    
    @property
    def supported_types(self) -> List[str]:
        """Get supported plugin types from metadata"""
        return self.metadata.get('plugin_type', ['onwrite'])
    
    def get_test_args(self, plugin_type: str, use_examples: bool = True) -> Dict:
        """Get test arguments for a specific plugin type from docstring metadata"""
        # Base arguments for different plugin types
        base_args = {
            "scheduled": {"dry_run": "true"},
            "onwrite": {"dry_run": "true"},
            "http": {"dry_run": "true"}
        }
        
        args = base_args.get(plugin_type, {"dry_run": "true"}).copy()
        
        if use_examples:
            # Get arguments from plugin metadata
            args_config_key = f"{plugin_type}_args_config"
            args_config = self.metadata.get(args_config_key, [])
            
            # Build arguments from required fields with examples
            for arg_def in args_config:
                arg_name = arg_def.get("name")
                if arg_name and arg_name != "config_file_path":  # Skip config_file_path for inline args
                    if arg_def.get("required", False):
                        example = arg_def.get("example", "")
                        if example:
                            args[arg_name] = example
                    elif arg_name in ["dry_run"] and arg_def.get("example"):
                        # Include important optional args like dry_run
                        args[arg_name] = arg_def.get("example")
        
        return args
    
    def get_required_packages(self) -> List[str]:
        """Get required packages for this plugin (from external config or defaults)"""
        # Could be extended to read from plugin metadata or config files
        common_packages = ["pint", "requests", "pandas", "numpy"]
        
        # Plugin-specific packages based on name patterns
        plugin_name_lower = self.plugin_path.name.lower()
        if "prophet" in plugin_name_lower:
            common_packages.extend(["prophet", "scikit-learn"])
        elif "adtk" in plugin_name_lower or "anomaly" in plugin_name_lower:
            common_packages.extend(["adtk", "scikit-learn"])
        elif "notification" in plugin_name_lower or "notifier" in plugin_name_lower:
            common_packages.extend(["slack-sdk"])
        
        return common_packages
    
    def get_test_data(self) -> str:
        """Get appropriate test data for this plugin"""
        # Default test data
        return "temperature,sensor=room1 value=25.5"
    
    def has_toml_support(self) -> bool:
        """Check if plugin supports TOML configuration"""
        for plugin_type in self.supported_types:
            args_config_key = f"{plugin_type}_args_config"
            args_config = self.metadata.get(args_config_key, [])
            for arg_def in args_config:
                if arg_def.get("name") == "config_file_path":
                    return True
        return False
    
    def create_test_toml_config(self, plugin_type: str) -> Optional[str]:
        """Create a test TOML configuration file for this plugin"""
        if not self.has_toml_support():
            return None
        
        args = self.get_test_args(plugin_type, use_examples=True)
        # Remove dry_run from TOML config to test parameter override
        args.pop("dry_run", None)
        
        if not args:
            return None
        
        # Create TOML content
        toml_content = "# Auto-generated test configuration\n\n"
        for key, value in args.items():
            if isinstance(value, str):
                toml_content += f'{key} = "{value}"\n'
            elif isinstance(value, bool):
                toml_content += f'{key} = {str(value).lower()}\n'
            elif isinstance(value, (int, float)):
                toml_content += f'{key} = {value}\n'
            else:
                toml_content += f'{key} = "{str(value)}"\n'
        
        # Write to test config file
        config_file = self.plugin_path / "test_config.toml"
        try:
            with open(config_file, 'w') as f:
                f.write(toml_content)
            return "test_config.toml"
        except Exception as e:
            print_warning(f"Failed to create test TOML config: {e}")
            return None


def get_plugin_args_for_type(plugin_name: str, plugin_type: str) -> Dict:
    """Get appropriate arguments for plugins using metadata-driven approach"""
    # Try to create PluginTestConfig for this plugin
    plugin_path = Path(plugin_name)
    if plugin_path.exists() and plugin_path.is_dir():
        plugin_file = f"{plugin_path.name}.py"
        config = PluginTestConfig(str(plugin_path), plugin_file)
        return config.get_test_args(plugin_type)
    
    # Fallback to basic args if plugin path doesn't exist or metadata parsing fails
    base_args = {
        "scheduled": {"window": "1h", "dry_run": "true"},
        "onwrite": {"dry_run": "true"},
        "http": {"dry_run": "true"}
    }
    
    return base_args.get(plugin_type, {"dry_run": "true"})

class BasePluginTester:
    """
    Base class providing common plugin testing operations.
    
    Contains shared functionality for:
    - API requests to InfluxDB 3
    - Docker container management
    - Database operations
    - Package installation
    - Basic trigger management
    """
    
    def __init__(self, database_name: str = DEFAULT_DATABASE_NAME,
                 host_url: str = DEFAULT_HOST_URL,
                 skip_container: bool = False):
        self.database_name = database_name
        self.host_url = host_url
        self.skip_container = skip_container or os.getenv('SKIP_CONTAINER_MANAGEMENT', '').lower() in ('true', '1')
        self.created_triggers = []
        
        # Set appropriate plugin directory for container context
        plugin_dir = "/plugins" if self.skip_container else "/host"
        os.environ["PLUGIN_DIR"] = plugin_dir
        
        if self.skip_container:
            print_status("Container management disabled (running inside container)")
    
    def run_command(self, command: List[str], capture_output: bool = False) -> subprocess.CompletedProcess:
        """Run a command and return the result"""
        try:
            if capture_output:
                result = subprocess.run(command, capture_output=True, text=True, check=True)
            else:
                result = subprocess.run(command, check=True)
            return result
        except subprocess.CalledProcessError as e:
            if capture_output:
                print_error(f"Command failed: {' '.join(command)}")
                if e.stderr:
                    print_error(f"Error output: {e.stderr}")
            raise
    
    def make_api_request(self, method: str, endpoint: str, data: Optional[Dict] = None, 
                        params: Optional[Dict] = None) -> Tuple[bool, Dict]:
        """Make an API request to InfluxDB 3"""
        url = f"{self.host_url}/api/v3{endpoint}"
        
        try:
            if method.upper() == "POST":
                response = requests.post(url, json=data, timeout=30)
            elif method.upper() == "DELETE":
                response = requests.delete(url, params=params, timeout=30)
            elif method.upper() == "GET":
                response = requests.get(url, params=params, timeout=30)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            try:
                response_data = response.json()
            except:
                response_data = {"message": response.text}
            
            return response.status_code < 400, response_data
            
        except requests.exceptions.RequestException as e:
            return False, {"error": str(e)}
    
    def start_influxdb(self) -> None:
        """Start influxdb3 container (skipped when running inside container)"""
        if self.skip_container:
            print_status("Skipping container startup (already running inside container)")
            # Just wait for InfluxDB to be ready
            self.wait_for_influxdb()
            return
            
        # Get container name from service type if available
        container_name = getattr(self, 'container_name', 'influxdb3-core')
        service_type = getattr(self, 'service_type', 'core')
        
        print_status(f"Starting InfluxDB 3 {service_type} container...")
        
        # Stop existing container
        try:
            self.run_command(["docker", "compose", "-f", "compose.yml", "down", container_name])
        except subprocess.CalledProcessError:
            pass  # Container might not be running
        
        # Pull latest image
        self.run_command(["docker", "compose", "-f", "compose.yml", "pull", container_name])
        
        # Start service
        self.run_command(["docker", "compose", "-f", "compose.yml", "up", "-d", container_name])
        
        # Wait for readiness
        self.wait_for_influxdb()
    
    def wait_for_influxdb(self) -> None:
        """Wait for InfluxDB to be ready"""
        container_name = getattr(self, 'container_name', 'influxdb3-core')
        service_type = getattr(self, 'service_type', 'core')
        
        print_status(f"Waiting for InfluxDB 3 {service_type} to be ready...")
        max_attempts = 30
        attempt = 0
        
        while attempt < max_attempts:
            try:
                response = requests.get(f"{self.host_url}/health", timeout=5)
                if response.status_code == 200:
                    print_status(f"InfluxDB 3 {service_type} is ready!")
                    return
            except requests.exceptions.RequestException:
                pass
            
            attempt += 1
            print_status(f"Attempt {attempt}/{max_attempts} - waiting 2 seconds...")
            time.sleep(2)
        
        print_error(f"InfluxDB 3 {service_type} failed to start within {max_attempts * 2} seconds")
        
        # Show container logs for debugging (only if not skipping container management)
        if not self.skip_container:
            try:
                result = self.run_command(["docker", "compose", "-f", "compose.yml", "logs", container_name], 
                                        capture_output=True)
                print_error("Container logs:")
                print(result.stdout)
            except subprocess.CalledProcessError:
                pass
        
        raise RuntimeError(f"InfluxDB 3 {service_type} failed to start")
    
    def delete_database(self, db_name: str) -> bool:
        """Delete a database using the API"""
        print_status(f"Deleting database: {db_name}")
        
        success, response = self.make_api_request(
            "DELETE",
            "/configure/database",
            params={"db": db_name}
        )
        
        if success:
            print_status(f"Database {db_name} deleted successfully")
            return True
        else:
            # Don't warn if database doesn't exist
            error_msg = response.get('message', '')
            if 'not found' not in error_msg.lower() and 'does not exist' not in error_msg.lower():
                print_warning(f"Database deletion might have failed: {response}")
            else:
                print_status(f"Database {db_name} not found (already deleted or never existed)")
            return False
    
    def create_database(self, db_name: str) -> bool:
        """Create a database using the API"""
        print_status(f"Creating database: {db_name}")
        
        success, response = self.make_api_request(
            "POST", 
            "/configure/database",
            {"db": db_name}
        )
        
        if success:
            print_status(f"Database {db_name} created successfully")
            return True
        else:
            print_warning(f"Database creation might have failed: {response}")
            return True  # Continue testing even if database creation fails
    
    def install_packages(self, packages: List[str]) -> bool:
        """Install Python packages using the API"""
        if not packages:
            print_status("No packages to install")
            return True
        
        print_status(f"Installing packages: {', '.join(packages)}")
        
        success, response = self.make_api_request(
            "POST",
            "/configure/plugin_environment/install_packages",
            {"packages": packages}
        )
        
        if success:
            print_status("Packages installed successfully")
            return True
        else:
            print_warning(f"Package installation might have failed: {response}")
            return True  # Continue testing even if package installation fails
    
    def install_package(self, package_name: str) -> bool:
        """Install a single Python package using the API"""
        return self.install_packages([package_name])
    
    def query_internal_db(self, query: str) -> List[Dict]:
        """Query the _internal database using SQL"""
        try:
            response = requests.post(
                f"{self.host_url}/api/v3/query_sql",
                json={
                    "db": "_internal",
                    "q": query,
                    "format": "json"},
                timeout=30
            )
            
            if response.status_code < 400:
                return response.json()
            else:
                print_warning(f"Internal query failed: {response.text}")
                return []
                
        except requests.exceptions.RequestException as e:
            print_warning(f"Failed to query _internal database: {e}")
            return []
    
    def disable_trigger(self, trigger_name: str) -> bool:
        """Disable a trigger by querying its configuration and updating it"""
        print_status(f"Disabling trigger: {trigger_name}")
        
        # Query the system.triggers table to get trigger configuration
        query = f"SELECT * FROM system.processing_engine_triggers WHERE trigger_name = '{trigger_name}'"
        results = self.query_internal_db(query)
        
        if not results:
            print_status(f"Trigger {trigger_name} not found (already deleted or doesn't exist)")
            return True  # Consider this a success since the trigger is gone
        
        # Get the first (should be only) result
        trigger_info = results[0]
        
        # Create the update payload with all required fields
        trigger_data = {
            "db": self.database_name,
            "trigger_name": trigger_name,
            "plugin_filename": trigger_info.get('plugin_filename', ''),
            "trigger_specification": trigger_info.get('trigger_specification', ''),
            "trigger_arguments": trigger_info.get('trigger_arguments', {}),
            "trigger_settings": trigger_info.get('trigger_settings', {}),
            "disabled": True  # This is what we're updating
        }
        
        success, response = self.make_api_request(
            "POST",
            "/configure/processing_engine_trigger",
            trigger_data
        )
        
        if success:
            print_status(f"Trigger {trigger_name} disabled successfully")
            return True
        else:
            print_warning(f"Trigger disable might have failed: {response}")
            return False
    
    def delete_trigger(self, trigger_name: str, force: bool = True) -> bool:
        """Delete a trigger using the API"""
        print_status(f"Deleting trigger: {trigger_name}")
        
        # First try to disable the trigger (but don't fail if this doesn't work)
        disable_success = self.disable_trigger(trigger_name)
        
        # Add a short delay to let the trigger stop if it was successfully disabled
        if disable_success:
            time.sleep(1)
        
        params = {"db": self.database_name, "trigger_name": trigger_name}
        if force:
            params["force"] = "true"
        
        success, response = self.make_api_request(
            "DELETE",
            "/configure/processing_engine_trigger",
            params=params
        )
        
        if success:
            print_status(f"Trigger {trigger_name} deleted successfully")
            return True
        else:
            # Check if the error message indicates the trigger is running
            error_msg = response.get('message', '')
            if 'Cannot delete running plugin' in error_msg and 'Disable it first' in error_msg:
                print_status(f"Trigger {trigger_name} was running, attempting forced deletion...")
                # Try again with explicit force and a longer delay
                time.sleep(2)
                success, response = self.make_api_request(
                    "DELETE",
                    "/configure/processing_engine_trigger",
                    params=params
                )
                if success:
                    print_status(f"Trigger {trigger_name} force deleted successfully")
                    return True
            
            # Only warn if it's not a "trigger not found" error (which is fine)
            if 'not found' not in error_msg.lower():
                print_warning(f"Trigger deletion might have failed: {response}")
            else:
                print_status(f"Trigger {trigger_name} not found (already deleted)")
            return False
    
    def write_test_data(self, test_data: str = "temperature,sensor=room1 value=25.5") -> bool:
        """Write test data using the API"""
        print_status("Writing test data...")
        
        try:
            response = requests.post(
                f"{self.host_url}/api/v3/write_lp",
                params={"db": self.database_name},
                data=test_data,
                headers={"Content-Type": "text/plain"},
                timeout=30
            )
            
            if response.status_code < 400:
                print_status("Test data written successfully")
                return True
            else:
                print_warning(f"Test data write might have failed: {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            print_error(f"Failed to write test data: {e}")
            return False
    
    def test_plugin_dir_env(self) -> bool:
        """Test: Check PLUGIN_DIR environment variable via Docker"""
        if self.skip_container:
            print_status("Test: Skipping PLUGIN_DIR check (running inside container)")
            return True
            
        print_status("Test: Checking PLUGIN_DIR environment variable")
        
        try:
            result = self.run_command([
                "docker", "compose", "-f", "compose.yml", "exec", "-T", "influxdb3-core",
                "env"
            ], capture_output=True)
            
            if "PLUGIN_DIR" in result.stdout:
                print_status("PLUGIN_DIR environment variable is set")
                plugin_dir_lines = [line for line in result.stdout.split('\n') if 'PLUGIN_DIR' in line]
                for line in plugin_dir_lines:
                    print(line)
                return True
            else:
                print_warning("PLUGIN_DIR environment variable not found")
                return False
        except subprocess.CalledProcessError as e:
            print_error("Failed to check environment variables")
            if e.stderr:
                print(e.stderr)
            return False
    
    def cleanup(self) -> None:
        """Cleanup created triggers"""
        if not self.created_triggers:
            return
            
        print_status("Cleaning up created triggers...")
        
        # Try to clean up triggers with retries
        remaining_triggers = self.created_triggers.copy()
        max_attempts = 3
        
        for attempt in range(max_attempts):
            if not remaining_triggers:
                break
                
            failed_triggers = []
            for trigger_name in remaining_triggers:
                if not self.delete_trigger(trigger_name, force=True):
                    failed_triggers.append(trigger_name)
            
            remaining_triggers = failed_triggers
            if remaining_triggers and attempt < max_attempts - 1:
                print_status(f"Retrying cleanup for {len(remaining_triggers)} triggers in 2 seconds...")
                time.sleep(2)
        
        if remaining_triggers:
            print_warning(f"Failed to cleanup {len(remaining_triggers)} triggers: {remaining_triggers}")
        else:
            print_status("All triggers cleaned up successfully")
        
        self.created_triggers.clear()
    
    def list_all_triggers(self) -> List[str]:
        """List all triggers in the database using system tables"""
        query = f"SELECT trigger_name FROM system.processing_engine_triggers"
        results = self.query_internal_db(query)
        
        if results:
            return [row.get('trigger_name', '') for row in results]
        else:
            return []
    
    def cleanup_all_test_triggers(self) -> None:
        """Cleanup all test triggers from the database"""
        print_status("Cleaning up all test triggers...")
        
        all_triggers = self.list_all_triggers()
        test_triggers = [name for name in all_triggers if name.startswith('test_')]
        
        if test_triggers:
            print_status(f"Found {len(test_triggers)} test triggers to cleanup: {test_triggers}")
            for trigger_name in test_triggers:
                self.delete_trigger(trigger_name, force=True)
        else:
            print_status("No test triggers found to cleanup")
    
    def cleanup_test_environment(self) -> None:
        """Complete cleanup of test environment (triggers and database)"""
        print_status("Performing complete test environment cleanup...")
        
        try:
            # Clean up all test triggers
            self.cleanup_all_test_triggers()
            
            # Clean up created triggers (if any remain)
            self.cleanup()
            
            # Small delay to ensure triggers are fully stopped
            time.sleep(1)
            
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
                 test_mode: str = "both",  # "api", "cli", or "both" 
                 skip_container: bool = False):
        super().__init__(database_name, host_url, skip_container)
        self.service_type = service_type
        self.container_name = f"influxdb3-{service_type}"
        self.test_mode = test_mode
        
        print_status(f"Set PLUGIN_DIR={os.environ.get('PLUGIN_DIR')} for TOML configuration support")
        print_status(f"Test mode: {test_mode}")
    
    def start_influxdb_container(self) -> None:
        """Start InfluxDB 3 container with service-specific logic"""
        self.start_influxdb()
    def create_trigger(self, trigger_name: str, plugin_path: str, plugin_type: str, 
                      trigger_args: Dict) -> bool:
        """Create a trigger using the API"""
        print_status(f"Creating {plugin_type} trigger: {trigger_name}")
        
        trigger_spec = get_trigger_spec_for_type(plugin_type)
        
        # Use /host as that's where plugins are mounted in the InfluxDB container
        plugin_path_prefix = "/host"
        
        # Construct full path to Python file
        plugin_name = Path(plugin_path).name
        plugin_filename = f"{plugin_path_prefix}/{plugin_path}/{plugin_name}.py"
        
        trigger_data = {
            "db": self.database_name,
            "plugin_filename": plugin_filename,
            "trigger_name": trigger_name,
            "trigger_specification": trigger_spec,
            "trigger_arguments": trigger_args,
            "trigger_settings": {
                "run_async": False,
                "error_behavior": "log"
            },
            "disabled": False
        }
        
        success, response = self.make_api_request(
            "POST",
            "/configure/processing_engine_trigger",
            trigger_data
        )
        
        if success:
            print_status(f"Trigger {trigger_name} created successfully")
            self.created_triggers.append(trigger_name)
            return True
        else:
            print_error(f"Trigger creation failed: {response}")
            return False
    
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
            config = PluginTestConfig(plugin_path, f"{plugin_name}.py")
            supported_types = config.supported_types
            
            print_status(f"Plugin {plugin_name} supports: {supported_types}")
            
            # Install plugin-specific packages
            required_packages = config.get_required_packages()
            if required_packages:
                self.install_packages(required_packages)
            
            # Write plugin-specific test data
            test_data = config.get_test_data()
            self.write_test_data(test_data)
            
            # Test each supported plugin type
            all_passed = True
            for plugin_type in supported_types:
                if self.test_mode in ["api", "both"]:
                    if not self._test_plugin_api_mode(config, plugin_path, plugin_type):
                        all_passed = False
                
                if self.test_mode in ["cli", "both"]:
                    if not self._test_plugin_cli_mode(config, plugin_path, plugin_type):
                        all_passed = False
            
            return all_passed
            
        except Exception as e:
            print_error(f"Error testing {plugin_name}: {e}")
            return False
    
    def _test_plugin_api_mode(self, config: PluginTestConfig, plugin_path: str, plugin_type: str) -> bool:
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
    
    def _test_plugin_cli_mode(self, config: PluginTestConfig, plugin_path: str, plugin_type: str) -> bool:
        """Test plugin using API test endpoints"""
        plugin_name = Path(plugin_path).name
        
        print_status(f"Testing {plugin_name} ({plugin_type}) with test API...")
        
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
    
    def _test_http_plugin_via_trigger(self, config: PluginTestConfig, plugin_path: str, plugin_type: str) -> bool:
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
    print("  python test/test_plugins.py influxdata --core                    - Test all influxdata plugins with Core")
    print("  python test/test_plugins.py influxdata/basic_transformation      - Test specific plugin (defaults to Core)")
    print("  python test/test_plugins.py examples --enterprise                - Test all examples plugins with Enterprise")
    print("  python test/test_plugins.py --list                               - List all available organizations and plugins")
    print("")
    print("Options:")
    print("  --core            Start influxdb3 (default)")
    print("  --enterprise      Start InfluxDB 3 Enterprise")
    print("  --test-mode MODE  Testing mode: 'api', 'cli', or 'both' (default: 'both')")
    print("  --list            List available plugins")
    print("  --skip-container  Skip Docker container management (for running inside containers)")
    print("  -h, --help        Show this help message")
    print("")
    print("Test Modes:")
    print("  api               Test using HTTP API with trigger creation")
    print("  cli               Test using 'influxdb3 test' command with inline args and TOML configs")
    print("  both              Run both API and CLI tests (default)")
    print("")
    print("Notes:")
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
    print("")
    print("Examples:")
    print("  python test_plugins.py influxdata --core --test-mode both")
    print("  python test_plugins.py influxdata/downsampler --enterprise --test-mode api")
    print("  python test_plugins.py examples --test-mode cli")

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Test InfluxDB 3 plugins using API and CLI modes')
    parser.add_argument('target', nargs='?', help='Organization or plugin path to test')
    parser.add_argument('--core', action='store_const', const='core', dest='service_type',
                       help='Use influxdb3 (default)')
    parser.add_argument('--enterprise', action='store_const', const='enterprise', dest='service_type',
                       help='Use InfluxDB 3 Enterprise')
    parser.add_argument('--test-mode', choices=['api', 'cli', 'both'], default='both', 
                       help='Testing mode: api (HTTP API), cli (influxdb3 test), or both (default: both)')
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
    
    print_status(f"Testing target: {target} with InfluxDB 3 {args.service_type} in {args.test_mode} mode")
    
    # Initialize test runner
    runner = PluginTestRunner(
        database_name=args.database,
        host_url=args.host,
        service_type=args.service_type,
        test_mode=args.test_mode,
        skip_container=args.skip_container
    )
    
    # Run tests
    success = runner.run_tests(target)
    
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())