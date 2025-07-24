#!/usr/bin/env python3
"""
Generic test script for InfluxDB 3 plugins with TOML configuration
This test verifies that InfluxDB 3 Core supports TOML file access with PLUGIN_DIR environment variable
"""

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Import shared modules
try:
    from .influxdb_api_client import print_status, print_warning, print_error
    from .plugin_config import PluginConfig, get_trigger_spec_for_type
    from .trigger_manager import TriggerManager
    from .test_plugins import BasePluginTester, DEFAULT_DATABASE_NAME, DEFAULT_HOST_URL
except ImportError:
    # When running as a script, use absolute imports
    from influxdb_api_client import print_status, print_warning, print_error
    from plugin_config import PluginConfig, get_trigger_spec_for_type
    from trigger_manager import TriggerManager
    from test_plugins import BasePluginTester, DEFAULT_DATABASE_NAME, DEFAULT_HOST_URL

class PluginTester(BasePluginTester):
    """
    TOML-specific plugin tester class.
    
    Extends BasePluginTester with TOML configuration testing capabilities:
    - Plugin metadata parsing from docstrings
    - TOML configuration file setup and testing
    - Multiple plugin types testing
    - Config file subdirectory testing
    """
    
    def __init__(self, plugin_path: str, plugin_file: str, 
                 database_name: str = DEFAULT_DATABASE_NAME,
                 host_url: str = DEFAULT_HOST_URL,
                 toml_config: Optional[str] = None,
                 packages: Optional[List[str]] = None,
                 test_data: Optional[str] = None,
                 inline_args: Optional[Dict] = None,
                 skip_container: bool = False):
        
        # Initialize base class
        super().__init__(database_name, host_url, skip_container)
        
        self.plugin_path = plugin_path
        self.plugin_file = plugin_file
        self.toml_config = toml_config
        
        # Initialize plugin configuration manager
        self.plugin_config = PluginConfig(plugin_path, plugin_file)
        
        # Use plugin-specific packages if not provided
        self.packages = packages or self.plugin_config.get_required_packages()
        
        # Use plugin-specific test data if not provided
        self.test_data = test_data or self.plugin_config.get_test_data()
        
        # Use plugin-derived inline args if not provided
        if inline_args is None:
            # Try to get args for the first supported type as fallback
            supported_types = self.plugin_config.supported_types
            if supported_types:
                self.inline_args = self.plugin_config.get_test_args(supported_types[0])
            else:
                self.inline_args = {"dry_run": "true"}
        else:
            self.inline_args = inline_args
        
        # Parse plugin metadata (for compatibility)
        self.metadata = self.plugin_config.metadata
        self.supported_types = self.plugin_config.supported_types
        
        print_status(f"Parsed plugin metadata: {self.metadata.get('plugin_type', 'unknown')}")
        print_status(f"Supported plugin types: {self.supported_types}")
    
    def create_trigger(self, trigger_name: str, plugin_type: str, config_file_path: Optional[str] = None) -> bool:
        """Create a trigger using the API with TOML-specific logic"""
        if config_file_path:
            trigger_args = {"config_file_path": config_file_path}
        else:
            # Use plugin configuration to get appropriate arguments
            trigger_args = self.plugin_config.get_test_args(plugin_type)
            
            # Fallback to inline args if no metadata-derived args
            if not trigger_args or trigger_args == {"dry_run": "true"}:
                trigger_args = self.inline_args
        
        return self.trigger_manager.create_trigger(
            trigger_name, self.plugin_path, plugin_type, trigger_args, self.plugin_file
        )
    
    def test_plugin_dependencies(self) -> bool:
        """Test: Install plugin dependencies"""
        print_status("Test: Installing plugin dependencies")
        print_status(f"Packages to install: {self.packages}")
        return self.api_client.install_packages(self.packages)
    
    def setup_test_files(self) -> None:
        """Setup test directories and files"""
        if not self.toml_config:
            print_status("No TOML config specified, skipping file setup")
            return
        
        print_status("Setting up test directories and files")
        
        # Create configs directory in the plugin directory
        config_dir = Path(f"{self.plugin_path}/configs")
        config_dir.mkdir(exist_ok=True)
        
        # Copy the TOML config file to the configs subdirectory
        source_file = Path(f"{self.plugin_path}/{self.toml_config}")
        dest_file = config_dir / self.toml_config
        
        if source_file.exists():
            shutil.copy(source_file, dest_file)
            print_status("Created configs directory and copied TOML file")
        else:
            print_warning(f"Source TOML file {source_file} not found")
    
    
    def run_tests(self) -> List[Tuple[str, bool]]:
        """Run all tests and return results"""
        print_status(f"Starting TOML configuration test for {self.plugin_path}")
        
        results = []
        
        try:
            # Start InfluxDB
            self.start_influxdb()
            
            # Clean up any existing test environment first
            print_status("Cleaning up any existing test environment...")
            self.cleanup_test_environment()
            
            # Create databases
            self.create_database(self.database_name)
            self.create_database("default")  # Common target database
            
            # Write test data
            self.write_test_data(self.test_data)
            
            # Setup test directories and files
            self.setup_test_files()
            
            # Run tests
            print_status("Running tests...")
            
            # Test plugin dependencies installation
            if self.test_plugin_dependencies():
                print_status("✓ Plugin dependencies installation works")
                results.append(("Plugin dependencies installation", True))
            else:
                print_error("✗ Plugin dependencies installation failed")
                results.append(("Plugin dependencies installation", False))
                return results
            
            # Test environment variable access
            plugin_dir_result = self.test_plugin_dir_env()
            results.append(("PLUGIN_DIR environment variable", plugin_dir_result))
            
            # Test each supported plugin type
            for plugin_type in self.supported_types:
                # Test inline arguments trigger (baseline)
                trigger_name = f"test_{plugin_type}_inline"
                if self.create_trigger(trigger_name, plugin_type, None):
                    print_status(f"✓ {plugin_type} inline arguments trigger works")
                    results.append((f"{plugin_type} inline arguments trigger", True))
                else:
                    print_error(f"✗ {plugin_type} inline arguments trigger failed")
                    results.append((f"{plugin_type} inline arguments trigger", False))
                
                # Test TOML config if available or auto-generate if plugin supports it
                toml_config_to_test = self.toml_config
                if not toml_config_to_test and self.plugin_config.has_toml_support():
                    print_status(f"Auto-generating TOML config for {plugin_type}...")
                    toml_config_to_test = self.plugin_config.create_test_toml_config(plugin_type)
                
                if toml_config_to_test:
                    # Test TOML config trigger creation
                    toml_trigger_name = f"test_{plugin_type}_toml"
                    if self.create_trigger(toml_trigger_name, plugin_type, toml_config_to_test):
                        print_status(f"✓ {plugin_type} TOML config trigger creation works")
                        results.append((f"{plugin_type} TOML config trigger creation", True))
                    else:
                        print_error(f"✗ {plugin_type} TOML config trigger creation failed")
                        results.append((f"{plugin_type} TOML config trigger creation", False))
                    
                    # Test config file in subdirectory
                    subdir_trigger_name = f"test_{plugin_type}_subdir"
                    if self.create_trigger(subdir_trigger_name, plugin_type, f"configs/{toml_config_to_test}"):
                        print_status(f"✓ {plugin_type} config file in subdirectory works")
                        results.append((f"{plugin_type} config file in subdirectory", True))
                    else:
                        print_error(f"✗ {plugin_type} config file in subdirectory failed")
                        results.append((f"{plugin_type} config file in subdirectory", False))
                elif self.plugin_config.has_toml_support():
                    print_warning(f"Plugin supports TOML but no config file provided or generated")
                    results.append((f"{plugin_type} TOML config support", False))
            
            # Cleanup
            self.cleanup()
            
            print_status("Test complete!")
            
            # Print results summary
            print_status("Test Results:")
            for test_name, result in results:
                status = "✓" if result else "✗"
                print_status(f"  {status} {test_name}")
            
            return results
            
        except Exception as e:
            print_error(f"Test failed with exception: {e}")
            self.cleanup_test_environment()
            raise
        finally:
            # Ensure complete cleanup at the end
            print_status("Performing final cleanup...")
            self.cleanup_test_environment()

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Test InfluxDB 3 plugin with TOML configuration')
    parser.add_argument('plugin_path', help='Path to plugin directory (e.g., influxdata/basic_transformation)')
    parser.add_argument('plugin_file', help='Plugin filename (e.g., basic_transformation.py)')
    parser.add_argument('--database', default=DEFAULT_DATABASE_NAME, help='Database name for testing')
    parser.add_argument('--host', default=DEFAULT_HOST_URL, help='InfluxDB host URL')
    parser.add_argument('--toml-config', help='TOML configuration filename (if not provided, will auto-generate if supported)')
    parser.add_argument('--packages', nargs='*', help='Python packages to install (if not provided, will use plugin-specific packages)')
    parser.add_argument('--test-data', help='Test data in line protocol format (if not provided, will use plugin-specific data)')
    parser.add_argument('--inline-args', help='Inline arguments as JSON string (if not provided, will derive from plugin metadata)')
    
    args = parser.parse_args()
    
    # Parse inline arguments if provided
    inline_args = None
    if args.inline_args:
        try:
            inline_args = json.loads(args.inline_args)
        except json.JSONDecodeError:
            print_error("Invalid JSON format for inline arguments")
            return 1
    
    print_status(f"Testing plugin: {args.plugin_path}/{args.plugin_file}")
    if args.toml_config:
        print_status(f"Using TOML config: {args.toml_config}")
    else:
        print_status("TOML config not specified - will auto-generate if plugin supports it")
    
    tester = PluginTester(
        plugin_path=args.plugin_path,
        plugin_file=args.plugin_file,
        database_name=args.database,
        host_url=args.host,
        toml_config=args.toml_config,
        packages=args.packages,
        test_data=args.test_data,
        inline_args=inline_args
    )
    
    # Show configuration summary
    print_status(f"Plugin supports types: {tester.supported_types}")
    print_status(f"Required packages: {tester.packages}")
    print_status(f"Test data: {tester.test_data}")
    print_status(f"TOML support: {tester.plugin_config.has_toml_support()}")
    
    try:
        results = tester.run_tests()
        
        # Print final summary
        passed_count = sum(1 for _, success in results if success)
        total_count = len(results)
        
        print_status(f"\nTest Summary: {passed_count}/{total_count} tests passed")
        
        # Check if all tests passed
        all_passed = all(result[1] for result in results)
        return 0 if all_passed else 1
        
    except Exception as e:
        print_error(f"Testing failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())