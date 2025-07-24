"""
Unified trigger management for InfluxDB 3 plugin testing

Handles trigger creation, deletion, and lifecycle management
with consistent error handling and retry logic.
"""

import time
from pathlib import Path
from typing import Dict, List, Optional

try:
    from .influxdb_api_client import InfluxDBApiClient, print_status, print_warning, print_error
    from .plugin_config import get_trigger_spec_for_type
except ImportError:
    # When running as a script, use absolute imports
    from influxdb_api_client import InfluxDBApiClient, print_status, print_warning, print_error
    from plugin_config import get_trigger_spec_for_type


class TriggerManager:
    """Centralized trigger management for plugin testing"""
    
    def __init__(self, api_client: InfluxDBApiClient, database_name: str, skip_container: bool = False):
        self.api_client = api_client
        self.database_name = database_name
        self.skip_container = skip_container
        self.created_triggers = []
    
    def create_trigger(self, trigger_name: str, plugin_path: str, plugin_type: str, 
                      trigger_args: Dict, plugin_file: Optional[str] = None) -> bool:
        """Create a trigger using the API"""
        print_status(f"Creating {plugin_type} trigger: {trigger_name}")
        
        trigger_spec = get_trigger_spec_for_type(plugin_type)
        
        # Use appropriate path prefix based on container context
        plugin_path_prefix = "/plugins" if self.skip_container else "/host"
        
        # Construct full path to Python file
        if plugin_file:
            plugin_filename = f"{plugin_path_prefix}/{plugin_path}/{plugin_file}"
        else:
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
        
        success, response = self.api_client.make_request(
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
    
    def disable_trigger(self, trigger_name: str) -> bool:
        """Disable a trigger by querying its configuration and updating it"""
        print_status(f"Disabling trigger: {trigger_name}")
        
        # Query the system.triggers table to get trigger configuration
        query = f"SELECT * FROM system.processing_engine_triggers WHERE trigger_name = '{trigger_name}'"
        results = self.api_client.query_internal_db(query)
        
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
        
        success, response = self.api_client.make_request(
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
        
        success, response = self.api_client.make_request(
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
                success, response = self.api_client.make_request(
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
    
    def list_all_triggers(self) -> List[str]:
        """List all triggers in the database using system tables"""
        query = "SELECT trigger_name FROM system.processing_engine_triggers"
        results = self.api_client.query_internal_db(query)
        
        if results:
            return [row.get('trigger_name', '') for row in results]
        else:
            return []
    
    def cleanup_created_triggers(self) -> None:
        """Cleanup triggers created by this manager"""
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
    
    def cleanup_all(self) -> None:
        """Complete cleanup of all triggers"""
        try:
            # Clean up all test triggers
            self.cleanup_all_test_triggers()
            
            # Clean up created triggers (if any remain)
            self.cleanup_created_triggers()
            
            # Small delay to ensure triggers are fully stopped
            time.sleep(1)
            
        except Exception as e:
            print_warning(f"Error during trigger cleanup: {e}")