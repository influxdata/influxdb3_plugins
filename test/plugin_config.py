"""
Unified plugin configuration system for InfluxDB 3 plugin testing

Handles plugin metadata parsing, test argument generation, 
and TOML configuration file management.
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Optional


def print_warning(message: str) -> None:
    """Print warning message in yellow"""
    print(f"\033[1;33m[WARN]\033[0m {message}")


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


class PluginConfig:
    """Unified plugin configuration manager"""
    
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
            "scheduled": {"dry_run": "false"},
            "onwrite": {"dry_run": "false"},
            "http": {"dry_run": "false"}
        }
        
        args = base_args.get(plugin_type, {"dry_run": "false"}).copy()
        
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
        """Get required packages for this plugin"""
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