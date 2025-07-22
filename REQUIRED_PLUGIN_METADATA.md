# Plugin Metadata Requirements for InfluxDB 3 Explorer

This document describes the metadata requirements for plugins to be properly displayed and configured in the InfluxDB 3 Explorer UI.

## Plugin Docstring Header

To work with InfluxDB 3 Explorer, each plugin file must start with a docstring containing a JSON metadata structure. This metadata provides information about what arguments the plugin accepts, including examples, names, and whether they are required.

### Metadata Template

```python
"""
{
  "plugin_type": ["scheduled", "onwrite"],  # Supported values: "http", "onwrite", "scheduled"
  "scheduled_args_config": [
    {
      "name": "measurement",
      "example": "home",
      "description": "Measurement to write to",
      "required": true
    },
    {
      "name": "senders",
      "example": "http.slack",
      "description": "Dot-separated list of notification channels (e.g., slack.discord). Supported channels: slack, discord, sms, whatsapp, http.",
      "required": true
    }
  ],
  "onwrite_args_config": [],  # For onwrite type, uses same format as scheduled_args_config
  "http_args_config": []      # For http type, uses same format as scheduled_args_config
}
"""
```

### Required Fields

- **plugin_type**: At a minimum, this field must be defined. It specifies which trigger types the plugin supports.
- **\*_args_config**: The configuration arrays (`http_args_config`, `onwrite_args_config`, and `scheduled_args_config`) can be omitted if the plugin does not use them.

### Argument Configuration

Each argument in the `*_args_config` arrays must include:
- **name**: The argument name
- **example**: An example value
- **description**: A clear description of the argument's purpose
- **required**: Boolean indicating whether the argument is mandatory

**Important**: The header structure must comply with valid JSON format.
## Plugin Library Metadata


Generated `plugin_library.json` files contain a "plugins" field with a list of plugins, where each plugin entry includes metadata extracted from the plugin's JSON docstring along with additional repository information.

For example, the `influxdata/plugin_library.json` file is automatically generated from plugin docstring metadata by repository owners to enable plugin discovery in the InfluxDB 3 Explorer UI. This file should not be manually edited.

### Generated Plugin Entry Format

The automatically generated plugin library entries have the following structure:

```json
{
  "name": "Downsampler",
  "path": "influxdata/downsampler/downsampler.py",
  "description": "Enables downsampling of data in an InfluxDB 3 instance with flexible configuration for time intervals, field aggregations, tag filtering, and batch processing. Supports both scheduler and HTTP trigger modes.",
  "author": "InfluxData",
  "docs_file_link": "https://github.com/influxdata/influxdb3_plugins/blob/main/influxdata/downsampler/README.md",
  "required_plugins": [],
  "required_libraries": [],
  "last_update": "2025-07-18",
  "trigger_types_supported": ["scheduler", "http"]
}
```

### Field Descriptions

- **name**: Plugin name (displayed in UI)
- **path**: Relative path to the plugin file from the repository root
  - For example, if the full URL is `https://github.com/influxdata/influxdb3_plugins/blob/main/influxdata/downsampler/downsampler.py`, use `"influxdata/downsampler/downsampler.py"`
- **description**: A short description of the plugin (displayed in UI)
- **author**: The author's name (displayed in UI)
- **docs_file_link**: Link to the documentation (README or other documentation, displayed in UI)
- **required_plugins**: Dependencies on other plugins, specified as:
  ```json
  {
    "name": "Notification sender",
    "path": "influxdata/notifier/notifier_plugin.py"
  }
  ```
- **required_libraries**: Python libraries required for the plugin to work
  - Example: `"required_libraries": ["httpx", "twilio"]`
- **last_update**: The date of the last plugin update
- **trigger_types_supported**: Array of supported trigger types
