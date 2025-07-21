# Style Guidelines for influxdb3_plugins Repository

## Purpose
Apply consistent documentation standards to the example plugins repository.

## File Organization

### Directory Structure
```
plugin_name/
├── README.md
├── plugin_name.py
├── config.py
├── test/
│   ├── test_plugin_name.py
│   └── fixtures/
└── examples/
    └── example_config.yaml
```

### File Naming
- Use snake_case for Python files
- Use kebab-case for documentation files
- Use descriptive names that match functionality

## README Structure

### Required Sections
Each plugin README should include:

1. **Plugin Name** (h1)
2. **Emoji Metadata** - Quick reference indicators
3. **Description** - Brief overview of functionality
4. **Configuration** - All configuration parameters
5. **Requirements** - Dependencies and prerequisites
6. **Trigger Setup** - How to configure triggers
7. **Example Usage** - Working examples with expected output
8. **Code Overview** - Walkthrough of key functions
9. **Troubleshooting** - Common issues and solutions

### Python Metadata Constants

Each plugin must define metadata as Python constants at the top of the plugin file:

```python
# Required metadata constants
PLUGIN_NAME = "Plugin Name"
PLUGIN_AUTHOR = "Author Name"
PLUGIN_VERSION = "1.0.0"
PLUGIN_DESCRIPTION = "Brief description of plugin 
functionality"
PLUGIN_TRIGGER_TYPES = ["scheduler", "http"]  # 
Supported trigger types
PLUGIN_REQUIRED_LIBRARIES = []  # Python 
dependencies
PLUGIN_REQUIRED_PLUGINS = []    # Plugin 
dependencies
```


#### Usage Guidelines
- Place emoji metadata directly after the h1 title
- Use one emoji per line
- Keep trigger types minimal (maximum 3 types)
- Use 2-4 descriptive tags maximum
- Order: trigger types first, then tags, then status indicators

#### Common Trigger Types
- `scheduled` - Time-based execution
- `wal` - Write-ahead log events (data writes)
- `http` - HTTP request handling

#### Common Tags
- `transformation` - Data modification/conversion
- `monitoring` - System/data monitoring
- `alerting` - Notification/alert generation
- `data-cleaning` - Data standardization/cleaning
- `downsampling` - Data aggregation/reduction
- `forecasting` - Predictive analytics
- `anomaly-detection` - Outlier identification
- `notification` - Message/alert delivery
- `unit-conversion` - Measurement unit changes

**Example:**
```markdown
# Basic Transformation Plugin

⚡ scheduled, wal
🏷️ transformation, data-cleaning, unit-conversion

## Description
...
```

### Configuration Documentation
- Document all parameters with types and defaults
- Use tables for complex configurations
- Include examples for each parameter

**Example:**
```markdown
## Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `database` | string | required | Target database name |
| `batch_size` | integer | 1000 | Maximum records per batch |
| `timeout` | integer | 30 | Connection timeout in seconds |
```

### Example Usage Format
- Provide complete, working examples
  - Use `curl` for HTTP examples
- Show expected input and output
- Include error handling examples
- Use long options in command line examples (`--option` instead of `-o`)

**Example:**
```markdown
## Example Usage

### Basic Usage
```python
from plugins.example_plugin import ExamplePlugin

plugin = ExamplePlugin({
    'database': 'my_database',
    'batch_size': 500
})

result = plugin.process(data)
```

### Expected Output
```json
{
  "success": true,
  "records_processed": 500,
  "timestamp": "2024-01-01T00:00:00Z"
}
```

## Voice, tone, and grammar
- Use Google Developer Documentation style
- Use active voice--state who or what is performing the action
- Use present tense
- Use simple, clear language
- Avoid jargon and complex terminology
- Use second person ("you") for instructions
- Use third person for general descriptions

## Markdown Style Guidelines

### Semantic Line Feeds
- Use semantic line feeds (one sentence per line)
- Break lines at natural language boundaries
- This improves diff readability and makes editing easier

**Example:**
```markdown
This is the first sentence.
This is the second sentence that continues the paragraph.
```

### Heading Structure
- Use h2-h6 headings in content (h1 comes from title)
- Use sentence case for headings in README files
- Don't skip heading levels

**Example:**
```markdown
## Configuration parameters
### Required parameters
#### Database connection
```

### Code Block Formatting
- Format code examples to fit within 80 characters
- Use long options in command line examples (`--option` instead of `-o`)
- Include proper syntax highlighting

**Example:**
```bash
influxdb3 write \
  --database DATABASE_NAME \
  --token AUTH_TOKEN \
  --precision ns
```

### File and Directory References
- Use backticks for file names and paths
- Use forward slashes for paths (even on Windows)

**Example:**
```markdown
Edit the `config.py` file in the `plugins/` directory.
```

## Code Documentation Standards

### Python Docstrings
- Use triple quotes for docstrings
- Follow Google docstring style
- Include type hints where possible

**Example:**
```python
def process_data(data: dict, config: dict) -> bool:
    """Process incoming data using the provided configuration.
    
    Args:
        data: The incoming data dictionary
        config: Plugin configuration parameters
        
    Returns:
        True if processing succeeded, False otherwise
        
    Raises:
        ValueError: If required configuration is missing
    """
```

### Comments
- Use inline comments to explain complex logic
- Avoid obvious comments
- Explain the "why" not the "what"

**Example:**
```python
# Convert timestamp to nanoseconds for InfluxDB compatibility
timestamp_ns = int(timestamp * 1e9)
```

## Placeholder Conventions

### Use UPPERCASE for placeholders
```bash
influxdb3 write --database DATABASE_NAME --token AUTH_TOKEN
```

### Placeholder Descriptions
- Don't use pronouns ("your", "this")
- Be specific about data types and formats
- Provide examples for complex formats

**Example:**
```markdown
Replace the following:
- `DATABASE_NAME`: the name of the database to write to
- `AUTH_TOKEN`: a database token with write permissions
```

## Links and References

### Internal Links
- Use relative paths for internal repository links
- Use descriptive link text

**Example:**
```markdown
See the [configuration guide](./docs/configuration.md) for details.
```

### External Links
- Use full URLs for external references
- Include link text that describes the destination

**Example:**
```markdown
For more information, see the [InfluxDB 3 documentation](https://docs.influxdata.com/influxdb3/).
```

## Testing Documentation

### Plugin Testing Overview

The repository provides comprehensive testing tools for validating plugins with both inline arguments and TOML configuration files. All tests verify proper PLUGIN_DIR environment variable setup and API functionality.

### Prerequisites

1. **Docker and Docker Compose** must be installed and running
2. **InfluxDB 3 Core Docker image** will be pulled automatically

### Testing Methods

You can run tests using either Docker (recommended) or a local Python environment.

#### Option 1: Docker-based Testing (Recommended)

No Python installation required. The Docker environment handles all dependencies:

```bash
# Test all influxdata plugins
./docker-test.sh all

# Test a specific plugin
./docker-test.sh plugin influxdata/basic_transformation

# Test with TOML configuration
./docker-test.sh toml influxdata/basic_transformation basic_transformation.py \
  --toml-config basic_transformation_config_scheduler.toml

# Start an interactive shell for debugging
./docker-test.sh shell

# Clean up containers
./docker-test.sh clean
```

#### Option 2: Local Python Environment

If you prefer to run tests locally:

1. **Python environment setup**:
   ```bash
   # Create and activate a virtual environment
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install test dependencies
   pip install -r requirements.txt
   ```
   
   Alternatively, use the provided setup script:
   ```bash
   ./setup-test-env.sh
   source venv/bin/activate
   ```

2. **Run tests**:
   ```bash
   python test-plugins.py influxdata --core
   ```

**Note:** Plugin-specific packages are installed automatically via the InfluxDB API during testing

### Available Test Scripts

#### 1. `test-plugins.py` - Organization/Plugin Testing
Tests all plugins in an organization or specific plugins using the InfluxDB 3 HTTP API.

**Usage:**
```bash
# Test all influxdata plugins with InfluxDB 3 Core (default)
python test-plugins.py influxdata --core

# Test a specific plugin
python test-plugins.py influxdata/basic_transformation

# Test with InfluxDB 3 Enterprise
python test-plugins.py influxdata --enterprise

# List available plugins
python test-plugins.py --list

# Using the virtual environment directly
venv/bin/python test-plugins.py influxdata --core
```

**Note:** The legacy `test-plugins.sh` bash script is also available but the Python version is recommended for better reliability and cross-platform compatibility.

#### 2. `test_plugin_toml.py` - Generic API-based Testing
A reusable Python script that uses the InfluxDB 3 HTTP API to test plugins with TOML configuration support.

**Features:**
- Automatically parses plugin metadata from JSON schema in docstrings
- Dynamically determines supported trigger types (`scheduled`, `onwrite`, `http`)
- Tests both inline arguments and TOML configuration files
- Validates PLUGIN_DIR environment variable setup
- Uses `/api/v3` endpoints for reliable testing

**Usage:**
```bash
# Test basic transformation plugin with TOML config
./test_plugin_toml.py influxdata/basic_transformation basic_transformation.py \
  --toml-config basic_transformation_config_scheduler.toml \
  --packages pint

# Test downsampler plugin
./test_plugin_toml.py influxdata/downsampler downsampler.py \
  --toml-config downsampling_config_scheduler.toml

# Test with custom settings
./test_plugin_toml.py influxdata/my_plugin my_plugin.py \
  --database custom_testdb \
  --host http://localhost:8282 \
  --packages numpy pandas \
  --test-data "metrics,host=server1 cpu=50.0"
```


### TOML Configuration Testing

#### Prerequisites
To test TOML configuration files, you must set the `PLUGIN_DIR` environment variable:

```bash
# Set PLUGIN_DIR when starting InfluxDB 3
PLUGIN_DIR=~/.plugins influxdb3 serve --node-id node0 --object-store file --data-dir ~/.influxdb3 --plugin-dir ~/.plugins
```

#### TOML Configuration Requirements
- The `PLUGIN_DIR` environment variable must be set in the InfluxDB 3 host environment
- TOML files must be located in the directory specified by `PLUGIN_DIR`
- Use `config_file_path` parameter with just the filename (not full path)

#### Example TOML Configuration
```toml
# basic_transformation_config_scheduler.toml
measurement = "temperature"
target_measurement = "temp_transformed"
window = "30d"

[names_transformations]
sensor = ["upper"]
temp = ["snake"]

[values_transformations]
temp = ["convert_degC_to_degF"]
```

### Plugin Metadata for Testing

The generic test script automatically reads plugin capabilities from the JSON schema in the plugin docstring:

```python
"""
{
    "plugin_type": ["scheduled", "onwrite"],
    "scheduled_args_config": [
        {
            "name": "measurement",
            "example": "temperature",
            "description": "Source measurement name",
            "required": true
        },
        {
            "name": "window",
            "example": "30d",
            "description": "Time window for data retrieval",
            "required": true
        }
    ],
    "onwrite_args_config": [
        {
            "name": "measurement",
            "example": "temperature",
            "description": "Source measurement name",
            "required": true
        }
    ]
}
"""
```

### Test Execution Flow

1. **Environment Setup**: Starts InfluxDB 3 Core container with Docker Compose
2. **Database Creation**: Creates test databases using `/api/v3/configure/database`
3. **Dependency Installation**: Installs required Python packages via `/api/v3/configure/plugin_environment/install_packages`
4. **Environment Validation**: Checks that `PLUGIN_DIR` is properly set
5. **Trigger Testing**: Creates triggers for each supported plugin type using `/api/v3/configure/processing_engine_trigger`
6. **TOML Configuration Testing**: Tests TOML file access and parsing
7. **Cleanup**: Removes test triggers and cleans up resources

### Test Categories

#### 1. Basic Functionality Tests
- Plugin dependency installation
- Environment variable validation
- Inline argument processing

#### 2. TOML Configuration Tests
- TOML file loading from `PLUGIN_DIR`
- Configuration parsing and validation
- Subdirectory config file access

#### 3. Plugin Type Tests
- **Scheduled plugins**: `every:5m` trigger specification
- **Onwrite plugins**: `all_tables` trigger specification  
- **HTTP plugins**: `request:test` trigger specification

### Expected Test Results

```bash
Starting TOML configuration test for influxdata/basic_transformation
[INFO] Starting InfluxDB 3 Core container...
[INFO] InfluxDB 3 Core is ready!
[INFO] Creating database: testdb
[INFO] Writing test data...
[INFO] Test: Installing plugin dependencies
[INFO] ✓ Plugin dependencies installation works
[INFO] Test: Checking PLUGIN_DIR environment variable
[INFO] ✓ PLUGIN_DIR environment variable works
[INFO] ✓ scheduled inline arguments trigger works
[INFO] ✓ onwrite inline arguments trigger works
[INFO] ✓ scheduled TOML config trigger creation works
[INFO] ✓ onwrite TOML config trigger creation works
[INFO] Test complete!
```

### Common Test Issues and Solutions

#### Issue: PLUGIN_DIR not found
**Solution**: Ensure `PLUGIN_DIR` environment variable is set when starting InfluxDB 3:
```bash
PLUGIN_DIR=~/.plugins influxdb3 serve --plugin-dir ~/.plugins
```

#### Issue: TOML config file not found
**Solution**: Verify the TOML file exists in the `PLUGIN_DIR` directory and use only the filename in `config_file_path`.

#### Issue: Plugin dependency installation fails
**Solution**: Check that the package name is correct and available in the Python package index.

### Test Documentation Standards
- Document what each test validates
- Include setup and teardown requirements
- Provide examples of test data
- Explain expected outcomes and error conditions

### Test Example Format
```python
def test_plugin_basic_functionality():
    """Test basic plugin functionality with mock data."""
    # Test implementation
    pass
```

## Error Handling

### Error Messages
- Use clear, actionable error messages
- Include suggested solutions
- Provide context about the error

**Example:**
```python
raise ValueError(
    f"Database '{database_name}' not found. "
    f"Check the database name and ensure it exists."
)
```

## Commit Message Format

### Use conventional commits
```
type(scope): description

feat(plugin): add new data transformation plugin
fix(docs): correct configuration parameter description
docs(readme): update installation instructions
```

### Types
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Code style changes
- `refactor`: Code refactoring
- `test`: Test changes
- `chore`: Maintenance tasks

_These standards are extracted from the [InfluxData Documentation guidelines](https://github.com/influxdata/docs-v2/blob/master/CONTRIBUTING.md)._