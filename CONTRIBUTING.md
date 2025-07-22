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

### Plugin Metadata

Each plugin must include JSON metadata in a docstring at the top of the plugin file. This metadata is required for:
- InfluxDB 3 Explorer UI integration and configuration
- Automated testing with the repository test scripts

For complete metadata specifications, formatting requirements, and examples, see [REQUIRED_PLUGIN_METADATA.md](REQUIRED_PLUGIN_METADATA.md).


#### Usage Guidelines
- Place emoji metadata directly after the h1 title
- Use one emoji per line
- Keep trigger types minimal (maximum 3 types)
- Use 2-4 descriptive tags maximum
- Order: trigger types first, then tags, then status indicators

#### Common Trigger Types
- `scheduled` - Time-based execution
- `data-write` - Write-ahead log events (data writes)
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

You can run tests using either Docker Compose (recommended) or a local Python environment.

#### Option 1: Docker Compose Testing (Recommended)

No Python installation required. Uses Docker Compose services for testing:

**Using the docker-test.sh script:**
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

**Using docker compose directly:**
```bash
# Test all plugins with InfluxDB 3 Core
docker compose --profile test run --rm test-core-all

# Test all plugins with InfluxDB 3 Enterprise  
docker compose --profile test run --rm test-enterprise-all

# Test a specific plugin with Core
PLUGIN_PATH="influxdata/basic_transformation" \
docker compose --profile test run --rm test-core-specific

# Test with TOML configuration
PLUGIN_PATH="influxdata/basic_transformation" \
PLUGIN_FILE="basic_transformation.py" \
TOML_CONFIG="basic_transformation_config_scheduler.toml" \
PACKAGES="pint" \
docker compose --profile test run --rm test-core-toml

# Start services manually for interactive testing
docker compose up -d influxdb3-core
docker compose run --rm plugin-tester bash
```

#### Option 2: Local Python Environment

For local development without Docker:

1. **Python environment setup**:
   ```bash
   # Create and activate a virtual environment
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install test dependencies
   pip install -r requirements.txt
   ```
   
   Or use the setup script:
   ```bash
   ./setup-test-env.sh
   source venv/bin/activate
   ```

2. **Start InfluxDB 3 locally**:
   ```bash
   # Core version
   docker compose up -d influxdb3-core
   
   # Or Enterprise version
   docker compose up -d influxdb3-enterprise
   ```

3. **Run tests**:
   ```bash
   # Test all influxdata plugins with Core
   python test-plugins.py influxdata --core --skip-container

   # Test specific plugin
   python test-plugins.py influxdata/basic_transformation --skip-container

   # Test with Enterprise
   python test-plugins.py influxdata --enterprise --skip-container --host http://localhost:8182

   # Test with TOML configuration
   python test_plugin_toml.py influxdata/basic_transformation basic_transformation.py \
     --toml-config basic_transformation_config_scheduler.toml \
     --packages pint
   ```

**Note:** Use `--skip-container` flag to avoid Docker management when running locally.

### Available Test Scripts

#### 1. `docker-test.sh` - Docker-based Test Runner
Wrapper script for Docker Compose testing with simplified commands.

**Commands:**
- `all` - Test all plugins in influxdata organization
- `plugin <path>` - Test a specific plugin
- `toml <path> <file>` - Test with TOML configuration
- `shell` - Start interactive test container
- `clean` - Stop and remove test containers

#### 2. `test-plugins.py` - Organization/Plugin Testing
Tests all plugins in an organization or specific plugins using the InfluxDB 3 HTTP API.

**Usage:**
```bash
# Test all influxdata plugins with InfluxDB 3 Core (default)
python test-plugins.py influxdata --core --skip-container

# Test a specific plugin
python test-plugins.py influxdata/basic_transformation --skip-container

# Test with InfluxDB 3 Enterprise
python test-plugins.py influxdata --enterprise --skip-container --host http://localhost:8182

# List available plugins
python test-plugins.py --list
```

#### 3. `test_plugin_toml.py` - Generic API-based Testing
Reusable script for testing plugins with TOML configuration support using the InfluxDB 3 HTTP API.

**Features:**
- Automatically parses plugin metadata from JSON schema in docstrings
- Dynamically determines supported trigger types (`scheduled`, `onwrite`, `http`)
- Tests both inline arguments and TOML configuration files
- Validates PLUGIN_DIR environment variable setup
- Uses `/api/v3` endpoints for reliable testing

**Usage:**
```bash
# Test basic transformation plugin with TOML config
python test_plugin_toml.py influxdata/basic_transformation basic_transformation.py \
  --toml-config basic_transformation_config_scheduler.toml \
  --packages pint

# Test downsampler plugin
python test_plugin_toml.py influxdata/downsampler downsampler.py \
  --toml-config downsampling_config_scheduler.toml

# Test with custom settings
python test_plugin_toml.py influxdata/my_plugin my_plugin.py \
  --database custom_testdb \
  --host http://localhost:8282 \
  --packages numpy pandas \
  --test-data "metrics,host=server1 cpu=50.0"
```

#### 4. Docker Compose Services
Direct access to containerized test services:

**Core services:**
- `test-core-all` - Test all plugins with InfluxDB 3 Core
- `test-core-specific` - Test specific plugin with Core
- `test-core-toml` - Test with TOML configuration and Core

**Enterprise services:**
- `test-enterprise-all` - Test all plugins with InfluxDB 3 Enterprise  
- `test-enterprise-specific` - Test specific plugin with Enterprise
- `test-enterprise-toml` - Test with TOML configuration and Enterprise

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