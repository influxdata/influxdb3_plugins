# Style Guidelines for influxdb3_plugins Repository

## Purpose

Apply consistent documentation standards to the example plugins repository.

## File Organization

### Directory Structure

 plugin_name/
 ├── README.md
 ├── plugin_name.py
 ├── config.py
 ├── test/
 │   ├── test_plugin_name.py
 │   └── fixtures/
 └── examples/
     └── example_config.yaml

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

Each plugin must include JSON metadata in a docstring at the top of the plugin file. This metadata is required for: -[InfluxDB 3 Explorer](https://docs.influxdata.com/influxdb3/explorer/) UI integration and configuration

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

- Use [Google Developer Documentation style](https://developers.google.com/style)
- Use active voice--state who or what is performing the action
- Use present tense
- Use simple, clear language
- Avoid jargon and complex terminology
- Use second person ("you") for instructions
- Use third person for general descriptions

## Markdown Style Guidelines

### Linting

Use markdownlint-cli2 to lint Markdown. The following command runs the linter on all Markdown files in the repository:

```bash
npx markdownlint-cli2 --fix "**/*.md"
```

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

### Test a plugin

Use the `influxdb3 test` CLI command to run a plugin without creating a trigger.
For an example, see [Run Tests](README.md#run-tests).

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

## Documentation Sync Process

### Syncing Plugin Documentation to docs-v2

When you update plugin READMEs in this repository, they need to be synchronized to the InfluxDB documentation site (docs-v2) to appear on docs.influxdata.com.

#### Automated Sync Workflow

1. **Make your changes** - Update plugin README files following the template structure
2. **Commit and push** - Push your changes to the master branch
3. **Check for reminder** - A workflow will automatically detect README changes and create a commit comment with a sync link
4. **Click sync link** - The comment includes a pre-filled link to create a sync request in docs-v2
5. **Submit request** - This automatically triggers validation, transformation, and PR creation
6. **Review PR** - The resulting PR in docs-v2 includes screenshots and change summaries

#### Manual Sync (Alternative)

You can also manually trigger synchronization:

1. **Navigate to sync form**: [Create sync request](https://github.com/influxdata/docs-v2/issues/new?template=sync-plugin-docs.yml)
2. **Fill in details** - Specify plugin names and source commit
3. **Submit** - The automation workflow handles the rest

#### Sync Requirements

Before syncing, ensure your README:

- ✅ Follows the [README_TEMPLATE.md](README_TEMPLATE.md) structure
- ✅ Includes proper emoji metadata (`⚡` triggers, `🏷️` tags, `🔧` compatibility)
- ✅ Has all required sections with proper formatting
- ✅ Contains working examples with expected output
- ✅ Passes validation (`python scripts/validate_readme.py`)

#### What Gets Transformed

During sync, your README content is automatically transformed for docs-v2:

- **Emoji metadata removed** (already in plugin JSON metadata)
- **Relative links converted** to GitHub URLs
- **Product references enhanced** with Hugo shortcodes (`{{% product-name %}}`)
- **Logging section added** with standard content
- **Support sections updated** with docs-v2 format

Your original README remains unchanged - these transformations only apply to the docs-v2 copy.

## Commit Message Format

### Use conventional commits

 type(scope): description

 feat(plugin): add new data transformation plugin
 fix(docs): correct configuration parameter description
 docs(readme): update installation instructions

### Types

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Code style changes
- `refactor`: Code refactoring
- `test`: Test changes
- `chore`: Maintenance tasks

### Documentation Sync Messages

When making documentation-focused commits, use clear messages that describe what changed:

```bash
# Good commit messages for docs sync
docs(basic_transformation): update configuration parameters and examples
feat(downsampler): add support for custom aggregation functions  
fix(notifier): correct email configuration example

# These will trigger sync reminders automatically
```

*These standards are extracted from the [InfluxData Documentation guidelines](https://github.com/influxdata/docs-v2/blob/master/CONTRIBUTING.md).*
