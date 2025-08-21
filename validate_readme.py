#!/usr/bin/env python3
"""
Validates plugin README files against the standard template.
Ensures consistency across all plugin documentation.
"""

import sys
import re
from pathlib import Path
from typing import List, Tuple

# Required sections in order
REQUIRED_SECTIONS = [
    ("# ", "Title with plugin name"),
    ("## Description", "Plugin description"),
    ("## Configuration", "Configuration overview"),
    ("### Plugin metadata", "Metadata description"),
    ("### Required parameters", "Required parameters table"),
    ("## Installation steps", "Installation instructions"),
    ("## Trigger setup", "Trigger configuration examples"),
    ("## Example usage", "Usage examples"),
    ("## Code overview", "Code structure description"),
    ("## Troubleshooting", "Common issues and solutions"),
    ("## Questions/Comments", "Support information")
]

# Optional but recommended sections
OPTIONAL_SECTIONS = [
    "### Optional parameters",
    "### TOML configuration",
    "### Data requirements",
    "### Software requirements",
    "### Schema requirements",
    "### Debugging tips",
    "### Performance considerations"
]

def validate_emoji_metadata(content: str) -> List[str]:
    """Validate the emoji metadata line."""
    errors = []
    
    # Check for emoji metadata pattern
    metadata_pattern = r'^⚡\s+[\w\-,\s]+\s+🏷️\s+[\w\-,\s]+\s+🔧\s+InfluxDB 3'
    if not re.search(metadata_pattern, content, re.MULTILINE):
        errors.append("Missing or invalid emoji metadata line (should have ⚡ trigger types 🏷️ tags 🔧 compatibility)")
    
    return errors

def validate_sections(content: str) -> List[str]:
    """Validate required sections are present and in order."""
    errors = []
    lines = content.split('\n')
    
    # Track section positions
    section_positions = {}
    for i, line in enumerate(lines):
        for section, description in REQUIRED_SECTIONS:
            if line.startswith(section) and section not in section_positions:
                # Special handling for title (should contain actual plugin name)
                if section == "# " and not line.startswith("# Plugin Name"):
                    section_positions[section] = i
                elif section != "# ":
                    section_positions[section] = i
    
    # Check all required sections are present
    for section, description in REQUIRED_SECTIONS:
        if section not in section_positions:
            errors.append(f"Missing required section: '{section.strip()}' - {description}")
    
    # Check sections are in correct order
    if len(section_positions) == len(REQUIRED_SECTIONS):
        positions = list(section_positions.values())
        if positions != sorted(positions):
            errors.append("Sections are not in the correct order (see template for proper ordering)")
    
    return errors

def validate_parameter_tables(content: str) -> List[str]:
    """Validate parameter table formatting."""
    errors = []
    
    # Check for parameter table headers
    if '| Parameter | Type | Default | Description |' not in content:
        errors.append("No properly formatted parameter tables found (should have Parameter | Type | Default | Description columns)")
    
    # Check for required parameters section
    if '### Required parameters' in content:
        section_start = content.index('### Required parameters')
        section_end = content.find('\n###', section_start + 1)
        if section_end == -1:
            section_end = content.find('\n##', section_start + 1)
        
        section_content = content[section_start:section_end] if section_end != -1 else content[section_start:]
        
        if 'required' not in section_content.lower():
            errors.append("Required parameters section should indicate which parameters are required")
    
    return errors

def validate_examples(content: str) -> List[str]:
    """Validate code examples and expected output."""
    errors = []
    
    # Check for bash code examples
    bash_examples = re.findall(r'```bash(.*?)```', content, re.DOTALL)
    if len(bash_examples) < 2:
        errors.append(f"Should have at least 2 bash code examples (found {len(bash_examples)})")
    
    # Check for influxdb3 commands in examples
    has_create_trigger = any('influxdb3 create trigger' in ex for ex in bash_examples)
    has_write_data = any('influxdb3 write' in ex for ex in bash_examples)
    has_query = any('influxdb3 query' in ex for ex in bash_examples)
    
    if not has_create_trigger:
        errors.append("Examples should include 'influxdb3 create trigger' command")
    if not has_write_data:
        errors.append("Examples should include 'influxdb3 write' command for test data")
    if not has_query:
        errors.append("Examples should include 'influxdb3 query' command to verify results")
    
    # Check for expected output
    expected_output_count = content.count('### Expected output') + content.count('**Expected output')
    if expected_output_count < 1:
        errors.append("Should include at least one 'Expected output' section in examples")
    
    return errors

def validate_links(content: str, plugin_path: Path) -> List[str]:
    """Validate internal links and references."""
    errors = []
    
    # Check for TOML file references if TOML configuration is mentioned
    if '### TOML configuration' in content:
        toml_links = re.findall(r'\[([^\]]+\.toml)\]\(([^)]+)\)', content)
        plugin_dir = plugin_path.parent
        
        for link_text, link_path in toml_links:
            # Check if it's a relative link (not starting with http)
            if not link_path.startswith('http'):
                toml_file = plugin_dir / link_path
                if not toml_file.exists():
                    errors.append(f"Referenced TOML file not found: {link_path}")
    
    # Check for influxdb3_plugins README reference
    if '/README.md' in content and 'influxdb3_plugins/README.md' not in content:
        errors.append("Link to main README should reference 'influxdb3_plugins/README.md'")
    
    return errors

def validate_troubleshooting(content: str) -> List[str]:
    """Validate troubleshooting section content."""
    errors = []
    
    if '## Troubleshooting' in content:
        section_start = content.index('## Troubleshooting')
        section_end = content.find('\n##', section_start + 1)
        section_content = content[section_start:section_end] if section_end != -1 else content[section_start:]
        
        # Check for common subsections
        if '### Common issues' not in section_content:
            errors.append("Troubleshooting should include 'Common issues' subsection")
        
        # Check for issue/solution pattern
        issue_count = section_content.count('#### Issue:') + section_content.count('**Issue:')
        solution_count = section_content.count('**Solution:') + section_content.count('Solution:')
        
        if issue_count < 2:
            errors.append("Troubleshooting should include at least 2 documented issues")
        if issue_count > solution_count:
            errors.append("Each troubleshooting issue should have a corresponding solution")
    
    return errors

def validate_code_overview(content: str) -> List[str]:
    """Validate code overview section."""
    errors = []
    
    if '## Code overview' in content:
        section_start = content.index('## Code overview')
        section_end = content.find('\n##', section_start + 1)
        section_content = content[section_start:section_end] if section_end != -1 else content[section_start:]
        
        # Check for required subsections
        if '### Files' not in section_content:
            errors.append("Code overview should include 'Files' subsection")
        if '### Main functions' not in section_content and '### Key functions' not in section_content:
            errors.append("Code overview should include 'Main functions' or 'Key functions' subsection")
        
        # Check for function documentation
        if 'def ' not in section_content and not re.search(r'`\w+\(.*?\)`', section_content):
            errors.append("Code overview should document main functions with their signatures")
    
    return errors

def format_validation_result(readme_path: Path, errors: List[str], warnings: List[str]) -> str:
    """Format validation results for display."""
    result = []
    
    if not errors and not warnings:
        result.append(f"✅ {readme_path}")
    else:
        result.append(f"\n{'❌' if errors else '⚠️'} {readme_path}:")
        
        if errors:
            result.append("  Errors:")
            for error in errors:
                result.append(f"    - {error}")
        
        if warnings:
            result.append("  Warnings:")
            for warning in warnings:
                result.append(f"    - {warning}")
    
    return '\n'.join(result)

def validate_readme(readme_path: Path) -> Tuple[List[str], List[str]]:
    """
    Validate a single README file.
    Returns tuple of (errors, warnings).
    """
    try:
        with open(readme_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        return [f"Could not read file: {e}"], []
    
    errors = []
    warnings = []
    
    # Run all validations
    errors.extend(validate_emoji_metadata(content))
    errors.extend(validate_sections(content))
    errors.extend(validate_parameter_tables(content))
    errors.extend(validate_examples(content))
    errors.extend(validate_links(content, readme_path))
    errors.extend(validate_troubleshooting(content))
    errors.extend(validate_code_overview(content))
    
    # Check for optional but recommended sections
    for section in OPTIONAL_SECTIONS:
        if section not in content and section not in ["### Debugging tips", "### Performance considerations"]:
            warnings.append(f"Consider adding '{section}' section")
    
    # Check for template remnants
    if 'Plugin Name' in content and '# Plugin Name' in content:
        errors.append("README still contains template placeholder 'Plugin Name'")
    if 'Template Usage Notes' in content:
        errors.append("README still contains 'Template Usage Notes' section (should be removed)")
    
    return errors, warnings

def main():
    """Main validation function."""
    # Find all plugin READMEs
    influxdata_dir = Path('influxdata')
    
    if not influxdata_dir.exists():
        print("❌ Error: 'influxdata' directory not found. Run this script from the influxdb3_plugins root directory.")
        sys.exit(1)
    
    readme_files = list(influxdata_dir.glob('*/README.md'))
    
    if not readme_files:
        print("❌ No README files found in influxdata/ subdirectories")
        sys.exit(1)
    
    print(f"Validating {len(readme_files)} plugin README files...\n")
    
    all_valid = True
    error_count = 0
    warning_count = 0
    
    for readme_path in sorted(readme_files):
        errors, warnings = validate_readme(readme_path)
        
        if errors:
            all_valid = False
            error_count += len(errors)
        warning_count += len(warnings)
        
        print(format_validation_result(readme_path, errors, warnings))
    
    # Print summary
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    print(f"Total files validated: {len(readme_files)}")
    print(f"Errors found: {error_count}")
    print(f"Warnings found: {warning_count}")
    
    if all_valid:
        print("\n✅ All README files are valid!")
        sys.exit(0)
    else:
        print(f"\n❌ Validation failed with {error_count} error(s)")
        print("\nPlease fix the errors above and ensure all READMEs follow the template.")
        print("See README_TEMPLATE.md for the correct structure.")
        sys.exit(1)

if __name__ == "__main__":
    main()