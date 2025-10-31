#!/usr/bin/env python3
"""
Validates plugin README files against the standard template.
Ensures consistency across all plugin documentation.
"""

import sys
import re
import argparse
from pathlib import Path
from typing import List, Tuple

# Validation thresholds and limits
MINIMUM_BASH_EXAMPLES = 2
MINIMUM_EXPECTED_OUTPUT_SECTIONS = 1
MINIMUM_TROUBLESHOOTING_ISSUES = 2
SUMMARY_SEPARATOR_LENGTH = 60

# Section search offsets
SECTION_SEARCH_OFFSET = 100
SECTION_NOT_FOUND = -1

# Exit codes
EXIT_SUCCESS = 0
EXIT_ERROR = 1
EMOJI_METADATA_PATTERN = r'^⚡\s+[\w\-,\s]+\s+🏷️\s+[\w\-,\s]+\s+🔧\s+InfluxDB 3'

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

OPTIONAL_SECTIONS_SKIP_WARNING = ["### Debugging tips", "### Performance considerations"]

def extract_section_content(content: str, section_heading: str) -> str:
    """
    Extract content between a markdown section heading and the next same-level heading.
    
    This function properly handles:
    - Subsections (###, ####) within the section
    - Code blocks containing ## characters
    - Section being at start or end of document
    - Missing sections
    
    Args:
        content: Full markdown document content
        section_heading: Heading text without the ## prefix (e.g., "Troubleshooting")
    
    Returns:
        Section content as string, or empty string if section not found
    
    Examples:
        >>> extract_section_content(doc, "Troubleshooting")
        '### Common issues\\n#### Issue: ...'
    """
    # Build the section marker - look for "## Heading" where Heading doesn't start with #
    # This ensures we match "## Troubleshooting" but not "### Troubleshooting"
    section_pattern = f'## {section_heading}'
    
    # Find the section heading
    section_index = content.find(section_pattern)
    if section_index == -1:
        return ""
    
    # Find the end of the heading line (start of content)
    content_start = content.find('\n', section_index)
    if content_start == -1:
        # Section heading is at end of file with no content
        return ""
    content_start += 1  # Move past the newline
    
    # Find the next same-level heading (## followed by space, not ###)
    # Use a loop to ensure we're not matching subsections
    search_pos = content_start
    while True:
        next_heading_pos = content.find('\n## ', search_pos)
        
        if next_heading_pos == -1:
            # No more headings, take everything to end of document
            return content[content_start:].rstrip()
        
        # Check that it's actually a ## heading and not ###
        # Look at the character after '## '
        check_pos = next_heading_pos + 4  # Position after '\n## '
        if check_pos < len(content) and content[check_pos] != '#':
            # This is a proper ## heading, not ### or ####
            return content[content_start:next_heading_pos].rstrip()
        
        # This was a false match (like \n### ), keep searching
        search_pos = next_heading_pos + 1
        
        # Safety: if we've searched too far, something is wrong
        if search_pos > len(content):
            return content[content_start:].rstrip()
        
def validate_emoji_metadata(content: str) -> List[str]:
    """Validate the emoji metadata line."""
    errors = []
    
    # Check for emoji metadata pattern
    if not re.search(EMOJI_METADATA_PATTERN, content, re.MULTILINE):
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
    
    # Check for parameter table headers with flexible whitespace
    # This regex allows variable spacing between columns
    table_pattern = r'\|\s*Parameter\s*\|\s*Type\s*\|\s*Default\s*\|\s*Description\s*\|'
    if not re.search(table_pattern, content):
        errors.append("No properly formatted parameter tables found (should have Parameter | Type | Default | Description columns)")
    
    # Validate Required parameters section if present
    if '### Required parameters' in content:
        # Find the subsection
        subsection_start = content.find('### Required parameters')
        
        # Find the end: next subsection (###) or next section (##)
        # Look for '\n### ' or '\n## ' after current position
        search_pos = subsection_start + len('### Required parameters')
        
        next_subsection = content.find('\n### ', search_pos)
        next_section = content.find('\n## ', search_pos)
        
        # Determine the end position
        if next_subsection == -1 and next_section == -1:
            subsection_end = len(content)
        elif next_subsection == -1:
            subsection_end = next_section
        elif next_section == -1:
            subsection_end = next_subsection
        else:
            subsection_end = min(next_subsection, next_section)
        
        section_content = content[subsection_start:subsection_end]
        
        # Check that it indicates which parameters are required
        if 'required' not in section_content.lower() and 'yes' not in section_content.lower():
            errors.append("Required parameters section should indicate which parameters are required")
    
    return errors

def validate_examples(content: str) -> List[str]:
    """Validate code examples and expected output."""
    errors = []
    
    # Check for bash code examples
    bash_examples = re.findall(r'```bash(.*?)```', content, re.DOTALL)
    if len(bash_examples) < MINIMUM_BASH_EXAMPLES:
        errors.append(f"Should have at least {MINIMUM_BASH_EXAMPLES} bash code examples (found {len(bash_examples)})")
    
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
    if expected_output_count < MINIMUM_EXPECTED_OUTPUT_SECTIONS:
        errors.append(f"Should include at least {MINIMUM_EXPECTED_OUTPUT_SECTIONS} 'Expected output' section in examples")
    
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
    
    # Check if section exists
    if '## Troubleshooting' not in content:
        return errors
    
    # Extract the section content using robust extraction
    section_content = extract_section_content(content, 'Troubleshooting')
    
    # Defensive check
    if not section_content.strip():
        errors.append("Troubleshooting section exists but appears empty")
        return errors
    
    # Check for required subsections
    if '### Common issues' not in section_content:
        errors.append("Troubleshooting should include 'Common issues' subsection")
    
    # Check for issue/solution patterns
    # Count both markdown heading style (####) and bold style (**)
    issue_count = section_content.count('#### Issue:') + section_content.count('**Issue:')
    solution_count = (section_content.count('**Solution**:') + section_content.count('**Solution:') + section_content.count('Solution:'))
    
    if issue_count < MINIMUM_TROUBLESHOOTING_ISSUES:
        errors.append(f"Troubleshooting should include at least {MINIMUM_TROUBLESHOOTING_ISSUES} documented issues")
    
    if issue_count > 0 and solution_count < issue_count:
        errors.append("Each troubleshooting issue should have a corresponding solution")
    
    return errors

def validate_code_overview(content: str) -> List[str]:
    """Validate code overview section."""
    errors = []
    
    # Check if section exists
    if '## Code overview' not in content:
        return errors
    
    # Extract the section content using robust extraction
    section_content = extract_section_content(content, 'Code overview')
    
    # Defensive check
    if not section_content.strip():
        errors.append("Code overview section exists but appears empty")
        return errors
    
    # Check for required subsections
    if '### Files' not in section_content:
        errors.append("Code overview should include 'Files' subsection")
    
    if not ('### Main functions' in section_content or '### Key functions' in section_content):
        errors.append("Code overview should include 'Main functions' or 'Key functions' subsection")
    
    # Check for function documentation (signatures in backticks or Python def)
    has_function_signatures = (
        'def ' in section_content or 
        re.search(r'####?\s+`\w+\(.*?\)`', section_content) or
        re.search(r'`\w+\([^)]*\)`:', section_content)
    )
    
    if not has_function_signatures:
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
        if section not in content and section not in OPTIONAL_SECTIONS_SKIP_WARNING:
            warnings.append(f"Consider adding '{section}' section")
    
    # Check for template remnants
    if 'Plugin Name' in content and '# Plugin Name' in content:
        errors.append("README still contains template placeholder 'Plugin Name'")
    if 'Template Usage Notes' in content:
        errors.append("README still contains 'Template Usage Notes' section (should be removed)")
    
    return errors, warnings

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Validates plugin README files against the standard template.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python scripts/validate_readme.py                    # Validate all plugins
  python scripts/validate_readme.py --plugins basic_transformation,downsampler
  python scripts/validate_readme.py --list            # List available plugins
  python scripts/validate_readme.py --quiet           # Show only errors

Validation Rules:
  - Checks for required sections in correct order
  - Validates emoji metadata format
  - Ensures parameter tables are properly formatted
  - Verifies code examples include required commands
  - Validates troubleshooting content structure
        '''
    )
    
    parser.add_argument(
        '--plugins',
        type=str,
        help='Comma-separated list of specific plugins to validate (e.g., "basic_transformation,downsampler")'
    )
    
    parser.add_argument(
        '--list',
        action='store_true',
        help='List all available plugins and exit'
    )
    
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Show only errors, suppress warnings and success messages'
    )
    
    parser.add_argument(
        '--errors-only',
        action='store_true',
        help='(Deprecated: this is now the default behavior) Exit with success code even if warnings are found'
    )
    
    return parser.parse_args()

def list_available_plugins():
    """List all available plugins and exit."""
    influxdata_dir = Path('influxdata')
    
    if not influxdata_dir.exists():
        print("❌ Error: 'influxdata' directory not found. Run this script from the influxdb3_plugins root directory.")
        sys.exit(EXIT_ERROR)
    
    readme_files = list(influxdata_dir.glob('*/README.md'))
    
    if not readme_files:
        print("❌ No plugins found in influxdata/ subdirectories")
        sys.exit(EXIT_ERROR)
    
    print(f"Available plugins ({len(readme_files)} found):")
    for readme_path in sorted(readme_files):
        plugin_name = readme_path.parent.name
        print(f"  - {plugin_name}")
    
    sys.exit(EXIT_SUCCESS)

def filter_plugins_by_name(readme_files: List[Path], plugin_names: str) -> List[Path]:
    """Filter README files by specified plugin names."""
    requested_plugins = [name.strip() for name in plugin_names.split(',')]
    filtered_files = []
    
    for readme_path in readme_files:
        plugin_name = readme_path.parent.name
        if plugin_name in requested_plugins:
            filtered_files.append(readme_path)
            requested_plugins.remove(plugin_name)
    
    # Report any plugins that weren't found
    if requested_plugins:
        print(f"⚠️  Warning: The following plugins were not found: {', '.join(requested_plugins)}")
        available_plugins = [f.parent.name for f in readme_files]
        print(f"Available plugins: {', '.join(sorted(available_plugins))}")
    
    return filtered_files

def main():
    """Main validation function."""
    args = parse_arguments()
    
    # Handle list option
    if args.list:
        list_available_plugins()
    
    # Find all plugin READMEs
    influxdata_dir = Path('influxdata')
    
    if not influxdata_dir.exists():
        print("❌ Error: 'influxdata' directory not found. Run this script from the influxdb3_plugins root directory.")
        sys.exit(EXIT_ERROR)
    
    readme_files = list(influxdata_dir.glob('*/README.md'))
    
    if not readme_files:
        print("❌ No README files found in influxdata/ subdirectories")
        sys.exit(EXIT_ERROR)
    
    # Filter by specific plugins if requested
    if args.plugins:
        readme_files = filter_plugins_by_name(readme_files, args.plugins)
        if not readme_files:
            print("❌ No matching plugins found")
            sys.exit(EXIT_ERROR)
    
    if not args.quiet:
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
        
        result = format_validation_result(readme_path, errors, warnings)
        
        # Apply quiet mode filtering
        if args.quiet:
            # Only show files with errors in quiet mode
            if errors:
                print(result)
        else:
            print(result)
    
    # Print summary
    if not args.quiet:
        print("\n" + "=" * SUMMARY_SEPARATOR_LENGTH)
        print("VALIDATION SUMMARY")
        print("=" * SUMMARY_SEPARATOR_LENGTH)
        print(f"Total files validated: {len(readme_files)}")
        print(f"Errors found: {error_count}")
        print(f"Warnings found: {warning_count}")
    
    # Determine exit status
    has_errors = error_count > 0
    has_warnings = warning_count > 0
    
    if not has_errors and not has_warnings:
        if not args.quiet:
            print("\n✅ All README files are valid!")
        sys.exit(EXIT_SUCCESS)
    elif not has_errors and has_warnings:
        if not args.quiet:
            print(f"\n⚠️  Validation completed with {warning_count} warning(s) but no errors")
        # Exit successfully when there are only warnings (warnings are advisory, not blocking)
        sys.exit(EXIT_SUCCESS)
    else:
        if not args.quiet:
            print(f"\n❌ Validation failed with {error_count} error(s)")
            if has_warnings:
                print(f"Also found {warning_count} warning(s)")
            print("\nPlease fix the errors above and ensure all READMEs follow the template.")
            print("See README_TEMPLATE.md for the correct structure.")
        sys.exit(EXIT_ERROR)

if __name__ == "__main__":
    main()