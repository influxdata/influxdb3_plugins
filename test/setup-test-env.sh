#!/bin/bash
# Setup script for InfluxDB 3 plugins test environment

echo "Setting up Python virtual environment for testing..."

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Create virtual environment in project root if it doesn't exist
if [ ! -d "$PROJECT_ROOT/venv" ]; then
    echo "Creating virtual environment in project root..."
    python3 -m venv "$PROJECT_ROOT/venv"
fi

# Activate virtual environment
echo "Activating virtual environment..."
source "$PROJECT_ROOT/venv/bin/activate"

# Upgrade pip
echo "Upgrading pip..."
python -m pip install --upgrade pip

# Install requirements from test directory
echo "Installing requirements..."
pip install -r "$SCRIPT_DIR/requirements.txt"

echo ""
echo "Setup complete! To run tests:"
echo "  source venv/bin/activate"
echo "  python test/test_plugins.py influxdata --core"
echo ""
echo "Or run directly with:"
echo "  venv/bin/python test/test_plugins.py influxdata --core"