#!/bin/bash
# Setup script for InfluxDB 3 plugins test environment

echo "Setting up Python virtual environment for testing..."

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
python -m pip install --upgrade pip

# Install requirements
echo "Installing requirements..."
pip install -r requirements.txt

echo ""
echo "Setup complete! To run tests:"
echo "  source venv/bin/activate"
echo "  python test-plugins.py influxdata --core"
echo ""
echo "Or run directly with:"
echo "  venv/bin/python test-plugins.py influxdata --core"