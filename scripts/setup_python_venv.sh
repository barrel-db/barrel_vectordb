#!/bin/bash
# Setup Python virtualenv for barrel_vectordb testing
#
# This script creates a project-local virtualenv in .venv/ and installs
# the required dependencies for reranking tests.
#
# Usage:
#   ./scripts/setup_python_venv.sh
#
# The venv is automatically detected by the integration tests.

set -e

VENV_DIR=".venv"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

# Create venv if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtualenv in $VENV_DIR..."
    python3 -m venv "$VENV_DIR"
fi

# Activate and install dependencies
source "$VENV_DIR/bin/activate"

echo "Installing dependencies..."
pip install --upgrade pip
pip install transformers torch sentence-transformers requests

# Optional: uvloop for better async performance
pip install uvloop 2>/dev/null || echo "Note: uvloop not available (optional, improves async performance)"

echo ""
echo "=========================================="
echo "Setup complete!"
echo "=========================================="
echo ""
echo "Virtualenv location: $PROJECT_DIR/$VENV_DIR"
echo "Python executable:   $PROJECT_DIR/$VENV_DIR/bin/python"
echo ""
echo "To activate manually:"
echo "  source $VENV_DIR/bin/activate"
echo ""
echo "To run integration tests:"
echo "  rebar3 eunit --module=barrel_vectordb_rerank_integration_tests"
