#!/bin/bash

# This script creates and pushes a version tag for a single Go module.

set -e # Exit immediately if a command exits with a non-zero status.

# --- Configuration ---
# The remote repository name
REMOTE_NAME="origin"

# --- Script Logic ---

# --- Script Logic ---

# Function to display help message
help_message() {
    echo "Usage: $0 <module-path> [OPTIONS]"
    echo ""
    echo "This script creates and pushes a version tag for a single Go module."
    echo ""
    echo "Arguments:"
    echo "  <module-path>    The path to the Go module (e.g., ./pool, ./cli)"
    echo ""
    echo "Options:"
    echo "  -h, --help       Display this help message"
    echo ""
    echo "Examples:"
    echo "  $0 ./pool"
    echo "  $0 ./cli"
    exit 0
}

# Check for help command
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    help_message
fi

# Check if a module path is provided
if [ -z "$1" ]; then
    echo "Error: Missing <module-path> argument."
    echo "Use '$0 --help' for more information."
    exit 1
fi

MODULE_PATH=$1

# Get the next version from the get-next-version.sh script
echo "Determining next version for module '$MODULE_PATH'..."
VERSION=$(./get-next-version.sh $MODULE_PATH)

if [ -z "$VERSION" ]; then
    echo "Error: Could not determine next version."
    exit 1
fi

echo "Preparing to tag module '$MODULE_PATH' with version $VERSION..."

# Remove the leading './' if it exists
CLEAN_PATH=${MODULE_PATH#./}

TAG_NAME="$CLEAN_PATH/$VERSION"

echo "Creating tag: $TAG_NAME"
git tag "$TAG_NAME"

echo ""
echo "Local tag created. Pushing to remote '$REMOTE_NAME'..."

# Push the new tag to the remote
git push "$REMOTE_NAME" "$TAG_NAME"

echo ""
echo "Successfully pushed tag $TAG_NAME."