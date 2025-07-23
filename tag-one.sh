#!/bin/bash

# This script creates and pushes a version tag for a single Go module.

set -e # Exit immediately if a command exits with a non-zero status.

# --- Configuration ---
# The remote repository name
REMOTE_NAME="origin"

# --- Script Logic ---

# Check if a module path is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <module-path>"
    echo "Example: $0 ./pool"
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