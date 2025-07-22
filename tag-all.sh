#!/bin/bash

# This script creates and pushes version tags for all Go modules in the workspace.
# It reads the module paths from the go.work file.

set -e # Exit immediately if a command exits with a non-zero status.

# --- Configuration ---
# The remote repository name
REMOTE_NAME="origin"

# --- Script Logic ---

# Get the next version from the get-next-version.sh script
echo "Determining next version..."
VERSION=$(./get-next-version.sh)

if [ -z "$VERSION" ]; then
    echo "Error: Could not determine next version."
    exit 1
fi

echo "Preparing to tag all modules with version $VERSION..."

# Find all module paths from go.work, skipping comments, 'use (', and ')' lines
# and removing leading/trailing whitespace.
MODULES=$(grep -E '^\s*\./' go.work | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')

if [ -z "$MODULES" ]; then
    echo "Error: Could not find any modules in go.work file."
    exit 1
fi

echo "Found modules:"
echo "$MODULES"
echo ""

# Loop through each module and create a tag
for module_path in $MODULES; do
  # Remove the leading './'
  clean_path=${module_path#./}

  tag_name="$clean_path/$VERSION"

  echo "Creating tag: $tag_name"
  git tag "$tag_name"
done

echo ""
echo "All local tags created. Pushing to remote '$REMOTE_NAME'..."

# Push all tags to the remote
git push "$REMOTE_NAME" --tags

echo ""
echo "Successfully pushed all tags."