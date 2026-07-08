#!/bin/bash

# This script creates and pushes version tags for all Go modules in the workspace.
# It reads the module paths from the go.work file.

set -e # Exit immediately if a command exits with a non-zero status.

# --- Configuration ---
# The remote repository name
REMOTE_NAME="origin"

# --- Script Logic ---

ALLOW_MAJOR_ARG=""

help_message() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "This script creates and pushes version tags for all Go modules in the workspace."
    echo ""
    echo "Options:"
    echo "  --allow-major    Allow major semantic version updates (breaking changes)"
    echo "  -h, --help       Display this help message"
    echo ""
    exit 0
}

for arg in "$@"; do
  if [ "$arg" = "--allow-major" ]; then
    ALLOW_MAJOR_ARG="--allow-major"
  elif [[ "$arg" == "-h" || "$arg" == "--help" ]]; then
    help_message
  fi
done

# Fetch the latest tags from the remote to avoid conflicts
echo "Fetching latest tags from remote '$REMOTE_NAME'..."
git fetch "$REMOTE_NAME" --tags
echo ""

# Get the next version from the get-next-version.sh script
echo "Determining next version..."
VERSION=$(./get-next-version.sh $ALLOW_MAJOR_ARG)

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
TAGS_TO_CREATE=()
for module_path in $MODULES; do
  # Remove the leading './'
  clean_path=${module_path#./}
  tag_name="$clean_path/$VERSION"

  # Check if the tag already exists locally or remotely
  if git rev-parse -q --verify "refs/tags/$tag_name" >/dev/null; then
    echo "Warning: Tag '$tag_name' already exists. Skipping."
  else
    echo "Preparing to create tag: $tag_name"
    TAGS_TO_CREATE+=("$tag_name")
  fi
done

if [ ${#TAGS_TO_CREATE[@]} -eq 0 ]; then
  echo ""
  echo "No new tags to create. Exiting."
  exit 0
fi

echo ""
echo "The following tags will be created:"
for tag in "${TAGS_TO_CREATE[@]}"; do
  echo "  - $tag"
done
echo ""

# Ask for confirmation before creating and pushing tags
read -p "Do you want to create these tags and push them to '$REMOTE_NAME'? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted by user."
    exit 1
fi

echo ""
echo "Creating local tags..."
for tag_name in "${TAGS_TO_CREATE[@]}"; do
  echo "Creating tag: $tag_name"
  git tag "$tag_name"
done

echo ""
echo "All local tags created. Pushing to remote '$REMOTE_NAME'..."

# Push all tags to the remote
git push "$REMOTE_NAME" --tags

echo ""
echo "Successfully pushed all tags."
