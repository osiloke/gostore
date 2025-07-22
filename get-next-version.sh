#!/bin/bash

# This script determines the next semantic version based on Conventional Commits
# since the last tag.

set -e

# Find the latest version number from all tags, including prefixed ones.
LATEST_VERSION=$(git tag | grep -o 'v[0-9]\+\.[0-9]\+\.[0-9]\+.*$' | sort -V | tail -n 1)

if [ -z "$LATEST_VERSION" ]; then
  echo "No previous version found. Starting with v0.1.0."
  NEXT_VERSION="v0.1.0"
else
  echo "Latest version detected: $LATEST_VERSION"

  # Find the commit hash where the latest version was tagged. This is complex with prefixed tags.
  # We'll find all commits that have a tag with the latest version number.
  LATEST_TAG_COMMIT=$(git rev-list -n 1 tags/$LATEST_VERSION 2>/dev/null || git rev-list -n 1 $LATEST_VERSION 2>/dev/null)

  # A simpler, more robust approach is to find the most recent commit that has ANY tag.
  LATEST_TAG_COMMIT=$(git rev-list --tags --max-count=1)

  COMMITS=$(git log $LATEST_TAG_COMMIT..HEAD --oneline)
  echo "Analyzing commits since last tag..."

  # Determine the type of change
  BUMP_TYPE="patch" # Default bump
  if echo "$COMMITS" | grep -q "BREAKING CHANGE"; then
    BUMP_TYPE="major"
  elif echo "$COMMITS" | grep -qE "^[a-f0-9]+ feat(\(.*\))?:"; then
    BUMP_TYPE="minor"
  elif echo "$COMMITS" | grep -qE "^[a-f0-9]+ fix(\(.*\))?:"; then
    BUMP_TYPE="patch"
  fi
  echo "Change type detected: $BUMP_TYPE"

  # Increment version
  MAJOR=$(echo $LATEST_VERSION | cut -d. -f1 | sed 's/v//')
  MINOR=$(echo $LATEST_VERSION | cut -d. -f2)
  PATCH=$(echo $LATEST_VERSION | cut -d. -f3 | sed 's/-.*//') # handle pre-release tags

  case "$BUMP_TYPE" in
    "major")
      MAJOR=$((MAJOR + 1))
      MINOR=0
      PATCH=0
      ;;
    "minor")
      MINOR=$((MINOR + 1))
      PATCH=0
      ;;
    "patch")
      PATCH=$((PATCH + 1))
      ;;
  esac
  NEXT_VERSION="v$MAJOR.$MINOR.$PATCH"
fi

echo "$NEXT_VERSION"