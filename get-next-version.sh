#!/bin/bash

# This script determines the next semantic version based on Conventional Commits
# since the last tag.

set -e

MODULE_PATH=$1

# If a module path is provided, find the latest tag for that module.
if [ -n "$MODULE_PATH" ]; then
  # Clean module path, removing leading './'
  CLEAN_PATH=${MODULE_PATH#./}
  LATEST_TAG=$(git tag --list "$CLEAN_PATH/v*" | sort -V | tail -n 1)
else
  # Fallback to the original behavior if no module path is given
  LATEST_TAG=$(git tag | grep -o 'v[0-9]\+\.[0-9]\+\.[0-9]\+.*$' | sort -V | tail -n 1)
fi

if [ -z "$LATEST_TAG" ]; then
  echo "No previous version found. Starting with v0.1.0." >&2
  NEXT_VERSION="v0.1.0"
else
  echo "Latest tag detected: $LATEST_TAG" >&2
  # Extract version from tag (e.g., pool/v1.0.1 -> v1.0.1)
  LATEST_VERSION=$(echo $LATEST_TAG | grep -o 'v[0-9]\+\.[0-9]\+\.[0-9]\+.*$')
  echo "Latest version detected: $LATEST_VERSION" >&2

  LATEST_TAG_COMMIT=$(git rev-list -n 1 $LATEST_TAG)

  # If a module path is provided, only look at commits in that directory
  if [ -n "$MODULE_PATH" ]; then
    COMMITS=$(git log $LATEST_TAG_COMMIT..HEAD --oneline -- $MODULE_PATH)
  else
    COMMITS=$(git log $LATEST_TAG_COMMIT..HEAD --oneline)
  fi
  echo "Analyzing commits since last tag..." >&2

  # Determine the type of change
  BUMP_TYPE="patch" # Default bump
  if echo "$COMMITS" | grep -q "BREAKING CHANGE"; then
    BUMP_TYPE="major"
  elif echo "$COMMITS" | grep -qE "^[a-f0-9]+ feat(\(.*\))?:"; then
    BUMP_TYPE="minor"
  elif echo "$COMMITS" | grep -qE "^[a-f0-9]+ fix(\(.*\))?:"; then
    BUMP_TYPE="patch"
  fi
  echo "Change type detected: $BUMP_TYPE" >&2

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