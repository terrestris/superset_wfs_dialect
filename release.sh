#!/usr/bin/env bash

set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: ./release.sh <version>"
  echo "Example: ./release.sh 0.0.1"
  exit 1
fi

VERSION="$1"
TAG="v$VERSION"

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Version must be in the format X.Y.Z"
  exit 1
fi

CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ "$CURRENT_BRANCH" != "main" ]; then
  echo "You must be on the 'main' branch to run this script (current: $CURRENT_BRANCH)."
  exit 1
fi

if [[ -n $(git status --porcelain) ]]; then
  echo "Your working directory is not clean. Please commit or stash your changes."
  exit 1
fi

echo "You are about to release version: $VERSION (PyPI)"
read -p "Do you want to continue? [y/N]: " CONFIRM
if [[ "$CONFIRM" != "y" && "$CONFIRM" != "Y" ]]; then
  echo "Release aborted."
  exit 1
fi

echo "Updating version to $VERSION in setup.py..."

sed -i.bak -E "s/^(\s*version\s*=\s*\").*?(\",?)/\1$VERSION\2/" setup.py
rm setup.py.bak

echo "Committing and tagging..."

git add setup.py
git commit -m "Release $VERSION"
git push

git tag "$TAG"
git push origin "$TAG"

echo "Released version $VERSION as tag $TAG"
