# GoStore CLI

A command-line interface for managing GoStore databases.

## Prerequisites

- Go 1.24.0 or higher
- Git (for version tagging)

## Project Structure

This is a multi-module Go workspace. The CLI depends on other modules in the workspace:

```
gostore/
├── cli/          # This CLI module
├── common/       # Common types and interfaces
├── indexer/      # Indexing functionality
└── stores/badger # BadgerDB store implementation
```

## Building

### Build with Development Version

```bash
cd /path/to/gostore
go build -o gostore-cli ./cli
```

### Build with Specific Version

To embed a specific version in the binary:

```bash
go build -ldflags="-X github.com/osiloke/gostore/cli/cmd.VERSION=v1.0.0" -o gostore-cli ./cli
```

### Build Using Makefile

From the repository root:

```bash
make build
```

This builds the CLI with version information from git tags.

## Installation

### Quick Install (Development)

```bash
go install ./cli
```

### Install with Version Tag

1. **Tag the CLI module** with a new version:

   ```bash
   ./tag-one.sh ./cli
   ```

   This script:
   - Analyzes commits since the last tag
   - Determines the next semantic version (following Conventional Commits)
   - Creates a tag like `cli/v1.0.0`
   - Pushes the tag to the remote

2. **Build with the tagged version**:

   ```bash
   VERSION=$(./get-next-version.sh ./cli | sed 's/v//')
   go build -ldflags="-X github.com/osiloke/gostore/cli/cmd.VERSION=v$VERSION" -o gostore-cli ./cli
   ```

3. **Install globally**:

   ```bash
   make install
   ```

### Using GoReleaser (Recommended for Releases)

[GoReleaser](https://goreleaser.com/) automates building and releasing:

```bash
# Install goreleaser
go install github.com/goreleaser/goreleaser@latest

# Build snapshot (no tags needed)
goreleaser build --snapshot --clean

# Full release (requires git tag)
goreleaser release --clean
```

## Version Management

### Check CLI Version

```bash
./gostore-cli version
```

### Tag All Modules

To tag all workspace modules with the same version:

```bash
./tag-all.sh
```

### Get Next Version

To preview the next version without creating a tag:

```bash
./get-next-version.sh ./cli
```

### Manual Tagging

For manual version tagging:

```bash
# Create a tag for CLI module
git tag cli/v1.0.0

# Push the tag
git push origin cli/v1.0.0
```

## Usage

### Database Commands

```bash
# List all records in a store
./gostore-cli db list -p ./db -s mystore -c 100

# Get a specific record
./gostore-cli db get mykey -p ./db -s mystore

# Create a new record
./gostore-cli db create -p ./db -s mystore -d '{"name":"test"}'

# Count records in a store
./gostore-cli db count mystore -p ./db

# List keys in a store
./gostore-cli db keys mystore -p ./db
```

### Common Flags

- `-p, --path`: Path to GoStore data folder (default: `./db`)
- `-t, --type`: Store type (default: `BADGER`)
- `-s, --store`: Store name (default: `_test`)

### List Command Flags

- `-c, --count`: Maximum number of rows to return (`-1` for all, default: `-1`)
- `-o, --output`: Output format: `json` or `csv` (default: `json`)

## Examples

### Export Store Data to JSON

```bash
./gostore-cli db list -p ./production-db -s users -c -1 -o json
```

### Export Store Data to CSV

```bash
./gostore-cli db list -p ./production-db -s products -c 1000 -o csv
```

### Backup and Restore

```bash
# Create backup
./gostore-cli db backup -p ./source-db -o ./backup-dir

# Restore from backup
./gostore-cli db restore -p ./new-db -i ./backup-dir
```

## Development

### Running Tests

```bash
cd cli
go test -v ./...
```

### Workspace Setup

Ensure you're in the workspace root and run:

```bash
go work sync
```

## License

Apache License 2.0 - See [LICENSE](../LICENSE) for details.
