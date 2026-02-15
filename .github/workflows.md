# GitHub Workflows and CI/CD

This directory contains GitHub Actions workflows for continuous integration, releases, and security scanning.

## Workflows

### CI Workflow (`ci.yml`)

Runs on every push to `main` and on all pull requests.

**Jobs:**

1. **Test** - Cross-platform testing
   - Runs on: Linux, macOS, Windows
   - Go versions: 1.23, 1.24, 1.25
   - Executes: Unit tests with and without race detector
   - Timeout: 5-10 minutes per test run

2. **Coverage** - Code coverage analysis
   - Runs on: Ubuntu (latest Go version)
   - Generates coverage report
   - Uploads to Codecov (requires `CODECOV_TOKEN` secret)

3. **Lint** - Code quality checks
   - Runs golangci-lint with project configuration
   - Checks: govet, errcheck, staticcheck, unused, security, complexity
   - Timeout: 10 minutes

4. **Build** - Multi-platform builds
   - Builds library and CLI on Linux, macOS, Windows
   - Verifies CLI works by running `version` command

5. **Fuzz** - Fuzzing tests (main branch only)
   - Runs fuzzing on entry format parser
   - Runs fuzzing on queue operations
   - Duration: 30 seconds per fuzz target

6. **Examples** - Example validation
   - Builds all example programs
   - Ensures examples compile without errors

7. **Benchmark** - Performance tracking (main branch only)
   - Runs benchmarks on internal/queue
   - Uploads results as artifacts (30-day retention)

### Release Workflow (`release.yml`)

Triggers on version tags (e.g., `v1.0.0`).

**Process:**
1. Runs full test suite
2. Builds binaries for multiple platforms:
   - Linux (amd64, arm64)
   - macOS (amd64, arm64)
   - Windows (amd64)
3. Generates SHA256 checksums
4. Extracts release notes from CHANGELOG.md
5. Creates GitHub release with binaries

**Creating a Release:**
```bash
git tag -a v1.0.0 -m "Release v1.0.0"
git push origin v1.0.0
```

### CodeQL Workflow (`codeql.yml`)

Security analysis and vulnerability scanning.

**Schedule:**
- On push to main
- On pull requests
- Weekly on Mondays at 00:00 UTC

**Analysis:**
- Language: Go
- Queries: security-and-quality
- Results uploaded to GitHub Security tab

## Dependabot

Automated dependency updates configured in `dependabot.yml`.

**Updates:**
- Go modules (weekly on Monday)
- GitHub Actions (weekly on Monday)
- Maximum 5 open PRs per ecosystem

## Local Testing

Before pushing, run these locally:

```bash
# Run tests
go test ./...

# Run tests with race detector
go test -race ./...

# Run linter
golangci-lint run

# Run fuzzing (30 seconds)
go test -fuzz=FuzzEntry -fuzztime=30s ./internal/format
go test -fuzz=FuzzEnqueueDequeue -fuzztime=30s ./internal/queue

# Run benchmarks
go test -bench=. -benchmem ./internal/queue
```

## Required Secrets

Configure these in repository settings:

- `CODECOV_TOKEN` (optional) - For code coverage uploads
- `GITHUB_TOKEN` (automatic) - For releases and CodeQL

## Badge Examples

Add to README.md:

```markdown
[![CI](https://github.com/1mb-dev/ledgerq/workflows/CI/badge.svg)](https://github.com/1mb-dev/ledgerq/actions?query=workflow%3ACI)
[![codecov](https://codecov.io/gh/1mb-dev/ledgerq/branch/main/graph/badge.svg)](https://codecov.io/gh/1mb-dev/ledgerq)
[![Go Report Card](https://goreportcard.com/badge/github.com/1mb-dev/ledgerq)](https://goreportcard.com/report/github.com/1mb-dev/ledgerq)
[![GoDoc](https://pkg.go.dev/badge/github.com/1mb-dev/ledgerq)](https://pkg.go.dev/github.com/1mb-dev/ledgerq/pkg/ledgerq)
```

## Workflow Optimization

**Caching:**
- Go modules cached using `actions/setup-go@v5` with `cache: true`
- Significantly reduces build times

**Parallelization:**
- Matrix builds run in parallel
- Independent jobs run concurrently
- Typical full CI run: ~10-15 minutes

**Resource Usage:**
- Fuzz and benchmark jobs only run on main branch
- Coverage only on one platform
- Minimizes GitHub Actions minutes usage
