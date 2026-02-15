# Contributing to LedgerQ

**Last Updated:** 2025-10-20

Thank you for your interest in contributing to LedgerQ! We welcome contributions from the community.

## Code of Conduct

This project adheres to a [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## New Contributor Checklist

For your first contribution:

- [ ] Fork the repo and clone locally
- [ ] Run `make install-tools` to install golangci-lint and security scanners
- [ ] Run `make test` to verify all tests pass on your machine
- [ ] Read [ARCHITECTURE.md](docs/ARCHITECTURE.md) to understand internal structure
- [ ] Pick a ["good first issue"](https://github.com/1mb-dev/ledgerq/labels/good%20first%20issue) or discuss new features in an issue first
- [ ] Create your feature branch from `main`
- [ ] Make changes + add tests (aim for >80% coverage)
- [ ] Run `make all` before committing (runs fmt, lint, test, build)
- [ ] Write a clear commit message following our guidelines
- [ ] Open pull request with description of changes

## How to Contribute

### Reporting Bugs

If you find a bug, please open an issue with:
- A clear description of the problem
- Steps to reproduce the issue
- Expected vs actual behavior
- Your environment (Go version, OS, etc.)

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, please include:
- A clear description of the proposed feature
- Why this enhancement would be useful
- Examples of how it would be used

### Pull Requests

1. **Fork the repository** and create your branch from `main`
2. **Make your changes** following our coding standards
3. **Add tests** for any new functionality
4. **Ensure all tests pass** (`make test`)
5. **Run the linter** (`make lint`) and fix any issues
6. **Format your code** (`make fmt`)
7. **Update documentation** if needed
8. **Write a clear commit message** describing your changes
9. **Submit your pull request**

## Development Setup

### Prerequisites

- Go 1.21 or later
- golangci-lint (install from https://golangci-lint.run/usage/install/)

### Getting Started

```bash
# Clone your fork
git clone https://github.com/YOUR-USERNAME/ledgerq.git
cd ledgerq

# Download dependencies
go mod download

# Install development and security tools
make install-tools

# Run tests
make test

# Run linter
make lint

# Format code
make fmt
```

## Development Workflow

### Running Tests

```bash
# Run all tests
make test

# Run tests with coverage
make coverage

# Run benchmarks
make bench

# Run tests with race detector (automatically enabled in make test)
go test -race -v ./...
```

### Building

```bash
# Build the CLI tool
make build

# The binary will be in bin/ledgerq
./bin/ledgerq --help
```

### Code Quality

We use several tools to maintain code quality:

- **golangci-lint** — Comprehensive linting (configured in `.golangci.yml`)
- **gofmt** — Code formatting
- **goimports** — Import organization
- **go vet** — Static analysis

Run all checks:
```bash
make all  # Runs fmt, lint, test, and build
```

## Coding Standards

### Go Style

- Follow [Effective Go](https://golang.org/doc/effective_go.html)
- Follow the [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- Use `gofmt` for formatting (tabs for indentation)
- Keep functions focused and concise
- Write clear, descriptive variable names

### Documentation

- All exported types, functions, and constants must have godoc comments
- Comments should start with the name of the element being described
- Use complete sentences with proper punctuation
- Include examples for complex functionality

Example:
```go
// Queue represents a durable, file-backed message queue.
// It provides crash-safe persistence with replay capabilities.
type Queue struct {
    // ...
}

// Open creates or opens an existing queue at the specified directory.
// If the directory doesn't exist, it will be created.
func Open(dir string, opts ...Option) (*Queue, error) {
    // ...
}
```

### Testing

- Write tests for all new functionality
- Use table-driven tests where appropriate
- Test edge cases and error conditions
- Aim for high test coverage (>80%)
- Use meaningful test names: `TestFunctionName_Scenario_ExpectedResult`

Example:
```go
func TestQueue_Enqueue_Success(t *testing.T) {
    // ...
}

func TestQueue_Dequeue_EmptyQueue_ReturnsError(t *testing.T) {
    // ...
}
```

### Error Handling

- Return errors, don't panic (except in truly exceptional cases)
- Use sentinel errors for expected error conditions
- Wrap errors with context using `fmt.Errorf("context: %w", err)`
- Check all errors, even in tests

### Commits

Write clear, concise commit messages:
- Use the imperative mood ("Add feature" not "Added feature")
- First line: brief summary (50 characters or less)
- Blank line, then detailed explanation if needed
- Reference issues: "Fixes #123" or "Related to #456"

Example:
```
Add batch dequeue functionality

Implement DequeueBatch method to efficiently retrieve multiple
messages in a single operation. This reduces I/O overhead for
high-throughput consumers.

Fixes #42
```

## Pull Request Process

1. **Update documentation** — Ensure README and godoc are up to date
2. **Update CHANGELOG.md** — Add your changes under "Unreleased"
3. **Ensure CI passes** — All tests and lints must pass
4. **Request review** — Tag maintainers for review
5. **Address feedback** — Make requested changes
6. **Squash commits** — Rebase and squash if requested
7. **Merge** — Maintainers will merge after approval

### PR Title Format

Use conventional commit prefixes:
- `feat:` — New feature
- `fix:` — Bug fix
- `docs:` — Documentation changes
- `test:` — Test additions or changes
- `refactor:` — Code refactoring
- `perf:` — Performance improvements
- `chore:` — Maintenance tasks

Example: `feat: add timestamp-based replay support`

## Project Structure

```
ledgerq/
├── cmd/ledgerq/           # CLI tool
├── pkg/ledgerq/           # Public API package
├── internal/              # Private implementation packages
│   ├── segment/          # Segment file management
│   ├── format/           # Entry and index format
│   ├── queue/            # Core queue implementation
│   ├── metrics/          # Metrics collection
│   └── logging/          # Logging interfaces
├── examples/             # Usage examples
├── .github/workflows/    # CI/CD configuration
└── README.md, LICENSE    # Documentation and licensing
```

### Internal Packages

Code in `internal/` is private to the project. Only put code there if:
- It's an implementation detail
- It shouldn't be imported by external projects
- It might change frequently

Public API is in `pkg/ledgerq/` for proper Go module structure.

## Performance Considerations

- Minimize allocations in hot paths
- Use `sync.Pool` for frequently allocated objects
- Profile before optimizing (`make bench`)
- Document performance characteristics in godoc

## Questions?

If you have questions about contributing, feel free to:
- Open an issue with the question
- Start a discussion in GitHub Discussions
- Reach out to maintainers

Thank you for contributing to LedgerQ!
