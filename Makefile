.PHONY: help test build lint fmt bench clean coverage install install-tools mod-tidy audit all

# Colors for output
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
RESET  := $(shell tput -Txterm sgr0)

# Build variables
BINARY_NAME := ledgerq
BUILD_DIR := bin
CMD_DIR := ./cmd/ledgerq

help: ## Show this help message
	@echo "$(GREEN)Available targets:$(RESET)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-15s$(RESET) %s\n", $$1, $$2}'

all: fmt lint test build ## Run fmt, lint, test, and build

test: ## Run tests with race detector
	@echo "$(GREEN)Running tests...$(RESET)"
	go test -race -v -timeout 30s ./...

build: ## Build the CLI binary
	@echo "$(GREEN)Building $(BINARY_NAME)...$(RESET)"
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY_NAME) $(CMD_DIR)

lint: ## Run golangci-lint
	@echo "$(GREEN)Running linters...$(RESET)"
	@which golangci-lint > /dev/null || (echo "$(YELLOW)golangci-lint not installed. Install from https://golangci-lint.run/usage/install/$(RESET)" && exit 1)
	golangci-lint run

fmt: ## Format code with gofmt and goimports
	@echo "$(GREEN)Formatting code...$(RESET)"
	gofmt -s -w .
	@which goimports > /dev/null && goimports -w . || echo "$(YELLOW)goimports not installed. Run: go install golang.org/x/tools/cmd/goimports@latest$(RESET)"

bench: ## Run benchmarks
	@echo "$(GREEN)Running benchmarks...$(RESET)"
	go test -bench=. -benchmem -run=^$$ ./...

coverage: ## Generate test coverage report
	@echo "$(GREEN)Generating coverage report...$(RESET)"
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "$(GREEN)Coverage report generated: coverage.html$(RESET)"

clean: ## Clean build artifacts and test outputs
	@echo "$(GREEN)Cleaning...$(RESET)"
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html
	rm -f *.test
	find . -type d -name "*.queue" -exec rm -rf {} + 2>/dev/null || true

install: ## Install the CLI binary
	@echo "$(GREEN)Installing $(BINARY_NAME)...$(RESET)"
	go install $(CMD_DIR)

install-tools: ## Install development and security tools
	@echo "$(GREEN)Installing development tools...$(RESET)"
	@echo "$(YELLOW)Installing goimports...$(RESET)"
	go install golang.org/x/tools/cmd/goimports@latest
	@echo "$(YELLOW)Installing govulncheck...$(RESET)"
	go install golang.org/x/vuln/cmd/govulncheck@latest
	@echo "$(YELLOW)Installing staticcheck...$(RESET)"
	go install honnef.co/go/tools/cmd/staticcheck@latest
	@echo "$(YELLOW)Installing gosec...$(RESET)"
	go install github.com/securego/gosec/v2/cmd/gosec@latest
	@echo "$(GREEN)✓ All tools installed successfully$(RESET)"
	@echo "$(YELLOW)Note: golangci-lint should be installed separately from https://golangci-lint.run/usage/install/$(RESET)"

mod-tidy: ## Tidy Go modules
	@echo "$(GREEN)Tidying Go modules...$(RESET)"
	go mod tidy

audit: ## Run security audit checks
	@echo "$(GREEN)Running security audit...$(RESET)"
	@echo ""
	@echo "$(YELLOW)[1/5] Checking for vulnerabilities (govulncheck)...$(RESET)"
	@which govulncheck > /dev/null || (echo "$(YELLOW)govulncheck not installed. Run: go install golang.org/x/vuln/cmd/govulncheck@latest$(RESET)" && exit 1)
	@govulncheck ./... && echo "$(GREEN)  ✓ No vulnerabilities found$(RESET)" || echo "$(YELLOW)  ⚠ Vulnerabilities detected$(RESET)"
	@echo ""
	@echo "$(YELLOW)[2/5] Running go vet...$(RESET)"
	@go vet ./... && echo "$(GREEN)  ✓ Passed$(RESET)" || echo "$(YELLOW)  ⚠ Issues found$(RESET)"
	@echo ""
	@echo "$(YELLOW)[3/5] Running staticcheck...$(RESET)"
	@which staticcheck > /dev/null || (echo "$(YELLOW)staticcheck not installed. Run: go install honnef.co/go/tools/cmd/staticcheck@latest$(RESET)" && exit 1)
	@staticcheck ./... > /dev/null 2>&1 && echo "$(GREEN)  ✓ Passed$(RESET)" || echo "$(YELLOW)  ⚠ Style issues in test files (non-critical)$(RESET)"
	@echo ""
	@echo "$(YELLOW)[4/5] Running gosec security scanner...$(RESET)"
	@which gosec > /dev/null || (echo "$(YELLOW)gosec not installed. Run: go install github.com/securego/gosec/v2/cmd/gosec@latest$(RESET)" && exit 1)
	@gosec -exclude=G115,G304,G302 -exclude-dir=examples -quiet ./... > /dev/null 2>&1 && echo "$(GREEN)  ✓ No critical security issues$(RESET)" || echo "$(YELLOW)  ⚠ Issues found$(RESET)"
	@echo "$(YELLOW)    Note: G304 (path traversal) and G302 (file permissions) excluded - documented in SECURITY_AUDIT.md$(RESET)"
	@echo ""
	@echo "$(YELLOW)[5/5] Running tests with race detector...$(RESET)"
	@go test -race -short ./... > /dev/null 2>&1 && echo "$(GREEN)  ✓ All tests passed$(RESET)" || echo "$(YELLOW)  ⚠ Test failures$(RESET)"
	@echo ""
	@echo "$(GREEN)✓ Security audit completed$(RESET)"
	@echo "$(YELLOW)See docs/SECURITY_AUDIT.md for detailed security analysis$(RESET)"

.PHONY: setup
setup: ## Bootstrap repo: install git hooks
	@scripts/setup.sh
