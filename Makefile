APP_NAME := messaging_service
GO ?= go
COVERAGE_THRESHOLD ?= 80

.PHONY: install
install: ## Download module dependencies
	$(GO) mod download

.PHONY: build
build: ## Build the server binary
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="-s -w" -o bin/$(APP_NAME) ./cmd/server

.PHONY: run
run: ## Run the service locally
	$(GO) run ./cmd/server

.PHONY: test
test: ## Run the whole test suite (no network, database or broker needed)
	$(GO) test ./... -race -count=1 -parallel 4

.PHONY: cover
cover: ## Run tests with coverage across internal packages and enforce the threshold
	$(GO) test ./... -race -count=1 -parallel 4 -covermode=atomic \
		-coverpkg=./internal/... -coverprofile=coverage.out
	@total=$$($(GO) tool cover -func=coverage.out | awk '/^total:/ {print $$3}' | tr -d '%'); \
	echo "total coverage: $$total% (threshold $(COVERAGE_THRESHOLD)%)"; \
	awk -v total="$$total" -v threshold="$(COVERAGE_THRESHOLD)" 'BEGIN { if (total+0 < threshold+0) { exit 1 } }'

.PHONY: lint
lint: ## Format check and vet
	@test -z "$$(gofmt -l cmd internal tests)" || (gofmt -l cmd internal tests; exit 1)
	$(GO) vet ./...

.PHONY: docker-build
docker-build: ## Build the container image
	docker build -t $(APP_NAME):local .

.PHONY: clean
clean: ## Remove build artefacts
	rm -rf bin coverage.out

.PHONY: help
help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "%-16s %s\n", $$1, $$2}'
