# Configuration
IMAGE_NAME := rs_app/distributed-transformer
VERSION := $(shell cat rs_app/VERSION 2>/dev/null || echo "0.1.0")
GIT_COMMIT := $(shell git rev-parse --short HEAD)
IMAGE_TAG ?= $(VERSION)-$(GIT_COMMIT)

# Load environment variables from .env
include rs_app/.env
export

# Kubernetes/Argo configuration
INPUT_URL ?= s3://$(S3_BUCKET_NAME)/input_data.parquet
OUTPUT_URL ?= s3://$(S3_BUCKET_NAME)/output_data.parquet
SQL_FILTER ?= "SELECT * FROM data"
K8S_NAMESPACE ?= default

# Test configuration
TEST_S3_BUCKET ?= test-bucket
TEST_AZURE_CONTAINER ?= test-container

# Colors for output
GREEN := \033[0;32m
RED := \033[0;31m
YELLOW := \033[0;33m
NC := \033[0m

.PHONY: all build push run clean test fmt check help version k8s-deploy k8s-delete test-local test-s3 test-azure lint coverage

all: check fmt lint test build ## Build and test everything

help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

version: ## Display version information
	@echo "Version: $(VERSION)"
	@echo "Git commit: $(GIT_COMMIT)"
	@echo "Image tag: $(IMAGE_TAG)"
	@echo "S3 Bucket: $(S3_BUCKET_NAME)"
	@echo "AWS Region: $(AWS_REGION)"

check: ## Check if all required tools are installed
	@command -v docker >/dev/null 2>&1 || { echo "$(RED)docker is required but not installed. Aborting.$(NC)" >&2; exit 1; }
	@command -v argo >/dev/null 2>&1 || { echo "$(RED)argo is required but not installed. Aborting.$(NC)" >&2; exit 1; }
	@command -v cargo >/dev/null 2>&1 || { echo "$(RED)cargo is required but not installed. Aborting.$(NC)" >&2; exit 1; }
	@command -v kubectl >/dev/null 2>&1 || { echo "$(RED)kubectl is required but not installed. Aborting.$(NC)" >&2; exit 1; }
	@test -f rs_app/.env || { echo "$(RED)rs_app/.env file is required but not found. Aborting.$(NC)" >&2; exit 1; }

fmt: ## Format Rust code
	@echo "$(GREEN)Formatting Rust code...$(NC)"
	@cd rs_app && cargo fmt

lint: ## Run clippy lints
	@echo "$(GREEN)Running clippy...$(NC)"
	@cd rs_app && cargo clippy -- -D warnings

test-local: ## Run local storage tests
	@echo "$(GREEN)Running local storage tests...$(NC)"
	@cd rs_app && cargo test -- --nocapture storage::tests::test_local_storage

test-s3: ## Run S3 storage tests (requires AWS credentials)
	@echo "$(YELLOW)Running S3 storage tests...$(NC)"
	@cd rs_app && TEST_S3_BUCKET=$(TEST_S3_BUCKET) cargo test -- --nocapture --ignored storage::tests::test_s3_storage

test-azure: ## Run Azure storage tests (requires Azure credentials)
	@echo "$(YELLOW)Running Azure storage tests...$(NC)"
	@cd rs_app && TEST_AZURE_CONTAINER=$(TEST_AZURE_CONTAINER) cargo test -- --nocapture --ignored storage::tests::test_azure_storage

test: test-local ## Run all tests (excluding cloud storage tests)
	@echo "$(GREEN)Running all tests...$(NC)"
	@cd rs_app && cargo test -- --nocapture

coverage: ## Run tests with coverage
	@echo "$(GREEN)Running tests with coverage...$(NC)"
	@cd rs_app && cargo install cargo-tarpaulin && cargo tarpaulin --out Html

clean: ## Clean build artifacts
	@echo "$(GREEN)Cleaning build artifacts...$(NC)"
	@cd rs_app && cargo clean
	@find . -type f -name "*.rs.bk" -delete

build: check ## Build the Docker image
	@echo "$(GREEN)Building Docker image: $(IMAGE_NAME):$(IMAGE_TAG)$(NC)"
	@cd rs_app && docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .
	@echo "$(GREEN)Successfully built $(IMAGE_NAME):$(IMAGE_TAG)$(NC)"

push: build ## Push the Docker image to registry
	@echo "$(GREEN)Pushing image to registry...$(NC)"
	@docker push $(IMAGE_NAME):$(IMAGE_TAG)
	@echo "$(GREEN)Successfully pushed $(IMAGE_NAME):$(IMAGE_TAG)$(NC)"

k8s-deploy: push ## Deploy to Kubernetes
	@echo "$(GREEN)Deploying to Kubernetes...$(NC)"
	@kubectl create namespace $(K8S_NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -
	@kubectl apply -f k8s/secrets.yaml -n $(K8S_NAMESPACE)
	@kubectl apply -f k8s/configmap.yaml -n $(K8S_NAMESPACE)
	@argo submit k8s/workflow.yaml \
		-n $(K8S_NAMESPACE) \
		--parameter input_url="$(INPUT_URL)" \
		--parameter output_url="$(OUTPUT_URL)" \
		--parameter filter_sql="$(SQL_FILTER)" \
		--parameter image_tag="$(IMAGE_TAG)"

k8s-delete: ## Delete Kubernetes resources
	@echo "$(GREEN)Deleting Kubernetes resources...$(NC)"
	@kubectl delete -f k8s/workflow.yaml -n $(K8S_NAMESPACE) --ignore-not-found
	@kubectl delete -f k8s/secrets.yaml -n $(K8S_NAMESPACE) --ignore-not-found
	@kubectl delete -f k8s/configmap.yaml -n $(K8S_NAMESPACE) --ignore-not-found

run: ## Run locally
	@echo "$(GREEN)Running locally...$(NC)"
	@cd rs_app && cargo run -- \
		--input-url "$(INPUT_URL)" \
		--output-url "$(OUTPUT_URL)" \
		--filter-sql "$(SQL_FILTER)"

watch: ## Watch for file changes and rebuild
	@echo "$(GREEN)Watching for changes...$(NC)"
	@cd rs_app && cargo watch -x run

.DEFAULT_GOAL := help
