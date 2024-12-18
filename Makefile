# Configuration
IMAGE_NAME := rs_app/distributed-transformer
VERSION := $(shell cat rs_app/VERSION 2>/dev/null || echo "0.1.0")
GIT_COMMIT := $(shell git rev-parse --short HEAD)
IMAGE_TAG ?= $(VERSION)-$(GIT_COMMIT)

# Kubernetes/Argo configuration
INPUT_URL ?= s3://mybucket/input_data.parquet
OUTPUT_URL ?= s3://mybucket/output_data.parquet
SQL_FILTER ?= "SELECT * FROM data"
K8S_NAMESPACE ?= default

# Colors for output
GREEN := \033[0;32m
NC := \033[0m

.PHONY: all build push run clean test fmt check help version k8s-deploy k8s-delete

all: check build test ## Build and test everything

help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

version: ## Display version information
	@echo "Version: $(VERSION)"
	@echo "Git commit: $(GIT_COMMIT)"
	@echo "Image tag: $(IMAGE_TAG)"

check: ## Check if all required tools are installed
	@command -v docker >/dev/null 2>&1 || { echo "docker is required but not installed. Aborting." >&2; exit 1; }
	@command -v argo >/dev/null 2>&1 || { echo "argo is required but not installed. Aborting." >&2; exit 1; }
	@command -v cargo >/dev/null 2>&1 || { echo "cargo is required but not installed. Aborting." >&2; exit 1; }
	@command -v kubectl >/dev/null 2>&1 || { echo "kubectl is required but not installed. Aborting." >&2; exit 1; }

fmt: ## Format Rust code
	@echo "$(GREEN)Formatting Rust code...$(NC)"
	@cd rs_app && cargo fmt

build: check ## Build the Docker image
	@echo "$(GREEN)Building Docker image: $(IMAGE_NAME):$(IMAGE_TAG)$(NC)"
	@cd rs_app && docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .
	@echo "$(GREEN)Successfully built $(IMAGE_NAME):$(IMAGE_TAG)$(NC)"

push: build ## Push the Docker image to registry
	@echo "$(GREEN)Pushing image to registry...$(NC)"
	@docker push $(IMAGE_NAME):$(IMAGE_TAG)
	@echo "$(GREEN)Successfully pushed $(IMAGE_NAME):$(IMAGE_TAG)$(NC)"

run: ## Run the Argo workflow
	@echo "$(GREEN)Submitting Argo workflow...$(NC)"
	@argo submit k8s/workflow.yaml \
		-n $(K8S_NAMESPACE) \
		-p input_url=$(INPUT_URL) \
		-p output_url=$(OUTPUT_URL) \
		-p filter_sql=$(SQL_FILTER) \
		-p image_tag=$(IMAGE_TAG)

test: ## Run Rust tests
	@echo "$(GREEN)Running tests...$(NC)"
	@cd rs_app && cargo test

clean: ## Clean up build artifacts
	@echo "$(GREEN)Cleaning up...$(NC)"
	@cd rs_app && cargo clean
	@docker images $(IMAGE_NAME) -q | xargs -r docker rmi -f

k8s-deploy: ## Deploy to Kubernetes (builds and pushes image first)
	@echo "$(GREEN)Deploying to Kubernetes...$(NC)"
	@make push
	@make run

k8s-delete: ## Delete the workflow from Kubernetes
	@echo "$(GREEN)Deleting workflow from Kubernetes...$(NC)"
	@argo delete -n $(K8S_NAMESPACE) --all

# Development shortcuts
dev-build: fmt build ## Format code and build

dev-run: dev-build run ## Build and run

# Watch for changes and rebuild (requires cargo-watch)
watch: ## Watch for changes and rebuild
	@cd rs_app && cargo watch -x build

# Default target
.DEFAULT_GOAL := help
