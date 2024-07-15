# COMMON CLI COMMANDS FOR DEVELOPMENT 

all: help

.PHONY: install_test
install_test: ## Install the 'dev, test and extras' dependencies
	@poetry install --with=development,testing --extras connect

.PHONY: install_deps
install_deps: ## Install all dependencies
	@poetry install --with=development,linting,testing,docs

.PHONY: update_deps
update_deps: ## Update dependencies
	@poetry update --with=development,linting,testing,docs

.PHONY: test
test: ## Run all tests
	@poetry run pytest tests

.PHONY: lint 
lint: ## Lint the code
	@poetry run ruff check --fix quinn

.PHONY: format
format: ## Format the code
	@poetry run ruff format quinn

# Inspired by https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
help: ## Show help for the commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
