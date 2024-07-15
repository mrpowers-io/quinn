# COMMON CLI COMMANDS FOR DEVELOPMENT 

all: help

.PHONY: install_test
install_test: ## Install the 'dev, test and extras' dependencies
	@poetry install --with=development,testing --extras connect

.PHONY: install_deps
install_deps: ## Install all dependencies
	@poetry install --with=development,linting,testing,docs

.PHONY: install_ruff
install_ruff: ## Install ruff for use within IDE
	@poetry run pip install ruff==0.5.2

.PHONY: update_deps
update_deps: ## Update dependencies
	@poetry update --with=development,linting,testing,docs

.PHONY: test
test: ## Run all tests
	@poetry run pytest tests

.PHONY: check
check: ## Lint and format the code by running pre-commit hooks
	@poetry run pre-commit run -a

.PHONY: help
help: ## Show help for the commands.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
