# COMMON CLI COMMANDS FOR DEVELOPMENT 

all: help

.PHONY: install_test
install_test:
	@poetry install --with=development,testing --extras connect

.PHONY: install_deps
install_deps: ## Install all dependencies
	@poetry install --with=development,linting,testing,docs

.PHONY: update_deps
update_deps: ## Update dependencies
	@poetry update --with=development,linting,testing,docs

.PHONY: test
test:
	@poetry run pytest tests -k "not test_spark_connect.py"

.PHONY: test
test_spark_connect:
	@poetry run pytest tests/test_spark_connect.py

.PHONY: lint 
lint: ## Lint the code
	@poetry run ruff check --fix quinn

.PHONY: format
format: ## Format the code
	@poetry run ruff format quinn

# Inspired by https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
help:
	@echo '................... Quinn ..........................'
	@echo 'help                      - print that message'
	@echo 'lint                      - run linter'
	@echo 'format                    - reformat the code'
	@echo 'test                      - run tests'
	@echo 'install_test              - install test deps'
	@echo 'install_deps              - install dev deps'
	@echo 'update_deps               - update and install deps'
help: ## Show help for the commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
