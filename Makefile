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

.PHONY: lint 
lint:
	@poetry run ruff check --fix quinn

.PHONY: format
format:
	@poetry run ruff format quinn

.PHONY: help
help:
	@echo '................... Quin ..........................'
	@echo 'help                      - print that message'
	@echo 'lint                      - run linter'
	@echo 'format                    - reformat the code'
	@echo 'test                      - run tests'
	@echo 'install_test              - install test deps'
	@echo 'install_deps              - install dev deps'
	@echo 'update_deps               - update and install deps'
