# COMMON CLI COMMANDS FOR DEVELOPMENT 

all: help

.PHONY: install_test
install_test:
	@poetry install --with=development,testing --extras connect

.PHONY: install_deps
install_deps:
	@poetry install --with=development,linting,testing,docs

.PHONY: update_deps
update_deps:
	@poetry update --with=development,linting,testing,docs

.PHONY: test
test:
	@poetry run pytest tests -k "not test_spark_connect.py"

.PHONY: test
test_spark_connect:
	@poetry run pytest tests/test_spark_connect.py

.PHONY: lint 
lint:
	@poetry run ruff check --fix quinn

.PHONY: format
format:
	@poetry run ruff format quinn

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
