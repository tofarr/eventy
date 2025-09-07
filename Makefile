# Makefile for Eventy project

.PHONY: help install lint test clean format

help:  ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install:  ## Install dependencies using Poetry
	poetry install

lint:  ## Run pylint on the eventy package
	poetry run pylint eventy

lint-report:  ## Run pylint with detailed report
	poetry run pylint eventy --output-format=text

test:  ## Run tests using pytest
	poetry run pytest

format:  ## Format code using black
	poetry run black eventy tests

format-check:  ## Check code formatting without making changes
	poetry run black --check eventy tests

clean:  ## Clean up temporary files
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .pytest_cache/
	rm -rf dist/
	rm -rf build/

dev-setup: install  ## Set up development environment
	@echo "Development environment setup complete!"
	@echo "Run 'make lint' to check code quality"
	@echo "Run 'make test' to run tests"
	@echo "Run 'make format' to format code"