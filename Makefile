.DEFAULT_GOAL := help
.PHONY: help install test lint fmt demo check

help:
	@echo "Targets:"
	@echo "  make install   install dependencies into the venv"
	@echo "  make test      run the test suite"
	@echo "  make lint      check lint rules and formatting"
	@echo "  make fmt       auto-format the code"
	@echo "  make demo      run the end-to-end demo"
	@echo "  make check     lint, then run the tests"

install:
	uv sync

test:
	uv run pytest

lint:
	uv run ruff check .

fmt:
	uv run ruff format .

demo:
	uv run python -m app.demo

check: lint test
