.PHONY: setup lint format typecheck compat security pre-commit test ci

SHELL := /usr/bin/bash

setup:
	uv sync --all-extras

lint:
	uv run ruff check .

format:
	uv run ruff format

typecheck:
	uv run basedpyright .

compat:
	uv run vermin --target=3.11- --violations .

security:
	uv run pip-audit .

pre-commit:
	uv run pre-commit run --all-files

test:
	uv run pytest

ci: setup lint typecheck compat security test pre-commit