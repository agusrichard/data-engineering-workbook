.PHONY: lint format check fix

lint:
	uv run ruff check .

format:
	uv run ruff format .

fix:
	uv run ruff check --fix .

check: lint
	uv run ruff format --check .
