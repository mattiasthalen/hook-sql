PYTHON ?= python
UV ?= uv
UV_LINK_MODE ?= copy

export UV_LINK_MODE

.PHONY: bootstrap test test-coverage ruff mypy

bootstrap:
	$(UV) sync --dev

test:
	$(UV) run pytest -v

test-coverage:
	$(UV) run pytest --cov=src --cov-report=term-missing

ruff:
	$(UV) run ruff check src

mypy:
	$(UV) run mypy src

full-check: test ruff mypy