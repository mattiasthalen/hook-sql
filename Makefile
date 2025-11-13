PYTHON ?= python
UV ?= uv
UV_LINK_MODE ?= copy

export UV_LINK_MODE

.PHONY: bootstrap install-pre-commit test test-coverage ruff mypy full-check

bootstrap:
	$(UV) sync --dev

install-pre-commit:
	$(UV) run pre-commit install

test:
	$(UV) run pytest -v

test-coverage:
	$(UV) run pytest --cov=src --cov-report=term-missing

ruff:
	$(UV) run ruff check src

mypy:
	$(UV) run mypy src

full-check: test ruff mypy