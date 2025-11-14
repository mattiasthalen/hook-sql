PYTHON ?= python
UV ?= uv
UV_LINK_MODE ?= copy

export UV_LINK_MODE

.PHONY: bootstrap install-pre-commit test test-coverage ruff mypy build build-check full-check clean bump-minor bump-major

# Helper function to get current version
define get-version
$(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0")
endef

# Helper function to parse version components
define parse-version
$(shell echo $(1) | sed 's/v\([0-9]*\)\.\([0-9]*\)\.\([0-9]*\)/\$(2)/')
endef

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

build:
	$(UV) build

build-check: build
	$(UV) run twine check dist/*

clean:
	rm -rf dist build .pytest_cache .mypy_cache .ruff_cache *.egg-info

full-check: clean test ruff mypy build-check

# Version bump helper - usage: make bump-version BUMP_TYPE=minor|major
bump-version:
	@echo "Stashing uncommitted changes..."
	@git stash push -u -m "Auto-stash before version bump" 2>&1 | grep -q "No local changes" && STASHED=1 || STASHED=0; \
	CURRENT=$$(git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0"); \
	echo "Current version: $$CURRENT"; \
	MAJOR=$$(echo $$CURRENT | sed 's/v\([0-9]*\)\.\([0-9]*\)\.\([0-9]*\)/\1/'); \
	MINOR=$$(echo $$CURRENT | sed 's/v\([0-9]*\)\.\([0-9]*\)\.\([0-9]*\)/\2/'); \
	if [ "$(BUMP_TYPE)" = "major" ]; then \
		NEW_MAJOR=$$((MAJOR + 1)); \
		NEW_VERSION="v$$NEW_MAJOR.0.0"; \
	elif [ "$(BUMP_TYPE)" = "minor" ]; then \
		NEW_MINOR=$$((MINOR + 1)); \
		NEW_VERSION="v$$MAJOR.$$NEW_MINOR.0"; \
	else \
		echo "Error: BUMP_TYPE must be 'minor' or 'major'"; \
		exit 1; \
	fi; \
	echo "New version: $$NEW_VERSION"; \
	$(MAKE) full-check && \
	git tag $$NEW_VERSION && \
	git push origin $$NEW_VERSION && \
	echo "Successfully released $$NEW_VERSION"; \
	if [ $$STASHED -eq 0 ]; then \
		echo "Restoring stashed changes..."; \
		git stash pop; \
	fi

bump-minor:
	$(MAKE) bump-version BUMP_TYPE=minor

bump-major:
	$(MAKE) bump-version BUMP_TYPE=major
