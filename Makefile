PYTHON ?= python
UV ?= uv
UV_LINK_MODE ?= copy

export UV_LINK_MODE

.PHONY: bootstrap install-pre-commit test test-coverage ruff mypy build build-check full-check clean release-patch release-minor release-major

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
	rm -rf dist build .pytest_cache .mypy_cache .ruff_cache *.egg-info tmp

full-check: clean test ruff mypy build-check

# Version release helper - usage: make release-version RELEASE_TYPE=patch|minor|major
release-version:
	@echo "Stashing uncommitted changes..."
	@git stash push -u -m "Auto-stash before version release" 2>&1 | grep -q "No local changes" && STASHED=1 || STASHED=0; \
	CURRENT=$$(git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0"); \
	echo "Current version: $$CURRENT"; \
	MAJOR=$$(echo $$CURRENT | sed 's/v\([0-9]*\)\.\([0-9]*\)\.\([0-9]*\)/\1/'); \
	MINOR=$$(echo $$CURRENT | sed 's/v\([0-9]*\)\.\([0-9]*\)\.\([0-9]*\)/\2/'); \
	PATCH=$$(echo $$CURRENT | sed 's/v\([0-9]*\)\.\([0-9]*\)\.\([0-9]*\)/\3/'); \
	if [ "$(RELEASE_TYPE)" = "major" ]; then \
		NEW_MAJOR=$$((MAJOR + 1)); \
		NEW_VERSION="v$$NEW_MAJOR.0.0"; \
	elif [ "$(RELEASE_TYPE)" = "minor" ]; then \
		NEW_MINOR=$$((MINOR + 1)); \
		NEW_VERSION="v$$MAJOR.$$NEW_MINOR.0"; \
	elif [ "$(RELEASE_TYPE)" = "patch" ]; then \
		NEW_PATCH=$$((PATCH + 1)); \
		NEW_VERSION="v$$MAJOR.$$MINOR.$$NEW_PATCH"; \
	else \
		echo "Error: RELEASE_TYPE must be 'patch', 'minor' or 'major'"; \
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

release-patch:
	$(MAKE) release-version RELEASE_TYPE=patch

release-minor:
	$(MAKE) release-version RELEASE_TYPE=minor

release-major:
	$(MAKE) release-version RELEASE_TYPE=major
