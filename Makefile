# Zamboni — the developer entry points, and the same checks CI runs.
#
# `make` on its own prints every target with what it guards. The organising idea
# is that a developer should be able to run **each CI job by hand, one at a
# time**, before pushing -- so every target below maps onto something
# .github/workflows/ci.yml does, and the mapping is named in the help text.
#
# Two conventions worth knowing before editing:
#
# * **Every target that runs Python goes through `uv`**, never a bare `python`.
#   The environment is built from `uv.lock` and nothing else; that is the whole
#   point of this project's setup, and a target that resolved against whatever
#   happened to be installed globally would be testing a different program.
# * **The dev-stack targets refuse the wrong stack rather than skipping.** The
#   `tests/test_dev_stack.py` fixtures skip when a port is closed, by design, so
#   "0 failures" is not the same as "it ran". `make test-spark` against a stack
#   with no Spark in it is an error here, not a green tick with nothing behind
#   it -- the same discipline CI enforces with ZAMBONI_REQUIRE_DEV_STACK and its
#   skip-count check.

SHELL := /bin/bash

VENV  := .venv
PY    := $(VENV)/bin/python
SRC   := src tests scripts

STACK    := dev-stack
COMPOSE  := docker compose
# Both engine profiles, for the teardown: `down` without them leaves a Trino or
# Spark container standing, and the next `make test-local` then refuses.
PROFILES := --profile trino --profile spark
# Present in every stack, engine or not. Used to answer "is a stack up at all".
BASE_SERVICES := lakekeeper minio
# The stack advertises an S3 endpoint on a pinned subnet, so it has to be free --
# free of *someone else's* network, that is. The compose project name is read
# from the compose file rather than repeated here, because the check has to be
# able to tell our own network from a stranger's: the first version could not,
# and refused to start a stack whenever one was already running, its own network
# being the thing holding the subnet.
SUBNET := 172.31.0.0/24
SUBNET_RE := 172\.31\.0\.0/24
COMPOSE_PROJECT := $(shell awk '/^name:/ {print $$2}' $(STACK)/docker-compose.yaml)
# Written by the no-skip check below; `.pytest_cache/` is already gitignored.
RUNLOG := .pytest_cache/make-run.log

MATRIX := 3.11 3.12 3.13

.DEFAULT_GOAL := all

# -- guards ---------------------------------------------------------------

# Warn, then fix it. The stricter version of this guard -- abort and tell the
# developer to create a virtualenv -- is the right call in a repository where
# the environment is a matter of taste. Here it is not: `uv sync` builds .venv
# from uv.lock deterministically, so there is exactly one correct answer and
# making the human type it adds nothing.
define require_venv
@if [ ! -x "$(PY)" ]; then \
  printf '\033[33mwarning: no virtualenv at %s\033[0m\n' '$(VENV)'; \
  printf '  building it from uv.lock now (the package plus the dev group)\n'; \
  $(MAKE) --no-print-directory venv; \
fi
$(ensure_pip)
@if [ -n "$$VIRTUAL_ENV" ] && \
   [ "$$(cd "$$VIRTUAL_ENV" 2>/dev/null && pwd -P)" != "$$(cd '$(VENV)' && pwd -P)" ]; then \
  printf '\033[33mwarning: VIRTUAL_ENV is %s\033[0m\n' "$$VIRTUAL_ENV"; \
  printf '  every target here runs `uv`, which uses %s regardless of what is activated.\n' '$(VENV)'; \
  printf '  Deactivate to avoid confusion; the run below is not using the activated one.\n'; \
fi
endef

# `uv sync` alone does not put pip in the environment, and the failure that
# causes is silent and off-target: with .venv activated but no pip inside it,
# `pip install x` runs whichever pip is next on PATH and installs into *that*
# interpreter. Measured here before adding this: `pip` resolved to
# ~/.local/bin/pip, a Python **3.10** pip, against a 3.13 project -- so the
# package lands in the user's global site-packages for a Python this project does
# not use, and the next `uv run` cannot see it.
#
# `uv venv --seed` covers a new environment and `uv pip install pip` repairs an
# existing one. Verified that `uv sync` prunes neither, so this is a one-time fix
# rather than a fight with every sync.
define ensure_pip
@if [ ! -x '$(VENV)/bin/pip' ]; then \
  printf '\033[33mwarning: %s has no pip -- installing it now\033[0m\n' '$(VENV)'; \
  printf '  Without it, `pip install` in the activated venv silently targets another\n'; \
  printf '  interpreter. `uv add` and `uv pip install` remain the right way to add a\n'; \
  printf '  dependency here, because they go through uv.lock; this is about the accident.\n'; \
  uv pip install pip >/dev/null; \
fi
endef

# What is running: `none`, or `local` plus whichever engines are up --
# `local`, `local+trino`, `local+spark`, `local+trino+spark`.
#
# Composite rather than one of three, because both engines really do run at once:
# `docker compose --profile spark up` starts Spark and leaves a Trino from an
# earlier session standing, which is a normal thing to end up with. The first
# version of this reported the highest-priority engine and nothing else, so
# `make test-trino` refused on a stack whose Trino tests pass -- found by running
# it against exactly that stack.
define stack_type
$$(cd $(STACK) && \
   running=$$($(COMPOSE) $(PROFILES) ps --services --filter status=running 2>/dev/null); \
   for svc in $(BASE_SERVICES); do \
     grep -qx "$$svc" <<< "$$running" || { echo none; exit 0; }; \
   done; \
   type=local; \
   grep -qx trino <<< "$$running" && type="$$type+trino"; \
   grep -qx spark <<< "$$running" && type="$$type+spark"; \
   echo "$$type")
endef

# -- help -----------------------------------------------------------------

##@ Getting started

all: help ## (default) print this help

help:
	@printf '\033[1mZamboni\033[0m -- make targets. Every test target maps onto a CI job.\n\n'
	@awk 'BEGIN {FS = ":.*##"} \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5); next } \
		/^[a-zA-Z0-9_-]+:.*##/ { printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 }' \
		$(MAKEFILE_LIST)
	@printf '\n\033[1mFirst time here\033[0m\n'
	@printf '  make venv && make test          the whole suite, no Docker, ~4 minutes\n'
	@printf '  make ci                         everything CI checks that needs no containers\n'
	@printf '  make local-stack-start test-local\n'
	@printf '                                  add a real catalog and object store\n'
	@printf '\n\033[1mStack\033[0m  now: \033[36m%s\033[0m  (local | trino | spark | none)\n' "$(stack_type)"
	@printf '\n'

venv: ## create .venv from uv.lock -- the package, the dev dependencies, and pip
	@if [ ! -d '$(VENV)' ]; then uv venv --seed; fi
	uv sync
	$(ensure_pip)
	@printf 'ready: %s -- %s, pip %s\n' '$(VENV)' "$$($(PY) --version)" \
	  "$$($(VENV)/bin/pip --version | awk '{print $$2}')"

doctor: ## can this machine run everything? checks the toolchain, not the code
	@fail=0; warn=0; \
	printf '\033[1mZamboni preflight\033[0m -- the machine, not the code\n\n'; \
	if command -v uv >/dev/null; then \
	  printf '  \033[32mok\033[0m    uv %s\n' "$$(uv --version | awk '{print $$2}')"; \
	else \
	  printf '  \033[31mFAIL\033[0m  uv is not installed -- every target here runs through it\n'; \
	  printf '        https://docs.astral.sh/uv/getting-started/installation/\n'; \
	  fail=$$((fail+1)); \
	fi; \
	ci_uv=$$(awk -F'"' '/UV_VERSION:/ {print $$2}' .github/workflows/ci.yml); \
	if command -v uv >/dev/null && [ "$$(uv --version | awk '{print $$2}')" != "$$ci_uv" ]; then \
	  printf '  \033[36minfo\033[0m  CI pins uv %s; yours differs. `uv sync --frozen` keeps the lock honest either way\n' "$$ci_uv"; \
	fi; \
	if [ -x '$(PY)' ]; then \
	  printf '  \033[32mok\033[0m    %s  %s\n' '$(VENV)' "$$($(PY) --version)"; \
	  if [ -x '$(VENV)/bin/pip' ]; then \
	    printf '  \033[32mok\033[0m    pip is inside the venv (%s)\n' \
	      "$$($(VENV)/bin/pip --version | awk '{print $$2}')"; \
	  else \
	    printf '  \033[33mwarn\033[0m  no pip in the venv -- `pip install` would target another interpreter\n'; \
	    printf '        make venv\n'; warn=$$((warn+1)); \
	  fi; \
	else \
	  printf '  \033[33mwarn\033[0m  no %s yet -- any target builds it, or run `make venv`\n' '$(VENV)'; \
	  warn=$$((warn+1)); \
	fi; \
	for v in $(MATRIX); do \
	  if uv python find "$$v" >/dev/null 2>&1; then \
	    printf '  \033[32mok\033[0m    Python %s, for make test-matrix\n' "$$v"; \
	  else \
	    printf '  \033[33mwarn\033[0m  no Python %s -- make test-matrix will fetch it (uv python install %s)\n' "$$v" "$$v"; \
	    warn=$$((warn+1)); \
	  fi; \
	done; \
	if ! command -v docker >/dev/null; then \
	  printf '  \033[33mwarn\033[0m  no docker -- everything except the stack targets still works\n'; \
	  warn=$$((warn+1)); \
	elif ! docker info >/dev/null 2>&1; then \
	  printf '  \033[33mwarn\033[0m  docker is installed but the daemon is not reachable\n'; \
	  warn=$$((warn+1)); \
	else \
	  printf '  \033[32mok\033[0m    docker %s, daemon reachable\n' "$$(docker version --format '{{.Server.Version}}' 2>/dev/null)"; \
	  printf '  \033[36minfo\033[0m  stack: %s\n' "$(stack_type)"; \
	  if $(MAKE) -s stack-subnet >/dev/null 2>&1; then \
	    printf '  \033[32mok\033[0m    %s is free, or held by our own stack\n' '$(SUBNET)'; \
	  else \
	    printf '  \033[31mFAIL\033[0m  %s is held by a network that is not ours\n' '$(SUBNET)'; \
	    fail=$$((fail+1)); \
	  fi; \
	fi; \
	if [ -f '$(STACK)/.env' ]; then \
	  printf '  \033[32mok\033[0m    %s/.env exists\n' '$(STACK)'; \
	else \
	  printf '  \033[36minfo\033[0m  no %s/.env -- the stack-start targets write it from .env.sample\n' '$(STACK)'; \
	fi; \
	if [ -f .git/hooks/pre-commit ]; then \
	  printf '  \033[32mok\033[0m    the pre-commit hook is installed\n'; \
	else \
	  printf '  \033[33mwarn\033[0m  no pre-commit hook -- uv run pre-commit install\n'; \
	  warn=$$((warn+1)); \
	fi; \
	if command -v gh >/dev/null && gh extension list 2>/dev/null | grep -q gh-agile; then \
	  printf '  \033[32mok\033[0m    gh with the agile extension, for the issue board\n'; \
	else \
	  printf '  \033[36minfo\033[0m  no `gh agile` -- only needed to file or move issues\n'; \
	  printf '        gh extension install paulcaron16k/gh-agile\n'; \
	fi; \
	printf '\n%s blocker(s), %s warning(s)\n' "$$fail" "$$warn"; \
	if [ "$$fail" -gt 0 ]; then exit 1; fi; \
	if [ -x '$(PY)' ]; then \
	  printf '\n\033[1mAnd what this PyIceberg can do\033[0m -- a different question, same word:\n'; \
	  uv run zamboni doctor; \
	fi

venv-frozen: ## same, but fail if uv.lock is stale against pyproject.toml (CI: lint)
	uv sync --frozen

# -- code quality ---------------------------------------------------------

##@ Code quality -- CI job `lint`

lint: ruff format-check ruff-pin typecheck precommit ## everything the `lint` job runs

ruff: ## ruff check over src, tests and scripts
	$(require_venv)
	uv run ruff check $(SRC)

format: ## ruff format and ruff --fix, writing changes
	$(require_venv)
	uv run ruff format $(SRC)
	uv run ruff check --fix $(SRC)

format-check: ## ruff format --check; fails rather than writing
	$(require_venv)
	uv run ruff format --check $(SRC)

typecheck: ## mypy over src and scripts (config declares its own file set)
	$(require_venv)
	uv run mypy

precommit: ## every pre-commit hook over every file
	$(require_venv)
	uv run pre-commit run --all-files --show-diff-on-failure

ruff-pin: ## the ruff in uv.lock and the one .pre-commit-config.yaml pins must match
	$(require_venv)
	@locked=$$(uv run ruff --version | awk '{print $$2}'); \
	hooked=$$(grep -A1 'ruff-pre-commit' .pre-commit-config.yaml | grep 'rev:' | tr -d ' v' | cut -d: -f2); \
	if [ "$$locked" != "$$hooked" ]; then \
	  printf '\033[31mruff mismatch -- uv.lock has %s, .pre-commit-config.yaml pins %s\033[0m\n' \
	    "$$locked" "$$hooked"; \
	  printf '  A hook on a different ruff rewrites files CI considers correct, so every commit churns.\n'; \
	  exit 1; \
	fi
	@printf 'ruff pinned consistently: %s\n' "$$(uv run ruff --version | awk '{print $$2}')"

# -- tests ----------------------------------------------------------------

##@ Tests that need nothing but Python -- CI job `test`

test: ## the suite without the dev-stack tests (exactly what CI runs)
	$(require_venv)
	uv run pytest -q --ignore=tests/test_dev_stack.py

test-matrix: ## the suite on 3.11, 3.12 and 3.13 as CI does; restores .venv after
	$(require_venv)
	@for v in $(MATRIX); do \
	  printf '\n\033[1m-- Python %s\033[0m\n' "$$v"; \
	  uv sync --frozen --python "$$v" >/dev/null && \
	  uv run --python "$$v" pytest -q --ignore=tests/test_dev_stack.py || { uv sync >/dev/null; exit 1; }; \
	done; \
	uv sync >/dev/null; \
	printf '\nrestored %s to the pinned Python\n' '$(VENV)'

test-docs: ## just the documentation invariants -- fast, run it while editing docs
	$(require_venv)
	uv run pytest tests/test_docs.py -q

test-executables: ## bin/ regenerates to a no-op and runs from outside the repo (CI: executables)
	$(require_venv)
	uv run scripts/build-executable.py
	@git diff --exit-code -- bin/ \
	  || { printf '\033[31mbin/ is stale -- the regeneration above changed it\033[0m\n'; exit 1; }
	cd /tmp && "$(CURDIR)/bin/zamboni" doctor
	cd /tmp && "$(CURDIR)/bin/zamboni" --help > /dev/null
	@printf 'bin/ is current and runs from outside the project directory\n'

ci: lint test test-executables version-watch ## every CI check that needs no containers

##@ Tests that need the dev stack -- CI jobs `dev-stack` and `spark`

test-local: local-stack ## dev-stack tests against a stack with no engine (CI: dev-stack)
	$(require_venv)
	$(call pytest_no_skips,tests/test_dev_stack.py -v -m "not spark and not trino")

test-trino: trino-stack ## the live Trino tests (CI: dev-stack, with the trino profile)
	$(require_venv)
	$(call pytest_no_skips,tests/test_dev_stack.py -v -m trino)

test-spark: spark-stack ## the live Spark Connect tests (CI: spark)
	$(require_venv)
	$(call pytest_no_skips,tests/test_dev_stack.py -v -m spark)

test-demo: local-stack ## the demo end to end on Lakekeeper and MinIO (CI: dev-stack)
	$(require_venv)
	ZAMBONI_URI=http://localhost:8182/catalog ZAMBONI_WAREHOUSE=zamboni ./bin/zamboni-demo --catalog lakekeeper clear
	ZAMBONI_URI=http://localhost:8182/catalog ZAMBONI_WAREHOUSE=zamboni ./bin/zamboni-demo --catalog lakekeeper next-day
	ZAMBONI_URI=http://localhost:8182/catalog ZAMBONI_WAREHOUSE=zamboni ./bin/zamboni-demo --catalog lakekeeper maintenance --reclaim-now
	ZAMBONI_URI=http://localhost:8182/catalog ZAMBONI_WAREHOUSE=zamboni ./bin/zamboni-demo --catalog lakekeeper query
	ZAMBONI_URI=http://localhost:8182/catalog ZAMBONI_WAREHOUSE=zamboni ./bin/zamboni-demo --catalog lakekeeper clear

# A skip here is a failure, because these fixtures skip on a closed port by
# design: without this, a stack that never came up produces a suite of skips and
# a tick that means nothing was tested. ZAMBONI_REQUIRE_DEV_STACK covers the
# base services; the engine fixtures skip even under it, which is why the skip
# count is checked outright -- the same reasoning as the `spark` CI job.
define pytest_no_skips
@mkdir -p $(dir $(RUNLOG))
@set -o pipefail; \
ZAMBONI_REQUIRE_DEV_STACK=1 uv run pytest $(1) 2>&1 | tee $(RUNLOG); \
status=$$?; \
if grep -qE '[0-9]+ skipped' $(RUNLOG); then \
  printf '\033[31merror: something skipped, so it did not run.\033[0m\n'; \
  printf '  The engine fixtures skip on a closed port. Start the stack this target needs.\n'; \
  exit 1; \
fi; \
exit $$status
endef

# -- the dev stack --------------------------------------------------------

##@ The dev stack -- Lakekeeper, Postgres, MinIO, and optionally one engine

local-stack-start: stack-env stack-subnet ## start the stack with no engine
	cd $(STACK) && $(COMPOSE) up -d --wait
	$(MAKE) --no-print-directory stack-bootstrap

trino-stack-start: stack-env stack-subnet ## start the stack plus Trino (--profile trino)
	cd $(STACK) && $(COMPOSE) --profile trino up -d --wait
	$(MAKE) --no-print-directory stack-bootstrap

spark-stack-start: stack-env stack-subnet ## start the stack plus Spark Connect (--profile spark)
	cd $(STACK) && $(COMPOSE) --profile spark up -d --wait
	$(MAKE) --no-print-directory stack-bootstrap

local-stack-stop: stack-stop ## stop the stack, keeping the warehouse data
trino-stack-stop: stack-stop ## stop the stack and Trino with it
spark-stack-stop: stack-stop ## stop the stack and Spark with it

# One teardown for all three, with both profiles named: `down` on its own leaves
# a Trino or Spark container standing, and the next `make test-local` refuses.
stack-stop: ## stop every service, whichever profile started it
	cd $(STACK) && $(COMPOSE) $(PROFILES) down

stack-clean: ## stop everything and delete the volumes -- the warehouse goes too
	cd $(STACK) && $(COMPOSE) $(PROFILES) down -v

stack-status: ## which stack is up, and what is in it
	@printf 'stack: \033[36m%s\033[0m\n' "$(stack_type)"
	@cd $(STACK) && $(COMPOSE) $(PROFILES) ps 2>/dev/null || true

stack-logs: ## the last 200 lines from every service
	cd $(STACK) && $(COMPOSE) $(PROFILES) logs --tail 200

stack-bootstrap: ## create the warehouse in Lakekeeper (idempotent)
	$(require_venv)
	cd $(STACK) && uv run bootstrap.py

stack-env: $(STACK)/.env
$(STACK)/.env:
	@printf 'writing %s from .env.sample -- it holds credentials and is gitignored\n' '$@'
	cp $(STACK)/.env.sample $@

stack-subnet:
	@if docker network ls -q | xargs -r -I{} docker network inspect {} \
	     --format '{{index .Labels "com.docker.compose.project"}} {{range .IPAM.Config}}{{.Subnet}}{{end}}' \
	     2>/dev/null \
	   | grep -v '^$(COMPOSE_PROJECT) ' | grep -q ' $(SUBNET_RE)$$'; then \
	  printf '\033[31m%s is in use by a network that is not ours\033[0m\n' '$(SUBNET)'; \
	  printf '  The stack pins it so the S3 endpoint it advertises is a knowable address.\n'; \
	  printf '  Change it in %s/docker-compose.yaml and S3_GATEWAY, or free the subnet.\n' '$(STACK)'; \
	  exit 1; \
	fi

# The three guards the test targets depend on. Each says how to get the stack it
# wanted, because "wrong stack" is a state a developer reaches by having done
# something reasonable earlier.
#
# Three recipes calling one define rather than three targets sharing one
# prerequisite: in a shared recipe `$@` is the prerequisite's own name, so the
# guard could not tell which stack its caller wanted.
define require_stack
@have="$(stack_type)"; want='$(1)'; ok=no; \
if [ "$$have" != none ]; then \
  if [ "$$want" = local ]; then \
    [ "$$have" = local ] && ok=yes; \
  else \
    case "$$have" in *"+$$want"*) ok=yes ;; esac; \
  fi; \
fi; \
if [ "$$ok" = yes ]; then printf 'stack: %s\n' "$$have"; exit 0; fi; \
if [ "$$have" = none ]; then \
  printf '\033[31mno dev stack is running; this needs the %s stack\033[0m\n' "$$want"; \
elif [ "$$want" = local ]; then \
  printf '\033[31mthe running stack is %s; test-local wants no engine at all\033[0m\n' "$$have"; \
  printf '  An engine changes what the dev-stack tests exercise, so it is refused rather\n'; \
  printf '  than tolerated. Stop everything first:  make stack-stop\n'; \
else \
  printf '\033[31mthe running stack is %s, with no %s in it\033[0m\n' "$$have" "$$want"; \
  printf '  Adding it does not disturb what is already up.\n'; \
fi; \
printf '  Then:           make %s-stack-start\n' "$$want"; \
exit 1
endef

local-stack:
	$(call require_stack,local)

trino-stack:
	$(call require_stack,trino)

spark-stack:
	$(call require_stack,spark)

# -- housekeeping ---------------------------------------------------------

##@ Housekeeping

version-watch: ## report upstream releases above a cap in pyproject.toml
	uv run --no-project scripts/version_watch.py

clean: ## remove caches and the generated demo warehouse
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	rm -rf data/healthims/iceberg_warehouse data/healthims/.spill
	rm -f data/healthims/iceberg_catalog.db data/healthims/iceberg_catalog.db-* data/healthims/demo.env
	find . -type d -name __pycache__ -not -path './.venv/*' -exec rm -rf {} + 2>/dev/null || true

clean-venv: ## delete .venv; `make venv` rebuilds it from uv.lock
	rm -rf $(VENV)

.PHONY: all help venv venv-frozen doctor \
        lint ruff format format-check typecheck precommit ruff-pin \
        test test-matrix test-docs test-executables ci \
        test-local test-trino test-spark test-demo \
        local-stack-start trino-stack-start spark-stack-start \
        local-stack-stop trino-stack-stop spark-stack-stop \
        stack-stop stack-clean stack-status stack-logs stack-bootstrap \
        stack-env stack-subnet \
        local-stack trino-stack spark-stack \
        version-watch clean clean-venv
