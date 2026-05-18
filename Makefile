# QHist Database Makefile
# Convenience targets for database management and job sync

# full bash shell requied for our complex make rules

.ONESHELL:
SHELL := /bin/bash

CONDA_ROOT := $(shell conda info --base)

# common way to inialize enviromnent across various types of systems
config_env := module load conda >/dev/null 2>&1 || true && . $(CONDA_ROOT)/etc/profile.d/conda.sh

PYTHON := python3
SCRIPTS := scripts
JOB_HISTORY_DATA_DIR ?= data

# Default date is today in YYYY-MM-DD format
DATE ?= $(shell date +%Y-%m-%d)
# Root of locally-mirrored PBS accounting logs (see sync-logs target)
LOG_DIR ?= ./data/sample_pbs_logs

.PHONY: clean docker-build docker-down docker-restart docker-up \
        dry-run-all dry-run-casper dry-run-derecho \
        help init-db sync-all sync-casper sync-derecho sync-logs \
        test-import update-vendor

help: ## Show this help message
	@echo ""
	@echo -e "\033[1;36mQHist Database Management\033[0m"
	@echo -e "\033[1;36m=========================\033[0m"
	@echo ""
	@echo -e "\033[1mAvailable targets:\033[0m"
	@echo ""
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[32m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo -e "\033[1mPattern rules:\033[0m"
	@echo -e "  \033[32mmake <name>\033[0m            Create conda environment from <name>.yaml"
	@echo -e "  \033[32mmake solve-<name>\033[0m      Dry-run solve for <name>.yaml (no install)"
	@echo ""
	@echo -e "\033[1mVariables:\033[0m"
	@echo "  DATE=YYYY-MM-DD        Date to sync (default: today)"
	@echo "  START=YYYY-MM-DD       Start date for range sync"
	@echo "  END=YYYY-MM-DD         End date for range sync"
	@echo "  LOG_DIR=<path>         Root of PBS log mirror (default: $(LOG_DIR))"
	@echo ""
	@echo -e "\033[1mExamples:\033[0m"
	@echo "  make sync-derecho DATE=2025-11-21"
	@echo "  make sync-all START=2025-11-01 END=2025-11-21"
	@echo ""

init-db: ## Create database tables (both machines)
	@echo "Initializing databases..."
	@$(PYTHON) -c "from job_history import init_db; init_db()"
	@echo "Created $(JOB_HISTORY_DATA_DIR)/casper.db"
	@echo "Created $(JOB_HISTORY_DATA_DIR)/derecho.db"

sync-casper: ## Sync Casper jobs for DATE (or START..END range)
ifdef START
	jobhist sync -m casper -l $(LOG_DIR)/casper --start $(START) $(if $(END),--end $(END)) -v #--upsert
else
	jobhist sync -m casper -l $(LOG_DIR)/casper -d $(DATE) -v --incremental
endif

sync-derecho: ## Sync Derecho jobs for DATE (or START..END range)
ifdef START
	jobhist sync -m derecho -l $(LOG_DIR)/derecho --start $(START) $(if $(END),--end $(END)) -v #--upsert
else
	jobhist sync -m derecho -l $(LOG_DIR)/derecho -d $(DATE) -v --incremental
endif

sync-all: sync-casper sync-derecho ## Sync both machines for DATE (or range)

clean: ## Remove all database files
	@echo "Removing databases..."
	@rm -f $(JOB_HISTORY_DATA_DIR)/casper.db $(JOB_HISTORY_DATA_DIR)/derecho.db $(JOB_HISTORY_DATA_DIR)/qhist.db
	@echo "Done."

# Development targets

test-import: ## Verify job_history package imports cleanly
	@$(PYTHON) -c "from job_history import Job, init_db; print('Import successful')"

dry-run-casper: ## Parse Casper logs for DATE without writing DB
	jobhist sync -m casper -l $(LOG_DIR)/casper -d $(DATE) --dry-run -v

dry-run-derecho: ## Parse Derecho logs for DATE without writing DB
	jobhist sync -m derecho -l $(LOG_DIR)/derecho -d $(DATE) --dry-run -v

dry-run-all: dry-run-casper dry-run-derecho ## Dry-run both machines for DATE

%: %.yaml
	[ -d $@ ] && mv $@ $@.old && rm -rf $@.old &
	$(config_env)
	conda env create --file $< --prefix $@
	conda activate ./$@
	conda list
	pip install -e ".[analysis]"
	pipdeptree --all 2>/dev/null || true

solve-%: %.yaml
	$(config_env)
	conda env create --file $< --prefix $@ --dry-run


sync-logs: ## rsync PBS accounting logs from casper + derecho
	rsync -axv derecho:/ncar/pbs/accounting/20* ./data/sample_pbs_logs/derecho/
	rsync -axv casper:/ssg/pbs/casper/accounting/2026* ./data/sample_pbs_logs/casper/

update-vendor: ## Re-download vendored pbs_parser_ncar/ncar.py
	@echo "Updating vendored pbs_parser_ncar/ncar.py..."
	@curl -sSf https://raw.githubusercontent.com/NCAR/pbs_parser_ncar/main/ncar.py \
	    -o job_history/_vendor/pbs_parser_ncar/ncar.py
	@printf '# Vendored from https://github.com/NCAR/pbs_parser_ncar\n# %s\n# Local modifications: none\n' \
	    "$$(date +%Y-%m-%d)" > job_history/_vendor/pbs_parser_ncar/README
	@echo "Done. Review with: git diff job_history/_vendor/"

docker-build: ## Build docker containers
	@docker compose build

docker-up: ## Start docker containers (waits until every service reports healthy)
	@# `--wait` blocks until every service with a healthcheck is healthy and
	@# exits non-zero if any becomes unhealthy. Replaces the older
	@# `grep -q healthy` loop, which returned as soon as the first container
	@# (usually cache, in 5s) reported healthy — well before mysql had
	@# finished restoring the backup.
	@docker compose up --detach --wait
	@echo "✅ Containers ready!"

docker-down: ## Stop docker containers
	docker compose down

docker-restart: ## Rebuild and restart docker containers
	@$(MAKE) docker-down
	@$(MAKE) docker-build
	@$(MAKE) docker-up
