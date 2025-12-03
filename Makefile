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
DATA_DIR := data

# Default date is today in YYYY-MM-DD format
DATE ?= $(shell date +%Y-%m-%d)

.PHONY: help init-db sync-casper sync-derecho sync-all clean

help:
	@echo "QHist Database Management"
	@echo ""
	@echo "Usage:"
	@echo "  make init-db          Create database tables (both machines)"
	@echo "  make sync-casper      Sync Casper jobs for DATE"
	@echo "  make sync-derecho     Sync Derecho jobs for DATE"
	@echo "  make sync-all         Sync both machines for DATE (uses --machine all)"
	@echo "  make clean            Remove all database files"
	@echo ""
	@echo "Database files:"
	@echo "  $(DATA_DIR)/casper.db   - Casper jobs"
	@echo "  $(DATA_DIR)/derecho.db  - Derecho jobs"
	@echo ""
	@echo "Variables:"
	@echo "  DATE=YYYY-MM-DD        Date to sync (default: today)"
	@echo "  START=YYYY-MM-DD       Start date for range sync"
	@echo "  END=YYYY-MM-DD         End date for range sync"
	@echo ""
	@echo "Examples:"
	@echo "  make sync-derecho DATE=2025-11-21"
	@echo "  make sync-all START=2025-11-01 END=2025-11-21"

init-db:
	@echo "Initializing databases..."
	@$(PYTHON) -c "from qhist_db import init_db; init_db()"
	@echo "Created $(DATA_DIR)/casper.db"
	@echo "Created $(DATA_DIR)/derecho.db"

sync-casper:
ifdef START
	$(PYTHON) $(SCRIPTS)/sync_jobs.py -m casper --start $(START) $(if $(END),--end $(END)) -v
else
	$(PYTHON) $(SCRIPTS)/sync_jobs.py -m casper -d $(DATE) -v
endif

sync-derecho:
ifdef START
	$(PYTHON) $(SCRIPTS)/sync_jobs.py -m derecho --start $(START) $(if $(END),--end $(END)) -v
else
	$(PYTHON) $(SCRIPTS)/sync_jobs.py -m derecho -d $(DATE) -v
endif

sync-all:
ifdef START
	$(PYTHON) $(SCRIPTS)/sync_jobs.py -m all --start $(START) $(if $(END),--end $(END)) -v
else
	$(PYTHON) $(SCRIPTS)/sync_jobs.py -m all -d $(DATE) -v
endif

clean:
	@echo "Removing databases..."
	@rm -f $(DATA_DIR)/casper.db $(DATA_DIR)/derecho.db $(DATA_DIR)/qhist.db
	@echo "Done."

# Development targets
.PHONY: test-import dry-run-casper dry-run-derecho dry-run-all

test-import:
	@$(PYTHON) -c "from qhist_db import Job, init_db; print('Import successful')"

dry-run-casper:
	$(PYTHON) $(SCRIPTS)/sync_jobs.py -m casper -d $(DATE) --dry-run -v

dry-run-derecho:
	$(PYTHON) $(SCRIPTS)/sync_jobs.py -m derecho -d $(DATE) --dry-run -v

dry-run-all:
	$(PYTHON) $(SCRIPTS)/sync_jobs.py -m all -d $(DATE) --dry-run -v

%: %.yaml
	$(config_env)
	[ -d $@ ] && mv $@ $@.old && rm -rf $@.old &
	conda env create --file $< --prefix $@
	conda activate ./$@
	conda list
	pip install -e ".[dev]"
	pipdeptree --all 2>/dev/null || true

solve-%: %.yaml
	$(config_env)
	conda env create --file $< --prefix $@ --dry-run
