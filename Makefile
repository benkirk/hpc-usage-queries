# QHist Database Makefile
# Convenience targets for database management and job sync

# full bash login shell requied for our complex make rules
SHELL := /bin/bash --login

# common way to inialize enviromnent across various types of systems
config_env := module load conda >/dev/null 2>&1 || true

PYTHON := python3
SCRIPTS := scripts
DATA_DIR := data

# Default date is today in YYYYMMDD format
DATE ?= $(shell date +%Y%m%d)

.PHONY: help init-db sync-casper sync-derecho sync-all clean

help:
	@echo "QHist Database Management"
	@echo ""
	@echo "Usage:"
	@echo "  make init-db          Create database tables (both machines)"
	@echo "  make sync-casper      Sync Casper jobs for DATE"
	@echo "  make sync-derecho     Sync Derecho jobs for DATE"
	@echo "  make sync-all         Sync both machines for DATE"
	@echo "  make clean            Remove all database files"
	@echo ""
	@echo "Database files:"
	@echo "  $(DATA_DIR)/casper.db   - Casper jobs"
	@echo "  $(DATA_DIR)/derecho.db  - Derecho jobs"
	@echo ""
	@echo "Variables:"
	@echo "  DATE=YYYYMMDD        Date to sync (default: today)"
	@echo "  START=YYYYMMDD       Start date for range sync"
	@echo "  END=YYYYMMDD         End date for range sync"
	@echo ""
	@echo "Examples:"
	@echo "  make sync-derecho DATE=20251121"
	@echo "  make sync-all START=20251101 END=20251121"

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

sync-all: sync-derecho sync-casper

clean:
	@echo "Removing databases..."
	@rm -f $(DATA_DIR)/casper.db $(DATA_DIR)/derecho.db $(DATA_DIR)/qhist.db
	@echo "Done."

# Development targets
.PHONY: test-import dry-run-casper dry-run-derecho

test-import:
	@$(PYTHON) -c "from qhist_db import Job, init_db; print('Import successful')"

dry-run-casper:
	$(PYTHON) $(SCRIPTS)/sync_jobs.py -m casper -d $(DATE) --dry-run -v

dry-run-derecho:
	$(PYTHON) $(SCRIPTS)/sync_jobs.py -m derecho -d $(DATE) --dry-run -v

%: %.yaml
	[ -d $@ ] && mv $@ $@.old && rm -rf $@.old &
	$(config_env) && conda env create --file $< --prefix $@
	$(config_env) && conda activate ./$@ && conda list
	$(config_env) && conda activate ./$@ && conda-tree deptree --small 2>/dev/null || true
	$(config_env) && conda activate ./$@ && pipdeptree --all 2>/dev/null || true

solve-%: %.yaml
	$(config_env) && conda env create --file $< --prefix $@ --dry-run
