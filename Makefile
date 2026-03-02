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

.PHONY: help init-db sync-casper sync-derecho sync-all clean update-vendor

help:
	@echo "QHist Database Management"
	@echo ""
	@echo "Usage:"
	@echo "  make init-db          Create database tables (both machines)"
	@echo "  make sync-casper      Sync Casper jobs for DATE"
	@echo "  make sync-derecho     Sync Derecho jobs for DATE"
	@echo "  make sync-all         Sync both machines for DATE"
	@echo "  make clean            Remove all database files"
	@echo "  make update-vendor    Re-download vendored third-party files"
	@echo ""
	@echo "Database files:"
	@echo "  $(JOB_HISTORY_DATA_DIR)/casper.db   - Casper jobs"
	@echo "  $(JOB_HISTORY_DATA_DIR)/derecho.db  - Derecho jobs"
	@echo ""
	@echo "Variables:"
	@echo "  DATE=YYYY-MM-DD        Date to sync (default: today)"
	@echo "  START=YYYY-MM-DD       Start date for range sync"
	@echo "  END=YYYY-MM-DD         End date for range sync"
	@echo "  LOG_DIR=<path>         Root of PBS log mirror (default: $(LOG_DIR))"
	@echo ""
	@echo "Examples:"
	@echo "  make sync-derecho DATE=2025-11-21"
	@echo "  make sync-all START=2025-11-01 END=2025-11-21"

init-db:
	@echo "Initializing databases..."
	@$(PYTHON) -c "from job_history import init_db; init_db()"
	@echo "Created $(JOB_HISTORY_DATA_DIR)/casper.db"
	@echo "Created $(JOB_HISTORY_DATA_DIR)/derecho.db"

sync-casper:
ifdef START
	jobhist sync -m casper -l $(LOG_DIR)/casper --start $(START) $(if $(END),--end $(END)) -v
else
	jobhist sync -m casper -l $(LOG_DIR)/casper -d $(DATE) -v
endif

sync-derecho:
ifdef START
	jobhist sync -m derecho -l $(LOG_DIR)/derecho --start $(START) $(if $(END),--end $(END)) -v
else
	jobhist sync -m derecho -l $(LOG_DIR)/derecho -d $(DATE) -v
endif

sync-all: sync-casper sync-derecho

clean:
	@echo "Removing databases..."
	@rm -f $(JOB_HISTORY_DATA_DIR)/casper.db $(JOB_HISTORY_DATA_DIR)/derecho.db $(JOB_HISTORY_DATA_DIR)/qhist.db
	@echo "Done."

# Development targets
.PHONY: test-import dry-run-casper dry-run-derecho dry-run-all

test-import:
	@$(PYTHON) -c "from job_history import Job, init_db; print('Import successful')"

dry-run-casper:
	jobhist sync -m casper -l $(LOG_DIR)/casper -d $(DATE) --dry-run -v

dry-run-derecho:
	jobhist sync -m derecho -l $(LOG_DIR)/derecho -d $(DATE) --dry-run -v

dry-run-all: dry-run-casper dry-run-derecho

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


sync-logs:
	rsync -axv derecho:/ncar/pbs/accounting/20* ./data/sample_pbs_logs/derecho/
	rsync -axv casper:/ssg/pbs/casper/accounting/2026* ./data/sample_pbs_logs/casper/

update-vendor:
	@echo "Updating vendored pbs_parser_ncar/ncar.py..."
	@curl -sSf https://raw.githubusercontent.com/NCAR/pbs_parser_ncar/main/ncar.py \
	    -o job_history/_vendor/pbs_parser_ncar/ncar.py
	@printf '# Vendored from https://github.com/NCAR/pbs_parser_ncar\n# %s\n# Local modifications: none\n' \
	    "$$(date +%Y-%m-%d)" > job_history/_vendor/pbs_parser_ncar/README
	@echo "Done. Review with: git diff job_history/_vendor/"
