# QHist Database Makefile
# Convenience targets for database management and job sync

PYTHON := python3
SCRIPTS := scripts
DB_PATH := data/qhist.db

# Default date is today in YYYYMMDD format
DATE ?= $(shell date +%Y%m%d)

.PHONY: help init-db sync-casper sync-derecho sync-all clean

help:
	@echo "QHist Database Management"
	@echo ""
	@echo "Usage:"
	@echo "  make init-db          Create database tables"
	@echo "  make sync-casper      Sync Casper jobs for DATE"
	@echo "  make sync-derecho     Sync Derecho jobs for DATE"
	@echo "  make sync-all         Sync both machines for DATE"
	@echo "  make clean            Remove database file"
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
	@echo "Initializing database..."
	@$(PYTHON) -c "from qhist_db import init_db; init_db()"
	@echo "Database created at $(DB_PATH)"

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
	@echo "Removing database..."
	@rm -f $(DB_PATH)
	@echo "Done."

# Development targets
.PHONY: test-import dry-run-casper dry-run-derecho

test-import:
	@$(PYTHON) -c "from qhist_db import CasperJob, DerechoJob, init_db; print('Import successful')"

dry-run-casper:
	$(PYTHON) $(SCRIPTS)/sync_jobs.py -m casper -d $(DATE) --dry-run -v

dry-run-derecho:
	$(PYTHON) $(SCRIPTS)/sync_jobs.py -m derecho -d $(DATE) --dry-run -v
