MODULES = dp_best_path
EXTENSION = dp_best_path
DATA = dp_best_path--1.0.sql
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
