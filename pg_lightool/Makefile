#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_lightool
#
# src/bin/pg_lightool/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_lightool,my light tools of pg"

PROGRAM = pg_lightool
SRCS = \
	pg_lightool.c util.c fileoperat.c xlogparser.c xlogread.c
OBJS = $(SRCS:.c=.o)


PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

$(OBJS): pg_lightool.h