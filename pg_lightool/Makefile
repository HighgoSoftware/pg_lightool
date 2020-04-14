#-------------------------------------------------------------------------
#
# Identification: 
# Makefile
#
# Copyright:
# Copyright (c) 2017-2020, HighGo Software Co.,Ltd. All right reserved
#
# Authored by lichuancheng@highgo.com ,20180821
#
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_lightool,my light tools of pg"

PROGRAM = pg_lightool
SRCS = \
	backup_utils.c pg_lightool.c util.c fileoperat.c xlogparser.c \
	xlogread.c pageread.c
OBJS = $(SRCS:.c=.o)


PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

$(OBJS): pg_lightool.h