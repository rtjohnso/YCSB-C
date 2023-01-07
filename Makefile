CC=g++
CFLAGS=-std=c++17 -g -Wall -pthread -I./
LDFLAGS=-L /usr/local/lib -lc++ -lm -ltbb -lsplinterdb -lrocksdb -lxxhash -lhiredis -lz -lbz2 -lpmem

# CFLAGS += -fsanitize=memory -fsanitize-memory-track-origins
# LDFLAGS += -fsanitize=memory

SUBDIRS=core db
SUBCPPSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc)
SUBCSRCS=$(wildcard core/*.c) $(wildcard db/*.c)
OBJECTS=$(SUBCPPSRCS:.cc=.o) $(SUBCSRCS:.c=.o)
EXEC=ycsbc

all: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	$(MAKE) -C $@

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(EXEC)

.PHONY: $(SUBDIRS) $(EXEC)

