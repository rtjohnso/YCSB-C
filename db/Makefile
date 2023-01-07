CC=g++
CFLAGS=-std=c++11 -c -g -O3 -Wall -D SPLINTERDB_PLATFORM_DIR=platform_linux

# CFLAGS += -fsanitize=memory -fsanitize-memory-track-origins
# LDFLAGS += -fsanitize=memory

INCLUDES=-I ../
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	$(RM) $(OBJECTS)

