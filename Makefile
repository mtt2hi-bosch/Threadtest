CC ?= $(CROSS_COMPILE)cc
CFLAGS ?= -O2 -g
CPPFLAGS ?= -D_GNU_SOURCE
LDFLAGS ?=
LDLIBS ?= -pthread

TARGET := threadtest
SRC := src/threadtest.c

.PHONY: all clean test

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CPPFLAGS) $(CFLAGS) -std=c11 -Wall -Wextra -Werror -pedantic -o $@ $(SRC) $(LDFLAGS) $(LDLIBS)

test: $(TARGET)
	sh tests/smoke.sh

clean:
	rm -f $(TARGET) *.o
