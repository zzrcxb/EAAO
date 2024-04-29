CC:=gcc
BIN:=bin

CFLAGS:=-O1 -g -Werror

.PHONY: all

all: bin/timer bin/rdseed

bin/timer: timer.c
	@mkdir -p $(BIN)
	$(CC) $(CFLAGS) -o $@ $<

bin/rdseed: rdseed.c
	@mkdir -p $(BIN)
	$(CC) $(CFLAGS) -o $@ $<

clean:
	rm -rf $(BIN)
