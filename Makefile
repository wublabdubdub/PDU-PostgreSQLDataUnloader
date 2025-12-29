CC = gcc
CFLAGS = -fdiagnostics-color=always -std=c99 -g
LDFLAGS = -lm -lz -ldl -llz4 -lpthread
SOURCES = decode.c parray.c pdu.c pg_walgettx.c pg_xlogreader.c read.c stringinfo.c tools.c info.c dropscan_fs.c
EXECUTABLE = pdu

all: $(EXECUTABLE)

$(EXECUTABLE): $(SOURCES)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

clean:
	rm -rf $(EXECUTABLE)
	@echo "Clean complete"

.PHONY: clean all
