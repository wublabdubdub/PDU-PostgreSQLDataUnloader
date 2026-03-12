CC = gcc
CFLAGS = -fdiagnostics-color=always -std=c99 -g -Wall -Wextra -Wno-unused-parameter -Wno-sign-compare -Wno-missing-field-initializers -Wno-unused-variable -Wno-unused-function -Wno-unused-but-set-variable -Wno-format-truncation
LDFLAGS = -lm -lz -ldl -llz4 -lpthread
SOURCES = decode.c parray.c pdu.c pg_walgettx.c pg_xlogreader.c read.c stringinfo.c tools.c info.c dropscan_fs.c
EXECUTABLE = pdu

all: $(EXECUTABLE)

$(EXECUTABLE): $(SOURCES)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

sanitize: CFLAGS += -fsanitize=address -fno-omit-frame-pointer
sanitize: LDFLAGS += -fsanitize=address
sanitize: $(EXECUTABLE)

clean:
	rm -rf $(EXECUTABLE)
	@echo "Clean complete"

.PHONY: clean all sanitize
