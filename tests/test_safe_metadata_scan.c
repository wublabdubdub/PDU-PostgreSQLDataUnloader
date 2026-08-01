#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pdu_safe.h"

static int test_valid_fields(void)
{
    FILE *file = tmpfile();
    char oid[4];
    char name[8];
    PduScanField fields[] = {
        PDU_SCAN_FIELD(oid),
        PDU_SCAN_FIELD(name)
    };

    if (file == NULL) {
        return 0;
    }
    fputs("123\tpublic\n", file);
    rewind(file);

    if (pdu_scan_fields(file, fields, PDU_SCAN_FIELD_COUNT(fields)) != 2 ||
        strcmp(oid, "123") != 0 || strcmp(name, "public") != 0) {
        fclose(file);
        return 0;
    }

    fclose(file);
    return 1;
}

static int test_oversized_field(void)
{
    FILE *file = tmpfile();
    char guarded[6] = { 'L', 0, 0, 0, 0, 'R' };
    PduScanField fields[] = {
        { guarded + 1, 4 }
    };

    if (file == NULL) {
        return 0;
    }
    fputs("toolong\n", file);
    rewind(file);

    if (pdu_scan_fields(file, fields, PDU_SCAN_FIELD_COUNT(fields)) != -1 ||
        guarded[0] != 'L' || guarded[5] != 'R' ||
        strcmp(guarded + 1, "too") != 0) {
        fclose(file);
        return 0;
    }

    fclose(file);
    return 1;
}

static int test_missing_field(void)
{
    FILE *file = tmpfile();
    char oid[8];
    char name[8];
    PduScanField fields[] = {
        PDU_SCAN_FIELD(oid),
        PDU_SCAN_FIELD(name)
    };

    if (file == NULL) {
        return 0;
    }
    fputs("123\n", file);
    rewind(file);

    if (pdu_scan_fields(file, fields, PDU_SCAN_FIELD_COUNT(fields)) != 1) {
        fclose(file);
        return 0;
    }

    fclose(file);
    return 1;
}

static int test_line_counting(void)
{
    FILE *file = tmpfile();
    int line_count = -1;

    if (file == NULL) {
        return 0;
    }
    fputs("one\r\ntwo\nthree", file);
    rewind(file);

    if (pdu_count_file_lines(file, &line_count) != 0 || line_count != 3 ||
        fgetc(file) != 'o') {
        fclose(file);
        return 0;
    }

    fclose(file);
    return 1;
}

int main(void)
{
    if (!test_valid_fields() || !test_oversized_field() ||
        !test_missing_field() || !test_line_counting()) {
        fprintf(stderr, "safe metadata scan regression test failed\n");
        return EXIT_FAILURE;
    }

    puts("safe metadata scan regression tests passed");
    return EXIT_SUCCESS;
}
