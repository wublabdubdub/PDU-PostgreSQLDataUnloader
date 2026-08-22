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

static int test_trailing_metadata_sentinel(void)
{
    FILE *file = tmpfile();
    int line_count = -1;
    char oid[50];
    char filenode[50];
    char toastoid[50];
    char toastnode[50];
    char namespace_oid[50];
    char table_name[50];
    char attributes[128];
    char types[128];
    char attribute_count[50];
    char modifiers[128];
    char lengths[128];
    char alignments[128];
    PduScanField fields[] = {
        PDU_SCAN_FIELD(oid),
        PDU_SCAN_FIELD(filenode),
        PDU_SCAN_FIELD(toastoid),
        PDU_SCAN_FIELD(toastnode),
        PDU_SCAN_FIELD(namespace_oid),
        PDU_SCAN_FIELD(table_name),
        PDU_SCAN_FIELD(attributes),
        PDU_SCAN_FIELD(types),
        PDU_SCAN_FIELD(attribute_count),
        PDU_SCAN_FIELD(modifiers),
        PDU_SCAN_FIELD(lengths),
        PDU_SCAN_FIELD(alignments)
    };

    if (file == NULL) {
        return 0;
    }
    fputs("16394\t16394\t0\t0\t2200\torders\t"
          "id,customer,amount\tint4,varchar,numeric\t3\t"
          "(),(32),(10.2)\t4,-1,-1\ti,i,i\n\r", file);
    rewind(file);

    if (pdu_count_file_lines(file, &line_count) != 0 || line_count != 1 ||
        pdu_scan_fields(file, fields, PDU_SCAN_FIELD_COUNT(fields)) !=
            (int) PDU_SCAN_FIELD_COUNT(fields) ||
        strcmp(table_name, "orders") != 0 ||
        strcmp(attributes, "id,customer,amount") != 0 ||
        pdu_scan_fields(file, fields, PDU_SCAN_FIELD_COUNT(fields)) != 0) {
        fclose(file);
        return 0;
    }

    fclose(file);
    return 1;
}

static int test_whitespace_only_file(void)
{
    FILE *file = tmpfile();
    int line_count = -1;

    if (file == NULL) {
        return 0;
    }
    fputs("\r\n\r \t\n", file);
    rewind(file);

    if (pdu_count_file_lines(file, &line_count) != 0 || line_count != 0) {
        fclose(file);
        return 0;
    }

    fclose(file);
    return 1;
}

int main(void)
{
    if (!test_valid_fields() || !test_oversized_field() ||
        !test_missing_field() || !test_line_counting() ||
        !test_trailing_metadata_sentinel() ||
        !test_whitespace_only_file()) {
        fprintf(stderr, "safe metadata scan regression test failed\n");
        return EXIT_FAILURE;
    }

    puts("safe metadata scan regression tests passed");
    return EXIT_SUCCESS;
}
