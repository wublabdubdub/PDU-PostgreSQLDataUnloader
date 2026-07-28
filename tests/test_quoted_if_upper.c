#include <stdio.h>
#include <stdlib.h>
#include <string.h>

char *quotedIfUpper(const char *input);
char *xman2Insertxman(char *xman, char *tablename);

static int
check_identifier(const char *input, const char *expected)
{
    char *actual = quotedIfUpper(input);
    int failed = 0;

    if (actual == NULL) {
        fprintf(stderr, "quotedIfUpper returned NULL for <%s>\n", input);
        return 1;
    }
    if (strcmp(actual, expected) != 0) {
        fprintf(stderr, "quotedIfUpper(<%s>) returned <%s>, expected <%s>\n",
                input, actual, expected);
        failed = 1;
    }
    free(actual);
    return failed;
}

static int
check_insert(char *values, char *tablename, const char *expected)
{
    char *actual = xman2Insertxman(values, tablename);
    int failed = 0;

    if (actual == NULL) {
        fprintf(stderr, "xman2Insertxman returned NULL for table <%s>\n",
                tablename);
        return 1;
    }
    if (strcmp(actual, expected) != 0) {
        fprintf(stderr, "xman2Insertxman returned <%s>, expected <%s>\n",
                actual, expected);
        failed = 1;
    }
    free(actual);
    return failed;
}

int
main(void)
{
    char long_name[512];
    char long_expected[514];
    int failed = 0;

    failed |= quotedIfUpper(NULL) != NULL;
    failed |= check_identifier("", "");
    failed |= check_identifier("item", "item");
    failed |= check_identifier("Item", "\"Item\"");
    failed |= check_identifier("ITEM", "\"ITEM\"");
    failed |= check_identifier("table_123", "table_123");
    failed |= check_insert("1,'data'", "item",
                           "INSERT INTO item VALUES(1,'data');");
    failed |= check_insert("1,'data'", "Item",
                           "INSERT INTO \"Item\" VALUES(1,'data');");

    memset(long_name, 'a', sizeof(long_name) - 1);
    long_name[sizeof(long_name) - 1] = '\0';
    long_name[255] = 'A';
    long_expected[0] = '"';
    memcpy(long_expected + 1, long_name, sizeof(long_name));
    long_expected[sizeof(long_expected) - 2] = '"';
    long_expected[sizeof(long_expected) - 1] = '\0';
    failed |= check_identifier(long_name, long_expected);

    if (failed)
        return 1;
    puts("quotedIfUpper regression tests passed");
    return 0;
}
