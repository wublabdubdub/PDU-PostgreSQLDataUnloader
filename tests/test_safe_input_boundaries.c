#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "pdu_safe.h"

int main(void)
{
    char destination[8] = "stale";
    int intValue = 0;
    uint32_t uintValue = 0;
    int64_t int64Value = 0;

    assert(pdu_copy_string(destination, sizeof(destination), "1234567") == 0);
    assert(strcmp(destination, "1234567") == 0);
    assert(pdu_copy_string(destination, sizeof(destination), "12345678") == -1);
    assert(destination[0] == '\0');

    assert(pdu_copy_token("  value trailing", destination,
                          sizeof(destination)) == 0);
    assert(strcmp(destination, "value") == 0);
    assert(pdu_copy_token("   ", destination, sizeof(destination)) == -1);
    assert(destination[0] == '\0');
    assert(pdu_copy_token("oversized", destination, sizeof(destination)) == -1);
    assert(destination[0] == '\0');

    assert(pdu_parse_int("2147483647", &intValue) == 0);
    assert(intValue == INT32_MAX);
    assert(pdu_parse_int("2147483648", &intValue) == -1);
    assert(pdu_parse_int("12x", &intValue) == -1);

    assert(pdu_parse_uint32("4294967295", &uintValue) == 0);
    assert(uintValue == UINT32_MAX);
    assert(pdu_parse_uint32("4294967296", &uintValue) == -1);
    assert(pdu_parse_uint32("-1", &uintValue) == -1);

    assert(pdu_parse_int64("9223372036854775807", &int64Value) == 0);
    assert(int64Value == INT64_MAX);
    assert(pdu_parse_int64("9223372036854775808", &int64Value) == -1);

    puts("safe input boundary tests passed");
    return 0;
}
