/* The host of the registration fixture: references the one public entry and nothing else,
 * and checks that every shard's registration ran, once each. */
#include <stdio.h>
#include <string.h>

#include "reg.h"

int main(int argc, char ** argv)
{
    const char * expected = argc > 1 ? argv[1] : "";
    char report[512];
    reg_report(report, sizeof(report));
    if (strcmp(report, expected) != 0)
    {
        fprintf(stderr, "host_reg: registrations \"%s\", expected \"%s\"\n", report, expected);
        return 1;
    }
    printf("host_reg: %s\n", report);
    return 0;
}
