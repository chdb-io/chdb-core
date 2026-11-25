/* Shard 2: the registry and its one public entry, deliberately not in the first shard. It
 * registers itself too. */
#include <stdio.h>
#include <string.h>

#include "reg.h"

static const char * names[64];
static int counts[64];
static int used;

void reg_add(const char * name)
{
    for (int i = 0; i < used; i++)
        if (strcmp(names[i], name) == 0)
        {
            counts[i]++;
            return;
        }
    if (used < 64)
    {
        names[used] = name;
        counts[used++] = 1;
    }
}

__attribute__((constructor)) static void reg_2(void) { reg_add("reg2"); }

/* Sorted by name: the order constructors run in is the linker's business. */
int reg_report(char * out, int size)
{
    int order[64], at = 0;
    for (int i = 0; i < used; i++)
    {
        int j = i;
        for (; j > 0 && strcmp(names[order[j - 1]], names[i]) > 0; j--)
            order[j] = order[j - 1];
        order[j] = i;
    }
    out[0] = '\0';
    for (int i = 0; i < used && at < size; i++)
        at += snprintf(out + at, size - at, "%s%s=%d", i ? "," : "", names[order[i]], counts[order[i]]);
    return used;
}
