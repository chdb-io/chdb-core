/* The registration fixture of test_split_static_lib.sh: registrations made by constructors
 * only, one per shard, as chDB's REGISTER_FUNCTION and friends are made. */
#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/* Internal: called by the constructors. */
void reg_add(const char * name);
/* The one public entry: writes "name=count,..." of every registration into `out`. */
int reg_report(char * out, int size);

#ifdef __cplusplus
}
#endif
