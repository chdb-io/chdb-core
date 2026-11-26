/* Shard 5: a caller of fx_policy() in a shard that defines neither copy. */
#include "fx.h"

int fx_policy(void);

int fx_get_policy(void) { return fx_policy(); }
