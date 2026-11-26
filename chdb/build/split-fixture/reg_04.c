/* Shard 4: nothing but a registration. Nothing references this shard, so a plain -l link
 * includes it only if the shard anchors pull it in. */
#include "reg.h"

__attribute__((constructor)) static void reg_4(void) { reg_add("reg4"); }
