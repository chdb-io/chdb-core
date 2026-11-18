/* Shard 1: nothing but a registration. Nothing references this shard, so a plain -l link
 * includes it only if the shard anchors pull it in. */
#include "reg.h"

__attribute__((constructor)) static void reg_1(void) { reg_add("reg1"); }
