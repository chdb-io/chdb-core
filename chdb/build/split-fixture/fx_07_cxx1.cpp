/* Shard 7: one instantiation of the inline counter. */
#include "fx.h"
#include "fx_counter.hpp"

extern "C" int fx_bump_1(void) { return fx_bump(); }
