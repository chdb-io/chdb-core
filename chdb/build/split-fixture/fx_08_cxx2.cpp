/* Shard 8: another instantiation of the same inline counter. */
#include "fx.h"
#include "fx_counter.hpp"

extern "C" int fx_bump_2(void) { return fx_bump(); }
