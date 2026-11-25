/* Shard 9: throws through frames whose objects must be destroyed on the way out. */
#include "fx_error.hpp"

int fx_destroyed = 0;

namespace
{
struct Guard
{
    ~Guard() { ++fx_destroyed; }
};
}

[[gnu::noinline]] void fx_throw_deep(int depth)
{
    Guard guard;
    if (depth == 0)
        throw FxError{7};
    fx_throw_deep(depth - 1);
}
