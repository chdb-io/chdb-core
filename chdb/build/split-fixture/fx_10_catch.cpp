/* Shard 10: catches what shard 9 throws. */
#include "fx.h"
#include "fx_error.hpp"

/* value * 100 + the number of frames unwound: 7 * 100 + 4 for fx_throw_deep(3). */
extern "C" int fx_catch(void)
{
    fx_destroyed = 0;
    try
    {
        fx_throw_deep(3);
    }
    catch (const FxError & e)
    {
        return e.value * 100 + fx_destroyed;
    }
    return -1;
}
