/* An inline function with a static counter. The fixture's two units and the host each
 * instantiate it under the same mangled name: the fixture's two copies must be one (COMDAT
 * deduplication inside the library), and the host's must be another. */
#pragma once

inline int & fx_counter()
{
    static int counter = 0;
    return counter;
}

inline int fx_bump()
{
    return ++fx_counter();
}
