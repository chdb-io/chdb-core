#pragma once

/* The fixture's exception type. Its typeinfo is emitted in both shards that use it. */
struct FxError
{
    int value;
};

void fx_throw_deep(int depth);
extern int fx_destroyed;
