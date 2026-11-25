/* Shard 4: the strong definition that overrides shard 3's weak default. The host defines a
   fx_policy() of its own that returns 3. */
__attribute__((noinline)) int fx_policy(void) { return 2; }
