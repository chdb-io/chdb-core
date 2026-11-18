/* Shard 2: a constructor that sets state another shard defines. */
extern int fx_state_value;

__attribute__((constructor)) static void fx_set_state(void) { fx_state_value = 42; }
