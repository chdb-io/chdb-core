/* Shard 2: a constructor that sets state another shard defines. It also calls two functions
 * whose code sections have the same name, each in a COMDAT group of its own, which `ld -r`
 * keeps apart, as it does for chDB's: the aarch64 code split must rename them as one. */
extern int fx_state_value;

void fx_dup_one(void);
void fx_dup_two(void);
__asm__(".section .text.fx_dup,\"axG\",%progbits,fx_dup_one,comdat\n"
        ".p2align 2\n.weak fx_dup_one\n.type fx_dup_one,%function\nfx_dup_one: ret\n"
        ".section .text.fx_dup,\"axG\",%progbits,fx_dup_two,comdat\n"
        ".p2align 2\n.weak fx_dup_two\n.type fx_dup_two,%function\nfx_dup_two: ret\n"
        ".text\n");

__attribute__((constructor)) static void fx_set_state(void) {
    fx_dup_one();
    fx_dup_two();
    fx_state_value = 42;
}
