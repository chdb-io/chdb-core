//go:build darwin
// +build darwin

package main

/*
#cgo CFLAGS: -I.
// -Wl,-force_load is required on macOS so SQL functions registered only via
// file-scope static initializers (e.g. FunctionMD5.cpp's REGISTER_FUNCTION(MD5))
// survive the final cgo->ld dead-strip. Without it, ld drops any libchdb.a .o
// whose symbols are not directly referenced by Go cgo code, even though dyld
// would otherwise run the static initializer at library load. Symbol-level
// dead-strip still runs, so binary size grows by only ~40 KB.
// 12.0 is the first deployment target for which ld emits chained fixups. Below it this
// link produces none, so it cannot catch bundled-runtime symbols coalescing with the
// system libc++/libc++abi - and with -force_load this binary carries the whole archive,
// making it the most exposed consumer we test.
#cgo LDFLAGS: -mmacosx-version-min=12.0 -L. -Wl,-force_load,./libchdb.a -liconv -framework CoreFoundation -framework Security
*/
import "C"
