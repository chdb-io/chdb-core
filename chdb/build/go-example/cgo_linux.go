//go:build linux
// +build linux

package main

/*
#cgo CFLAGS: -I.
// -Wl,--whole-archive is the Linux counterpart of macOS's -Wl,-force_load.
// Required so SQL functions registered only via file-scope static initializers
// (e.g. FunctionMD5.cpp's REGISTER_FUNCTION(MD5)) survive the final cgo->ld
// dead-strip. Without it, ld drops any libchdb.a .o whose symbols are not
// directly referenced by Go cgo code. Symbol-level dead-strip still runs.
// CGO_LDFLAGS_ALLOW must include these patterns; test_go_example.sh exports it.
#cgo LDFLAGS: -L. -Wl,--whole-archive -lchdb -Wl,--no-whole-archive -lc -lm -lrt -lpthread -ldl
*/
import "C"
