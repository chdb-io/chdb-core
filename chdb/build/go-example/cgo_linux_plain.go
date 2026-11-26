//go:build linux && chdb_plain_link
// +build linux,chdb_plain_link

package main

/*
#cgo CFLAGS: -I.
// A plain -lchdb, as a consumer that knows nothing of static registrations links it. Only
// the split archive (chdb-io/chdb-core#342) is built with this tag: a ring of anchor symbols
// between its shards must pull every one of them in without --whole-archive, which
// cgo_linux.go would otherwise do for it. The registration checks in chdb_go_example.go
// then fail if a shard was left out.
#cgo LDFLAGS: -L. -lchdb -lc -lm -lrt -lpthread -ldl
*/
import "C"
