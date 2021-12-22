package sst

import (
	"testing"

	"github.com/patrickgombert/lsmt/common"
	"github.com/patrickgombert/lsmt/config"
)

func TestUnboundedIterator(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	sink := &config.Sink{BlockSize: 4096, BlockCacheSize: 8192, BlockCacheShards: 1, SSTSize: 524288000, BloomFilterSize: 1024}
	options := &config.Options{Levels: common.EMPTY_LEVELS, Sink: sink, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	flush := newFlush(options, sink, NOMAX)
	flush.accept(&common.Pair{Key: []byte{1}, Value: []byte{1, 1}})
	flush.accept(&common.Pair{Key: []byte{2}, Value: []byte{2, 2}})
	ssts, _ := flush.close()

	sst, _ := OpenSst(ssts[0].file)
	iter, _ := sst.UnboundedIterator()
	defer iter.Close()

	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{1}, []byte{1, 1}, t)
	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{2}, []byte{2, 2}, t)
	common.CompareNext(iter, false, t)
}
