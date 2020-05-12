package lsmt

type Level struct {
}

type Options struct {
	levels               []Level
	memtableMaxiumumSize uint64
}

type lsmt struct {
	options           Options
	activeMemtable    *memtable
	inactiveMemtables []*memtable
}
