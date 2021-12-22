package common

import "errors"

var (
	ERR_LSMT_CLOSED            = errors.New("lsmt is closed")
	ERR_KEY_NIL_OR_EMPTY       = errors.New("key must not be nil and must not be empty")
	ERR_VAL_NIL_OR_EMPTY       = errors.New("value must not be nil and must not be empty")
	ERR_KEY_TOO_LARGE          = errors.New("key must not be greater than the maximum key size")
	ERR_VAL_TOO_LARGE          = errors.New("value must not be greater than the maximum value size")
	ERR_START_NIL_OR_EMPTY     = errors.New("start must not be nil and must not be empty")
	ERR_END_NIL_OR_EMPTY       = errors.New("end must not be nil and must not be empty")
	ERR_START_GREATER_THAN_END = errors.New("start must be less than end")
	ERR_NIL_ITERATOR           = errors.New("unable to flush nil iterator")
)
