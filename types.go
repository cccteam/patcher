package patcher

import (
	"time"

	"github.com/cccteam/ccc/accesstypes"
	"github.com/cccteam/ccc/patchset"
)

type RowStruct interface {
	New() any
	Type() any
}

type row[T any] struct {
	r *T
}

func NewRowStruct[T any](rowStruct T) RowStruct {
	return &row[T]{r: &rowStruct}
}

func (r *row[T]) New() any {
	return new(T)
}

func (r *row[T]) Type() any {
	return r.r
}

type DataChangeEvent struct {
	TableName   accesstypes.Resource `spanner:"TableName"`
	RowID       string               `spanner:"RowId"`
	EventTime   time.Time            `spanner:"EventTime"`
	EventSource string               `spanner:"EventSource"`
	ChangeSet   string               `spanner:"ChangeSet"`
}

type Mutation struct {
	TableName accesstypes.Resource
	RowStruct RowStruct
	PatchSet  *patchset.PatchSet
}

type DiffElem struct {
	Old any
	New any
}

type cacheEntry struct {
	index int
	tag   string
}
