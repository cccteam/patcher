package patcher

import (
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/cccteam/httpio/patchset"
)

type RowStruct interface {
	New() any
	Type() any
}

type row[T any] struct {
	r *T
}

func NewRowStruct[T any](tableStruct T) RowStruct {
	return &row[T]{r: &tableStruct}
}

func (r *row[T]) New() any {
	return new(T)
}

func (r *row[T]) Type() any {
	return r.r
}

type DataChangeEvent struct {
	TableName   string    `spanner:"TableName"`
	RowID       string    `spanner:"RowId"`
	EventTime   time.Time `spanner:"EventTime"`
	EventSource string    `spanner:"EventSource"`
	ChangeSet   string    `spanner:"ChangeSet"`
}

type Event struct {
	TableName   string
	RowStruct   RowStruct
	PrimaryKeys PrimaryKey
	PatchSet    *patchset.PatchSet
}

type keyPart struct {
	key   string
	value any
}

// PrimaryKey is an object that represents a single or composite primary key and its value.
type PrimaryKey struct {
	keyParts []keyPart
}

func NewPrimaryKey(key string, value any) PrimaryKey {
	return PrimaryKey{
		keyParts: []keyPart{
			{key: key, value: value},
		},
	}
}

// Add adds an additional column to the primary key creating a composite primary key
//   - PrimaryKey is immutable.
//   - Add returns a new PrimaryKey that should be used for all subsequent operations.
func (p PrimaryKey) Add(key string, value any) PrimaryKey {
	p.keyParts = append(p.keyParts, keyPart{
		key:   key,
		value: value,
	})

	return p
}

func (p PrimaryKey) RowID() string {
	var id strings.Builder
	for _, v := range p.keyParts {
		id.WriteString(fmt.Sprintf("|%v", v.value))
	}

	return id.String()[1:]
}

func (p PrimaryKey) KeySet() spanner.KeySet {
	keys := make([]any, 0, len(p.keyParts))
	for _, v := range p.keyParts {
		keys = append(keys, v.value)
	}

	return spanner.Key{keys}
}

func (p PrimaryKey) Map() map[string]any {
	pKeyMap := make(map[string]any)
	for _, keypart := range p.keyParts {
		pKeyMap[keypart.key] = keypart.value
	}

	return pKeyMap
}

type DiffElem struct {
	Old any
	New any
}

type cacheEntry struct {
	index int
	tag   string
}
