// package patcher provides functionality to patch resources
package patcher

import (
	"bytes"
	"context"
	"encoding"
	"fmt"
	"iter"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cccteam/ccc"
	"github.com/cccteam/httpio/columnset"
	"github.com/cccteam/httpio/patchset"
	"github.com/go-playground/errors/v5"
)

type dbType string

const (
	spannerdbType  dbType = "spanner"
	postgresdbType dbType = "postgres"
)

type patcher struct {
	tagName string
	dbType  dbType

	mu    sync.RWMutex
	cache map[reflect.Type]map[string]cacheEntry
}

func (p *patcher) EmptyPatchSet(databaseType any) *patchset.PatchSet {
	fieldTagMapping, err := p.get(databaseType)
	if err != nil {
		panic(err)
	}

	ps := make(map[string]any, len(fieldTagMapping))
	for field := range fieldTagMapping {
		ps[field] = nil
	}

	return patchset.NewPatchSet(ps)
}

// ViewableColumns returns the database struct tags for the fields in databaseType that the user has access to view.
func (p *patcher) ViewableColumns(ctx context.Context, columnSet columnset.ColumnSet, databaseType any) (string, error) {
	fileds, err := columnSet.StructFields(ctx)
	if err != nil {
		return "", errors.Wrap(err, "columnset.ColumnSet.Fields()")
	}

	return p.columns(fileds, databaseType)
}

// PatchSetColumns returns the database struct tags for the field in databaseType if it exists in patchSet.
func (p *patcher) PatchSetColumns(patchSet *patchset.PatchSet, databaseType any) (string, error) {
	fields := patchSet.StructFields()

	return p.columns(fields, databaseType)
}

func (p *patcher) columns(fields []string, databaseType any) (string, error) {
	fieldTagMapping, err := p.get(databaseType)
	if err != nil {
		return "", err
	}

	columnEntries := make([]cacheEntry, 0, len(fields))
	for _, field := range fields {
		c, ok := fieldTagMapping[field]
		if !ok {
			return "", errors.Newf("field %s not found in struct", field)
		}

		columnEntries = append(columnEntries, c)
	}
	sort.Slice(columnEntries, func(i, j int) bool {
		return columnEntries[i].index < columnEntries[j].index
	})

	columns := make([]string, 0, len(columnEntries))
	for _, c := range columnEntries {
		columns = append(columns, c.tag)
	}

	switch p.dbType {
	case spannerdbType:
		return strings.Join(columns, ", "), nil
	case postgresdbType:
		return fmt.Sprintf(`"%s"`, strings.Join(columns, `", "`)), nil
	default:
		return "", errors.Newf("unsupported dbType: %s", p.dbType)
	}
}

// Resolve returns a map with the keys set to the database struct tags found on databaseType, and the values set to the values in patchSet.
func (p *patcher) Resolve(pkeys PrimaryKey, patchSet *patchset.PatchSet, databaseType any) (map[string]any, error) {
	if len(pkeys.keyParts) == 0 {
		return nil, errors.New("must include at least one primary key in call to Resolve")
	}

	fieldTagMapping, err := p.get(databaseType)
	if err != nil {
		return nil, err
	}

	newMap := make(map[string]any, len(pkeys.keyParts)+patchSet.Len())
	for structField, value := range all(patchSet.Data(), pkeys.Map()) {
		c, ok := fieldTagMapping[structField]
		if !ok {
			return nil, errors.Newf("field %s not found in struct", structField)
		}
		newMap[c.tag] = value
	}

	return newMap, nil
}

// Diff returns a map of fields that have changed between old and patchSet.
func (p *patcher) Diff(old any, patchSet *patchset.PatchSet) (map[string]DiffElem, error) {
	oldValue := reflect.ValueOf(old)
	oldType := reflect.TypeOf(old)

	if oldValue.Kind() == reflect.Pointer {
		oldValue = oldValue.Elem()
	}

	if oldType.Kind() == reflect.Pointer {
		oldType = oldType.Elem()
	}

	if kind := oldType.Kind(); kind != reflect.Struct {
		return nil, errors.Newf("Patcher.Diff(): old must be of kind struct, found kind %s", kind.String())
	}

	oldMap := map[string]any{}
	for _, field := range reflect.VisibleFields(oldType) {
		oldMap[field.Name] = oldValue.FieldByName(field.Name).Interface()
	}

	diff := map[string]DiffElem{}
	for field, newV := range patchSet.Data() {
		oldV, foundInOld := oldMap[field]
		if !foundInOld {
			return nil, errors.Newf("Patcher.Diff(): field %s in patchSet does not exist in old", field)
		}

		if match, err := match(oldV, newV); err != nil {
			return nil, err
		} else if !match {
			diff[field] = DiffElem{
				Old: oldV,
				New: newV,
			}
		}
	}

	return diff, nil
}

func (p *patcher) deleteChangeSet(old any) (map[string]DiffElem, error) {
	oldValue := reflect.ValueOf(old)
	oldType := reflect.TypeOf(old)

	if oldValue.Kind() == reflect.Pointer {
		oldValue = oldValue.Elem()
	}

	if oldType.Kind() == reflect.Pointer {
		oldType = oldType.Elem()
	}

	if kind := oldType.Kind(); kind != reflect.Struct {
		return nil, errors.Newf("Patcher.Diff(): old must be of kind struct, found kind %s", kind.String())
	}

	oldMap := map[string]DiffElem{}
	for _, field := range reflect.VisibleFields(oldType) {
		oldValue := oldValue.FieldByName(field.Name)
		if oldValue.IsValid() && !oldValue.IsZero() {
			oldMap[field.Name] = DiffElem{
				Old: oldValue.Interface(),
			}
		}
	}

	return oldMap, nil
}

func (p *patcher) get(v any) (map[string]cacheEntry, error) {
	p.mu.RLock()

	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if tagMap, ok := p.cache[t]; ok {
		defer p.mu.RUnlock()

		return tagMap, nil
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()

	if tagMap, ok := p.cache[t]; ok {
		return tagMap, nil
	}

	if t.Kind() != reflect.Struct {
		return nil, errors.Newf("expected struct, got %s", t.Kind())
	}

	p.cache[t] = structTags(t, p.tagName)

	return p.cache[t], nil
}

// all returns an iterator over key-value pairs from m.
//   - all is a similar to maps.All but it takes a variadic
//   - duplicate keys will not be deduped and will be yielded once for each duplication
func all[Map ~map[K]V, K comparable, V any](maps ...Map) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for _, m := range maps {
			for k, v := range m {
				if !yield(k, v) {
					return
				}
			}
		}
	}
}

func structTags(t reflect.Type, key string) map[string]cacheEntry {
	tagMap := make(map[string]cacheEntry)
	for i := range t.NumField() {
		field := t.Field(i)
		tag := field.Tag.Get(key)

		list := strings.Split(tag, ",")
		if len(list) == 0 || list[0] == "" || list[0] == "-" {
			continue
		}

		tagMap[field.Name] = cacheEntry{index: i, tag: list[0]}
	}

	return tagMap
}

func match(v, v2 any) (matched bool, err error) {
	switch t := v.(type) {
	case int:
		return matchPrimitive(t, v2)
	case *int:
		return matchPrimitivePtr(t, v2)
	case []int:
		return matchSlice(t, v2)
	case []*int:
		return matchSlice(t, v2)
	case int8:
		return matchPrimitive(t, v2)
	case *int8:
		return matchPrimitivePtr(t, v2)
	case []int8:
		return matchSlice(t, v2)
	case []*int8:
		return matchSlice(t, v2)
	case int16:
		return matchPrimitive(t, v2)
	case *int16:
		return matchPrimitivePtr(t, v2)
	case []int16:
		return matchSlice(t, v2)
	case []*int16:
		return matchSlice(t, v2)
	case int32:
		return matchPrimitive(t, v2)
	case *int32:
		return matchPrimitivePtr(t, v2)
	case []int32:
		return matchSlice(t, v2)
	case []*int32:
		return matchSlice(t, v2)
	case int64:
		return matchPrimitive(t, v2)
	case *int64:
		return matchPrimitivePtr(t, v2)
	case []int64:
		return matchSlice(t, v2)
	case []*int64:
		return matchSlice(t, v2)
	case uint:
		return matchPrimitive(t, v2)
	case *uint:
		return matchPrimitivePtr(t, v2)
	case []uint:
		return matchSlice(t, v2)
	case []*uint:
		return matchSlice(t, v2)
	case uint8:
		return matchPrimitive(t, v2)
	case *uint8:
		return matchPrimitivePtr(t, v2)
	case []uint8:
		return matchSlice(t, v2)
	case []*uint8:
		return matchSlice(t, v2)
	case uint16:
		return matchPrimitive(t, v2)
	case *uint16:
		return matchPrimitivePtr(t, v2)
	case []uint16:
		return matchSlice(t, v2)
	case []*uint16:
		return matchSlice(t, v2)
	case uint32:
		return matchPrimitive(t, v2)
	case *uint32:
		return matchPrimitivePtr(t, v2)
	case []uint32:
		return matchSlice(t, v2)
	case []*uint32:
		return matchSlice(t, v2)
	case uint64:
		return matchPrimitive(t, v2)
	case *uint64:
		return matchPrimitivePtr(t, v2)
	case []uint64:
		return matchSlice(t, v2)
	case []*uint64:
		return matchSlice(t, v2)
	case float32:
		return matchPrimitive(t, v2)
	case *float32:
		return matchPrimitivePtr(t, v2)
	case []float32:
		return matchSlice(t, v2)
	case []*float32:
		return matchSlice(t, v2)
	case float64:
		return matchPrimitive(t, v2)
	case *float64:
		return matchPrimitivePtr(t, v2)
	case []float64:
		return matchSlice(t, v2)
	case []*float64:
		return matchSlice(t, v2)
	case string:
		return matchPrimitive(t, v2)
	case *string:
		return matchPrimitivePtr(t, v2)
	case []string:
		return matchSlice(t, v2)
	case []*string:
		return matchSlice(t, v2)
	case bool:
		return matchPrimitive(t, v2)
	case *bool:
		return matchPrimitivePtr(t, v2)
	case []bool:
		return matchSlice(t, v2)
	case []*bool:
		return matchSlice(t, v2)
	case time.Time:
		switch t2 := v2.(type) {
		case time.Time:
			return matchTextMarshaler(t, t2)
		default:
			return false, errors.Newf("match(): attempted to diff incomparable types, old: %T, new: %T", v, v2)
		}
	case *time.Time:
		switch t2 := v2.(type) {
		case *time.Time:
			return matchTextMarshalerPtr(t, t2)
		default:
			return false, errors.Newf("match(): attempted to diff incomparable types, old: %T, new: %T", v, v2)
		}
	case ccc.UUID:
		switch t2 := v2.(type) {
		case ccc.UUID:
			return matchTextMarshaler(t, t2)
		default:
			return false, errors.Newf("match(): attempted to diff incomparable types, old: %T, new: %T", v, v2)
		}
	case *ccc.UUID:
		switch t2 := v2.(type) {
		case *ccc.UUID:
			return matchTextMarshalerPtr(t, t2)
		default:
			return false, errors.Newf("match(): attempted to diff incomparable types, old: %T, new: %T", v, v2)
		}
	case ccc.NullUUID:
		switch t2 := v2.(type) {
		case ccc.NullUUID:
			return matchTextMarshaler(t, t2)
		default:
			return false, errors.Newf("match(): attempted to diff incomparable types, old: %T, new: %T", v, v2)
		}
	}

	if reflect.TypeOf(v) != reflect.TypeOf(v2) {
		return false, errors.Newf("attempted to compare values having a different type, v.(type) = %T, v2.(type) = %T", v, v2)
	}

	return reflect.DeepEqual(v, v2), nil
}

func matchSlice[T comparable](v []T, v2 any) (bool, error) {
	t2, ok := v2.([]T)
	if !ok {
		return false, errors.Newf("matchSlice(): attempted to diff incomparable types, old: %T, new: %T", v, v2)
	}
	if len(v) != len(t2) {
		return false, nil
	}

	for i := range v {
		if match, err := match(v[i], t2[i]); err != nil {
			return false, err
		} else if !match {
			return false, nil
		}
	}

	return true, nil
}

func matchPrimitive[T comparable](v T, v2 any) (bool, error) {
	t2, ok := v2.(T)
	if !ok {
		return false, errors.Newf("matchPrimitive(): attempted to diff incomparable types, old: %T, new: %T", v, v2)
	}
	if v == t2 {
		return true, nil
	}

	return false, nil
}

func matchPrimitivePtr[T comparable](v *T, v2 any) (bool, error) {
	t2, ok := v2.(*T)
	if !ok {
		return false, errors.Newf("matchPrimitivePtr(): attempted to diff incomparable types, old: %T, new: %T", v, v2)
	}
	if v == nil || t2 == nil {
		if v == nil && t2 == nil {
			return true, nil
		}

		return false, nil
	}
	if *v == *t2 {
		return true, nil
	}

	return false, nil
}

func matchTextMarshalerPtr[T encoding.TextMarshaler](v, v2 *T) (bool, error) {
	if v == nil || v2 == nil {
		if v == nil && v2 == nil {
			return true, nil
		}

		return false, nil
	}

	return matchTextMarshaler(*v, *v2)
}

func matchTextMarshaler[T encoding.TextMarshaler](v, v2 T) (bool, error) {
	vText, err := v.MarshalText()
	if err != nil {
		return false, errors.Wrap(err, "encoding.TextMarshaler.MarshalText()")
	}

	v2Text, err := v2.MarshalText()
	if err != nil {
		return false, errors.Wrap(err, "encoding.TextMarshaler.MarshalText()")
	}

	if bytes.Equal(vText, v2Text) {
		return true, nil
	}

	return false, nil
}
