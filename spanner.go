package patcher

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/cccteam/ccc/accesstypes"
	"github.com/cccteam/ccc/patchset"
	"github.com/cccteam/httpio"
	"github.com/cccteam/spxscan"
	"github.com/go-playground/errors/v5"
)

type SpannerPatcher struct {
	changeTrackingTable string
	*patcher
}

func NewSpannerPatcher() *SpannerPatcher {
	return &SpannerPatcher{
		changeTrackingTable: "DataChangeEvents",
		patcher: &patcher{
			cache:   make(map[reflect.Type]map[accesstypes.Field]cacheEntry),
			tagName: "spanner",
			dbType:  spannerdbType,
		},
	}
}

func (p *SpannerPatcher) WithDataChangeTableName(tableName string) *SpannerPatcher {
	p.changeTrackingTable = tableName

	return p
}

func (p *SpannerPatcher) Insert(ctx context.Context, s *spanner.Client, event *Event) error {
	mutations := []*spanner.Mutation{}

	mutation, err := p.RowInsertMutation(event)
	if err != nil {
		return err
	}
	mutations = append(mutations, mutation)

	if _, err := s.Apply(ctx, mutations); err != nil {
		return errors.Wrap(err, "spanner.Client.Apply()")
	}

	return nil
}

func (p *SpannerPatcher) Update(ctx context.Context, s *spanner.Client, event *Event) error {
	mutations := []*spanner.Mutation{}

	mutation, err := p.RowUpdateMutation(event)
	if err != nil {
		return err
	}
	mutations = append(mutations, mutation)

	if _, err := s.Apply(ctx, mutations); err != nil {
		if errors.Is(err, spxscan.ErrNotFound) {
			return httpio.NewNotFoundMessagef("%s %q not found", event.TableName, event.PrimaryKeys.RowID())
		}

		return errors.Wrap(err, "spanner.Client.Apply()")
	}

	return nil
}

func (p *SpannerPatcher) Delete(ctx context.Context, s *spanner.Client, event *Event) error {
	mutations := []*spanner.Mutation{}

	mutation, err := p.RowDeleteMutation(event)
	if err != nil {
		return err
	}
	mutations = append(mutations, mutation)

	if _, err := s.Apply(ctx, mutations); err != nil {
		if errors.Is(err, spxscan.ErrNotFound) {
			return httpio.NewNotFoundMessagef("%s %q not found", event.TableName, event.PrimaryKeys.RowID())
		}

		return errors.Wrap(err, "spanner.Client.Apply()")
	}

	return nil
}

func (p *SpannerPatcher) RowInsertMutation(event *Event) (*spanner.Mutation, error) {
	patch, err := p.Resolve(event.PrimaryKeys, event.PatchSet, event.RowStruct.Type())
	if err != nil {
		return nil, errors.Wrap(err, "Resolve()")
	}
	mutation := spanner.InsertMap(string(event.TableName), patch)

	return mutation, nil
}

func (p *SpannerPatcher) RowUpdateMutation(event *Event) (*spanner.Mutation, error) {
	patch, err := p.Resolve(event.PrimaryKeys, event.PatchSet, event.RowStruct.Type())
	if err != nil {
		return nil, errors.Wrap(err, "Resolve()")
	}
	mutation := spanner.UpdateMap(string(event.TableName), patch)

	return mutation, nil
}

func (p *SpannerPatcher) RowDeleteMutation(event *Event) (*spanner.Mutation, error) {
	mutation := spanner.Delete(string(event.TableName), event.PrimaryKeys.KeySet())

	return mutation, nil
}

func (p *SpannerPatcher) InsertWithDataChangeEvent(ctx context.Context, s *spanner.Client, eventSource string, event *Event) error {
	if _, err := s.ReadWriteTransaction(ctx, func(_ context.Context, txn *spanner.ReadWriteTransaction) error {
		mutations, err := p.RowAndDataChangeEventInsertMutations(eventSource, event)
		if err != nil {
			return err
		}

		if err := txn.BufferWrite(mutations); err != nil {
			return errors.Wrap(err, "spanner.Client.Apply()")
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return nil
}

func (p *SpannerPatcher) UpdateWithDataChangeEvent(ctx context.Context, s *spanner.Client, eventSource string, event *Event) error {
	if _, err := s.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		mutations, err := p.RowAndDataChangeEventUpdateMutations(ctx, txn, eventSource, event)
		if err != nil {
			return err
		}

		if err := txn.BufferWrite(mutations); err != nil {
			return errors.Wrap(err, "spanner.Client.Apply()")
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return nil
}

func (p *SpannerPatcher) DeleteWithDataChangeEvent(ctx context.Context, s *spanner.Client, eventSource string, event *Event) error {
	if _, err := s.ReadWriteTransaction(ctx, func(_ context.Context, txn *spanner.ReadWriteTransaction) error {
		mutations, err := p.RowAndDataChangeEventDeleteMutations(ctx, txn, eventSource, event)
		if err != nil {
			return err
		}

		if err := txn.BufferWrite(mutations); err != nil {
			return errors.Wrap(err, "spanner.Client.Apply()")
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return nil
}

func (p *SpannerPatcher) RowAndDataChangeEventInsertMutations(eventSource string, event *Event) ([]*spanner.Mutation, error) {
	mutations := []*spanner.Mutation{}

	mutation, err := p.RowInsertMutation(event)
	if err != nil {
		return nil, err
	}
	mutations = append(mutations, mutation)

	mutation, err = p.dataChangeEventInsertMutation(eventSource, event)
	if err != nil {
		return nil, err
	}
	mutations = append(mutations, mutation)

	return mutations, nil
}

func (p *SpannerPatcher) RowAndDataChangeEventUpdateMutations(ctx context.Context, txn *spanner.ReadWriteTransaction, eventSource string, event *Event) ([]*spanner.Mutation, error) {
	mutations := []*spanner.Mutation{}

	mutation, err := p.RowUpdateMutation(event)
	if err != nil {
		return nil, err
	}
	mutations = append(mutations, mutation)

	mutation, err = p.dataChangeEventUpdateMutation(ctx, txn, eventSource, event)
	if err != nil {
		return nil, err
	}
	mutations = append(mutations, mutation)

	return mutations, nil
}

func (p *SpannerPatcher) RowAndDataChangeEventDeleteMutations(ctx context.Context, txn *spanner.ReadWriteTransaction, eventSource string, event *Event) ([]*spanner.Mutation, error) {
	mutations := []*spanner.Mutation{}

	mutation, err := p.RowDeleteMutation(event)
	if err != nil {
		return nil, err
	}
	mutations = append(mutations, mutation)

	mutation, err = p.dataChangeEventDeleteMutation(ctx, txn, eventSource, event)
	if err != nil {
		return nil, err
	}
	mutations = append(mutations, mutation)

	return mutations, nil
}

func (p *SpannerPatcher) dataChangeEventInsertMutation(eventSource string, event *Event) (*spanner.Mutation, error) {
	jsonChangeSet, err := p.jsonInsertSet(event.PatchSet, event.RowStruct)
	if err != nil {
		return nil, err
	}

	mutation, err := spanner.InsertStruct(p.changeTrackingTable,
		&DataChangeEvent{
			TableName:   event.TableName,
			RowID:       event.PrimaryKeys.RowID(),
			EventTime:   spanner.CommitTimestamp,
			EventSource: eventSource,
			ChangeSet:   string(jsonChangeSet),
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "spanner.InsertStruct()")
	}

	return mutation, nil
}

func (p *SpannerPatcher) dataChangeEventUpdateMutation(ctx context.Context, txn *spanner.ReadWriteTransaction, eventSource string, event *Event) (*spanner.Mutation, error) {
	jsonChangeSet, err := p.jsonUpdateSet(ctx, txn, event.TableName, event.PrimaryKeys, event.PatchSet, event.RowStruct)
	if err != nil {
		return nil, err
	}

	mutation, err := spanner.InsertStruct(p.changeTrackingTable,
		&DataChangeEvent{
			TableName:   event.TableName,
			RowID:       event.PrimaryKeys.RowID(),
			EventTime:   spanner.CommitTimestamp,
			EventSource: eventSource,
			ChangeSet:   string(jsonChangeSet),
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "spanner.InsertStruct()")
	}

	return mutation, nil
}

func (p *SpannerPatcher) dataChangeEventDeleteMutation(ctx context.Context, txn *spanner.ReadWriteTransaction, eventSource string, event *Event) (*spanner.Mutation, error) {
	jsonChangeSet, err := p.jsonDeleteSet(ctx, txn, event.TableName, event.PrimaryKeys, event.PatchSet, event.RowStruct)
	if err != nil {
		return nil, err
	}

	mutation, err := spanner.InsertStruct(p.changeTrackingTable,
		&DataChangeEvent{
			TableName:   event.TableName,
			RowID:       event.PrimaryKeys.RowID(),
			EventTime:   spanner.CommitTimestamp,
			EventSource: eventSource,
			ChangeSet:   string(jsonChangeSet),
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "spanner.InsertStruct()")
	}

	return mutation, nil
}

func (p *SpannerPatcher) jsonInsertSet(patchSet *patchset.PatchSet, row RowStruct) ([]byte, error) {
	changeSet, err := p.Diff(row.New(), patchSet)
	if err != nil {
		return nil, errors.Wrap(err, "Diff()")
	}

	if len(changeSet) == 0 {
		return nil, httpio.NewBadRequestMessage("No data to insert")
	}

	jsonBytes, err := json.Marshal(changeSet)
	if err != nil {
		return nil, errors.Wrap(err, "json.Marshal()")
	}

	return jsonBytes, nil
}

func (p *SpannerPatcher) jsonUpdateSet(ctx context.Context, txn *spanner.ReadWriteTransaction, tableName accesstypes.Resource, pkeys PrimaryKey, patchSet *patchset.PatchSet, row RowStruct) ([]byte, error) {
	patchSetColumns, err := p.PatchSetColumns(patchSet, row.Type())
	if err != nil {
		return nil, errors.Wrap(err, "SpannerPatcher.Columns()")
	}

	where := strings.Builder{}
	for _, keyPart := range pkeys.keyParts {
		where.WriteString(fmt.Sprintf(" AND %s = @%s", keyPart.key, strings.ToLower(string(keyPart.key))))
	}

	stmt := spanner.NewStatement(fmt.Sprintf(`
			SELECT
				%s
			FROM %s 
			WHERE %s`, patchSetColumns, tableName, where.String()[5:],
	))
	for _, keyPart := range pkeys.keyParts {
		stmt.Params[strings.ToLower(string(keyPart.key))] = keyPart.value
	}

	oldValues := row.New()
	if err := spxscan.Get(ctx, txn, oldValues, stmt); err != nil {
		if errors.Is(err, spxscan.ErrNotFound) {
			return nil, httpio.NewNotFoundMessagef("%s %q not found", tableName, pkeys.RowID())
		}

		return nil, errors.Wrap(err, "spxscan.Get()")
	}

	changeSet, err := p.Diff(oldValues, patchSet)
	if err != nil {
		return nil, errors.Wrap(err, "Diff()")
	}

	if len(changeSet) == 0 {
		return nil, httpio.NewBadRequestMessage("No changes to apply")
	}

	jsonBytes, err := json.Marshal(changeSet)
	if err != nil {
		return nil, errors.Wrap(err, "json.Marshal()")
	}

	return jsonBytes, nil
}

func (p *SpannerPatcher) jsonDeleteSet(ctx context.Context, txn *spanner.ReadWriteTransaction, tableName accesstypes.Resource, pkeys PrimaryKey, patchSet *patchset.PatchSet, row RowStruct) ([]byte, error) {
	patchSetColumns, err := p.PatchSetColumns(patchSet, row.Type())
	if err != nil {
		return nil, errors.Wrap(err, "SpannerPatcher.Columns()")
	}

	where := strings.Builder{}
	for _, keyPart := range pkeys.keyParts {
		where.WriteString(fmt.Sprintf(" AND %s = @%s", keyPart.key, strings.ToLower(string(keyPart.key))))
	}

	stmt := spanner.NewStatement(fmt.Sprintf(`
			SELECT
				%s
			FROM %s 
			WHERE %s`, patchSetColumns, tableName, where.String()[5:],
	))
	for _, keyPart := range pkeys.keyParts {
		stmt.Params[strings.ToLower(string(keyPart.key))] = keyPart.value
	}

	oldValues := row.New()
	if err := spxscan.Get(ctx, txn, oldValues, stmt); err != nil {
		if errors.Is(err, spxscan.ErrNotFound) {
			return nil, httpio.NewNotFoundMessagef("%s %q not found", tableName, pkeys.RowID())
		}

		return nil, errors.Wrap(err, "spxscan.Get()")
	}

	changeSet, err := p.deleteChangeSet(oldValues)
	if err != nil {
		return nil, errors.Wrap(err, "Diff()")
	}

	if len(changeSet) == 0 {
		return nil, httpio.NewBadRequestMessage("No changes to apply")
	}

	jsonBytes, err := json.Marshal(changeSet)
	if err != nil {
		return nil, errors.Wrap(err, "json.Marshal()")
	}

	return jsonBytes, nil
}
