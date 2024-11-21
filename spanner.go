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

func (p *SpannerPatcher) Insert(ctx context.Context, s *spanner.Client, mutation *Mutation) error {
	if _, err := s.ReadWriteTransaction(ctx, func(_ context.Context, txn *spanner.ReadWriteTransaction) error {
		if err := p.BufferInsert(txn, mutation); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return nil
}

func (p *SpannerPatcher) Update(ctx context.Context, s *spanner.Client, mutation *Mutation) error {
	if _, err := s.ReadWriteTransaction(ctx, func(_ context.Context, txn *spanner.ReadWriteTransaction) error {
		if err := p.BufferUpdate(txn, mutation); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return nil
}

func (p *SpannerPatcher) InsertOrUpdate(ctx context.Context, s *spanner.Client, mutation *Mutation) error {
	if _, err := s.ReadWriteTransaction(ctx, func(_ context.Context, txn *spanner.ReadWriteTransaction) error {
		if err := p.BufferInsertOrUpdate(txn, mutation); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return nil
}

func (p *SpannerPatcher) Delete(ctx context.Context, s *spanner.Client, mutation *Mutation) error {
	if _, err := s.ReadWriteTransaction(ctx, func(_ context.Context, txn *spanner.ReadWriteTransaction) error {
		if err := p.BufferDelete(txn, mutation); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return nil
}

func (p *SpannerPatcher) InsertWithDataChangeEvent(ctx context.Context, s *spanner.Client, eventSource string, mutation *Mutation) error {
	if _, err := s.ReadWriteTransaction(ctx, func(_ context.Context, txn *spanner.ReadWriteTransaction) error {
		if err := p.BufferInsertWithDataChangeEvent(txn, eventSource, mutation); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return nil
}

func (p *SpannerPatcher) InsertOrUpdateWithDataChangeEvent(ctx context.Context, s *spanner.Client, eventSource string, mutation *Mutation) error {
	if _, err := s.ReadWriteTransaction(ctx, func(_ context.Context, txn *spanner.ReadWriteTransaction) error {
		if err := p.BufferInsertOrUpdateWithDataChangeEvent(txn, eventSource, mutation); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return nil
}

func (p *SpannerPatcher) UpdateWithDataChangeEvent(ctx context.Context, s *spanner.Client, eventSource string, mutation *Mutation) error {
	if _, err := s.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		if err := p.BufferUpdateWithDataChangeEvent(ctx, txn, eventSource, mutation); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return nil
}

func (p *SpannerPatcher) DeleteWithDataChangeEvent(ctx context.Context, s *spanner.Client, eventSource string, mutation *Mutation) error {
	if _, err := s.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		if err := p.BufferDeleteWithDataChangeEvent(ctx, txn, eventSource, mutation); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return nil
}

func (p *SpannerPatcher) BufferInsert(txn *spanner.ReadWriteTransaction, mutation *Mutation) error {
	patch, err := p.Resolve(mutation.PatchSet, mutation.RowStruct.Type())
	if err != nil {
		return errors.Wrap(err, "Resolve()")
	}
	m := spanner.InsertMap(string(mutation.TableName), patch)

	if err := txn.BufferWrite([]*spanner.Mutation{m}); err != nil {
		return errors.Wrap(err, "spanner.ReadWriteTransaction.BufferWrite()")
	}

	return nil
}

func (p *SpannerPatcher) BufferUpdate(txn *spanner.ReadWriteTransaction, mutation *Mutation) error {
	patch, err := p.Resolve(mutation.PatchSet, mutation.RowStruct.Type())
	if err != nil {
		return errors.Wrap(err, "Resolve()")
	}
	m := spanner.UpdateMap(string(mutation.TableName), patch)

	if err := txn.BufferWrite([]*spanner.Mutation{m}); err != nil {
		return errors.Wrap(err, "spanner.ReadWriteTransaction.BufferWrite()")
	}

	return nil
}

func (p *SpannerPatcher) BufferInsertOrUpdate(txn *spanner.ReadWriteTransaction, mutation *Mutation) error {
	patch, err := p.Resolve(mutation.PatchSet, mutation.RowStruct.Type())
	if err != nil {
		return errors.Wrap(err, "Resolve()")
	}
	m := spanner.InsertOrUpdateMap(string(mutation.TableName), patch)

	if err := txn.BufferWrite([]*spanner.Mutation{m}); err != nil {
		return errors.Wrap(err, "spanner.ReadWriteTransaction.BufferWrite()")
	}

	return nil
}

func (p *SpannerPatcher) BufferDelete(txn *spanner.ReadWriteTransaction, mutation *Mutation) error {
	m := spanner.Delete(string(mutation.TableName), mutation.PatchSet.PrimaryKey().KeySet())

	if err := txn.BufferWrite([]*spanner.Mutation{m}); err != nil {
		return errors.Wrap(err, "spanner.ReadWriteTransaction.BufferWrite()")
	}

	return nil
}

func (p *SpannerPatcher) BufferInsertWithDataChangeEvent(txn *spanner.ReadWriteTransaction, eventSource string, mutation *Mutation) error {
	if err := p.BufferInsert(txn, mutation); err != nil {
		return err
	}

	if err := p.bufferInsertWithDataChangeEvent(txn, eventSource, mutation); err != nil {
		return err
	}

	return nil
}

func (p *SpannerPatcher) BufferInsertOrUpdateWithDataChangeEvent(txn *spanner.ReadWriteTransaction, eventSource string, mutation *Mutation) error {
	if err := p.BufferInsertOrUpdate(txn, mutation); err != nil {
		return err
	}

	if err := p.bufferInsertWithDataChangeEvent(txn, eventSource, mutation); err != nil {
		return err
	}

	return nil
}

func (p *SpannerPatcher) BufferUpdateWithDataChangeEvent(ctx context.Context, txn *spanner.ReadWriteTransaction, eventSource string, mutation *Mutation) error {
	if err := p.BufferUpdate(txn, mutation); err != nil {
		return err
	}

	if err := p.bufferUpdateWithDataChangeEvent(ctx, txn, eventSource, mutation); err != nil {
		return err
	}

	return nil
}

func (p *SpannerPatcher) BufferDeleteWithDataChangeEvent(ctx context.Context, txn *spanner.ReadWriteTransaction, eventSource string, mutation *Mutation) error {
	if err := p.BufferDelete(txn, mutation); err != nil {
		return err
	}

	if err := p.bufferDeleteWithDataChangeEvent(ctx, txn, eventSource, mutation); err != nil {
		return err
	}

	return nil
}

func (p *SpannerPatcher) bufferInsertWithDataChangeEvent(txn *spanner.ReadWriteTransaction, eventSource string, mutation *Mutation) error {
	jsonChangeSet, err := p.jsonInsertSet(mutation.PatchSet, mutation.RowStruct)
	if err != nil {
		return err
	}

	m, err := spanner.InsertStruct(p.changeTrackingTable,
		&DataChangeEvent{
			TableName:   mutation.TableName,
			RowID:       mutation.PatchSet.PrimaryKey().RowID(),
			EventTime:   spanner.CommitTimestamp,
			EventSource: eventSource,
			ChangeSet:   string(jsonChangeSet),
		},
	)
	if err != nil {
		return errors.Wrap(err, "spanner.InsertStruct()")
	}

	if err := txn.BufferWrite([]*spanner.Mutation{m}); err != nil {
		return errors.Wrap(err, "spanner.ReadWriteTransaction.BufferWrite()")
	}

	return nil
}

func (p *SpannerPatcher) bufferUpdateWithDataChangeEvent(ctx context.Context, txn *spanner.ReadWriteTransaction, eventSource string, mutation *Mutation) error {
	pkey := mutation.PatchSet.PrimaryKey()
	jsonChangeSet, err := p.jsonUpdateSet(ctx, txn, mutation.TableName, pkey, mutation.PatchSet, mutation.RowStruct)
	if err != nil {
		return err
	}

	m, err := spanner.InsertStruct(p.changeTrackingTable,
		&DataChangeEvent{
			TableName:   mutation.TableName,
			RowID:       pkey.RowID(),
			EventTime:   spanner.CommitTimestamp,
			EventSource: eventSource,
			ChangeSet:   string(jsonChangeSet),
		},
	)
	if err != nil {
		return errors.Wrap(err, "spanner.InsertStruct()")
	}

	if err := txn.BufferWrite([]*spanner.Mutation{m}); err != nil {
		return errors.Wrap(err, "spanner.ReadWriteTransaction.BufferWrite()")
	}

	return nil
}

func (p *SpannerPatcher) bufferDeleteWithDataChangeEvent(ctx context.Context, txn *spanner.ReadWriteTransaction, eventSource string, mutation *Mutation) error {
	pkey := mutation.PatchSet.PrimaryKey()
	jsonChangeSet, err := p.jsonDeleteSet(ctx, txn, mutation.TableName, pkey, mutation.RowStruct)
	if err != nil {
		return err
	}

	m, err := spanner.InsertStruct(p.changeTrackingTable,
		&DataChangeEvent{
			TableName:   mutation.TableName,
			RowID:       pkey.RowID(),
			EventTime:   spanner.CommitTimestamp,
			EventSource: eventSource,
			ChangeSet:   string(jsonChangeSet),
		},
	)
	if err != nil {
		return errors.Wrap(err, "spanner.InsertStruct()")
	}

	if err := txn.BufferWrite([]*spanner.Mutation{m}); err != nil {
		return errors.Wrap(err, "spanner.ReadWriteTransaction.BufferWrite()")
	}

	return nil
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

func (p *SpannerPatcher) jsonUpdateSet(
	ctx context.Context, txn *spanner.ReadWriteTransaction, tableName accesstypes.Resource, pkeys patchset.PrimaryKey, patchSet *patchset.PatchSet, row RowStruct) ([]byte, error,
) {
	patchSetColumns, err := p.PatchSetColumns(patchSet, row.Type())
	if err != nil {
		return nil, errors.Wrap(err, "SpannerPatcher.Columns()")
	}

	where := strings.Builder{}
	for _, keyPart := range pkeys.Parts() {
		where.WriteString(fmt.Sprintf(" AND %s = @%s", keyPart.Key, strings.ToLower(string(keyPart.Key))))
	}

	stmt := spanner.NewStatement(fmt.Sprintf(`
			SELECT
				%s
			FROM %s 
			WHERE %s`, patchSetColumns, tableName, where.String()[5:],
	))
	for _, keyPart := range pkeys.Parts() {
		stmt.Params[strings.ToLower(string(keyPart.Key))] = keyPart.Value
	}

	oldValues := row.New()
	if err := spxscan.Get(ctx, txn, oldValues, stmt); err != nil {
		if errors.Is(err, spxscan.ErrNotFound) {
			return nil, httpio.NewNotFoundMessagef("%s (%s) not found", tableName, pkeys.String())
		}

		return nil, errors.Wrap(err, "spxscan.Get()")
	}

	changeSet, err := p.Diff(oldValues, patchSet)
	if err != nil {
		return nil, errors.Wrap(err, "Diff()")
	}

	if len(changeSet) == 0 {
		return nil, httpio.NewBadRequestMessagef("No changes to apply on %s (%s)", tableName, pkeys.String())
	}

	jsonBytes, err := json.Marshal(changeSet)
	if err != nil {
		return nil, errors.Wrap(err, "json.Marshal()")
	}

	return jsonBytes, nil
}

func (p *SpannerPatcher) jsonDeleteSet(
	ctx context.Context, txn *spanner.ReadWriteTransaction, tableName accesstypes.Resource, pkeys patchset.PrimaryKey, row RowStruct,
) ([]byte, error) {
	columns, err := p.AllColumns(row.Type())
	if err != nil {
		return nil, errors.Wrap(err, "SpannerPatcher.Columns()")
	}

	where := strings.Builder{}
	for _, keyPart := range pkeys.Parts() {
		where.WriteString(fmt.Sprintf(" AND %s = @%s", keyPart.Key, strings.ToLower(string(keyPart.Key))))
	}

	stmt := spanner.NewStatement(fmt.Sprintf(`
			SELECT
				%s
			FROM %s 
			WHERE %s`, columns, tableName, where.String()[5:],
	))
	for _, keyPart := range pkeys.Parts() {
		stmt.Params[strings.ToLower(string(keyPart.Key))] = keyPart.Value
	}

	oldValues := row.New()
	if err := spxscan.Get(ctx, txn, oldValues, stmt); err != nil {
		if errors.Is(err, spxscan.ErrNotFound) {
			return nil, httpio.NewNotFoundMessagef("%s (%s) not found", tableName, pkeys.RowID())
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
