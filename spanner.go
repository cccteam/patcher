package patcher

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"cloud.google.com/go/spanner"
	"github.com/cccteam/ccc/accesstypes"
	"github.com/cccteam/ccc/resource"
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

func (p *SpannerPatcher) Insert(ctx context.Context, s *spanner.Client, patchSet *resource.PatchSet) error {
	if _, err := s.ReadWriteTransaction(ctx, func(_ context.Context, txn *spanner.ReadWriteTransaction) error {
		if err := p.BufferInsert(txn, patchSet); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return nil
}

func (p *SpannerPatcher) Update(ctx context.Context, s *spanner.Client, patchSet *resource.PatchSet) error {
	if _, err := s.ReadWriteTransaction(ctx, func(_ context.Context, txn *spanner.ReadWriteTransaction) error {
		if err := p.BufferUpdate(txn, patchSet); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return nil
}

func (p *SpannerPatcher) InsertOrUpdate(ctx context.Context, s *spanner.Client, patchSet *resource.PatchSet) error {
	if _, err := s.ReadWriteTransaction(ctx, func(_ context.Context, txn *spanner.ReadWriteTransaction) error {
		if err := p.BufferInsertOrUpdate(txn, patchSet); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return nil
}

func (p *SpannerPatcher) Delete(ctx context.Context, s *spanner.Client, patchSet *resource.PatchSet) error {
	if _, err := s.ReadWriteTransaction(ctx, func(_ context.Context, txn *spanner.ReadWriteTransaction) error {
		if err := p.BufferDelete(txn, patchSet); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return nil
}

func (p *SpannerPatcher) InsertWithDataChangeEvent(ctx context.Context, s *spanner.Client, eventSource string, patchSet *resource.PatchSet) error {
	if _, err := s.ReadWriteTransaction(ctx, func(_ context.Context, txn *spanner.ReadWriteTransaction) error {
		if err := p.BufferInsertWithDataChangeEvent(txn, eventSource, patchSet); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return nil
}

func (p *SpannerPatcher) InsertOrUpdateWithDataChangeEvent(ctx context.Context, s *spanner.Client, eventSource string, patchSet *resource.PatchSet) error {
	if _, err := s.ReadWriteTransaction(ctx, func(_ context.Context, txn *spanner.ReadWriteTransaction) error {
		if err := p.BufferInsertOrUpdateWithDataChangeEvent(txn, eventSource, patchSet); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return nil
}

func (p *SpannerPatcher) UpdateWithDataChangeEvent(ctx context.Context, s *spanner.Client, eventSource string, patchSet *resource.PatchSet) error {
	if _, err := s.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		if err := p.BufferUpdateWithDataChangeEvent(ctx, txn, eventSource, patchSet); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return nil
}

func (p *SpannerPatcher) DeleteWithDataChangeEvent(ctx context.Context, s *spanner.Client, eventSource string, patchSet *resource.PatchSet) error {
	if _, err := s.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		if err := p.BufferDeleteWithDataChangeEvent(ctx, txn, eventSource, patchSet); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "spanner.Client.ReadWriteTransaction()")
	}

	return nil
}

func (p *SpannerPatcher) BufferInsert(txn *spanner.ReadWriteTransaction, patchSet *resource.PatchSet) error {
	patch, err := p.Resolve(patchSet)
	if err != nil {
		return errors.Wrap(err, "Resolve()")
	}
	m := spanner.InsertMap(string(patchSet.Resource()), patch)

	if err := txn.BufferWrite([]*spanner.Mutation{m}); err != nil {
		return errors.Wrap(err, "spanner.ReadWriteTransaction.BufferWrite()")
	}

	return nil
}

func (p *SpannerPatcher) BufferUpdate(txn *spanner.ReadWriteTransaction, patchSet *resource.PatchSet) error {
	patch, err := p.Resolve(patchSet)
	if err != nil {
		return errors.Wrap(err, "Resolve()")
	}
	m := spanner.UpdateMap(string(patchSet.Resource()), patch)

	if err := txn.BufferWrite([]*spanner.Mutation{m}); err != nil {
		return errors.Wrap(err, "spanner.ReadWriteTransaction.BufferWrite()")
	}

	return nil
}

func (p *SpannerPatcher) BufferInsertOrUpdate(txn *spanner.ReadWriteTransaction, patchSet *resource.PatchSet) error {
	patch, err := p.Resolve(patchSet)
	if err != nil {
		return errors.Wrap(err, "Resolve()")
	}
	m := spanner.InsertOrUpdateMap(string(patchSet.Resource()), patch)

	if err := txn.BufferWrite([]*spanner.Mutation{m}); err != nil {
		return errors.Wrap(err, "spanner.ReadWriteTransaction.BufferWrite()")
	}

	return nil
}

func (p *SpannerPatcher) BufferDelete(txn *spanner.ReadWriteTransaction, patchSet *resource.PatchSet) error {
	m := spanner.Delete(string(patchSet.Resource()), patchSet.PrimaryKey().KeySet())

	if err := txn.BufferWrite([]*spanner.Mutation{m}); err != nil {
		return errors.Wrap(err, "spanner.ReadWriteTransaction.BufferWrite()")
	}

	return nil
}

func (p *SpannerPatcher) BufferInsertWithDataChangeEvent(txn *spanner.ReadWriteTransaction, eventSource string, patchSet *resource.PatchSet) error {
	if err := p.BufferInsert(txn, patchSet); err != nil {
		return err
	}

	if err := p.bufferInsertWithDataChangeEvent(txn, eventSource, patchSet); err != nil {
		return err
	}

	return nil
}

func (p *SpannerPatcher) BufferInsertOrUpdateWithDataChangeEvent(txn *spanner.ReadWriteTransaction, eventSource string, patchSet *resource.PatchSet) error {
	if err := p.BufferInsertOrUpdate(txn, patchSet); err != nil {
		return err
	}

	if err := p.bufferInsertWithDataChangeEvent(txn, eventSource, patchSet); err != nil {
		return err
	}

	return nil
}

func (p *SpannerPatcher) BufferUpdateWithDataChangeEvent(ctx context.Context, txn *spanner.ReadWriteTransaction, eventSource string, patchSet *resource.PatchSet) error {
	if err := p.BufferUpdate(txn, patchSet); err != nil {
		return err
	}

	if err := p.bufferUpdateWithDataChangeEvent(ctx, txn, eventSource, patchSet); err != nil {
		return err
	}

	return nil
}

func (p *SpannerPatcher) BufferDeleteWithDataChangeEvent(ctx context.Context, txn *spanner.ReadWriteTransaction, eventSource string, patchSet *resource.PatchSet) error {
	if err := p.BufferDelete(txn, patchSet); err != nil {
		return err
	}

	if err := p.bufferDeleteWithDataChangeEvent(ctx, txn, eventSource, patchSet); err != nil {
		return err
	}

	return nil
}

func (p *SpannerPatcher) bufferInsertWithDataChangeEvent(txn *spanner.ReadWriteTransaction, eventSource string, patchSet *resource.PatchSet) error {
	jsonChangeSet, err := p.jsonInsertSet(patchSet)
	if err != nil {
		return err
	}

	m, err := spanner.InsertStruct(p.changeTrackingTable,
		&DataChangeEvent{
			TableName:   patchSet.Resource(),
			RowID:       patchSet.PrimaryKey().RowID(),
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

func (p *SpannerPatcher) bufferUpdateWithDataChangeEvent(ctx context.Context, txn *spanner.ReadWriteTransaction, eventSource string, patchSet *resource.PatchSet) error {
	jsonChangeSet, err := p.jsonUpdateSet(ctx, txn, patchSet.Resource(), patchSet)
	if err != nil {
		return err
	}

	m, err := spanner.InsertStruct(p.changeTrackingTable,
		&DataChangeEvent{
			TableName:   patchSet.Resource(),
			RowID:       patchSet.PrimaryKey().RowID(),
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

func (p *SpannerPatcher) bufferDeleteWithDataChangeEvent(ctx context.Context, txn *spanner.ReadWriteTransaction, eventSource string, patchSet *resource.PatchSet) error {
	keySet := patchSet.PrimaryKey()
	jsonChangeSet, err := p.jsonDeleteSet(ctx, txn, patchSet)
	if err != nil {
		return err
	}

	m, err := spanner.InsertStruct(p.changeTrackingTable,
		&DataChangeEvent{
			TableName:   patchSet.Resource(),
			RowID:       keySet.RowID(),
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

func (p *SpannerPatcher) jsonInsertSet(patchSet *resource.PatchSet) ([]byte, error) {
	changeSet, err := p.Diff(patchSet.Row(), patchSet)
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
	ctx context.Context, txn *spanner.ReadWriteTransaction, tableName accesstypes.Resource, patchSet *resource.PatchSet) ([]byte, error,
) {
	patchSetColumns, err := p.PatchColumns(patchSet)
	if err != nil {
		return nil, errors.Wrap(err, "SpannerPatcher.Columns()")
	}

	where, params, err := p.Where(patchSet.PrimaryKey(), patchSet.Row())
	if err != nil {
		return nil, errors.Wrap(err, "patcher.Where()")
	}

	stmt := spanner.NewStatement(fmt.Sprintf(`
			SELECT
				%s
			FROM %s 
			%s`, patchSetColumns, tableName, where,
	))
	for param, value := range params {
		stmt.Params[param] = value
	}

	oldValues := patchSet.Row()
	if err := spxscan.Get(ctx, txn, oldValues, stmt); err != nil {
		if errors.Is(err, spxscan.ErrNotFound) {
			return nil, httpio.NewNotFoundMessagef("%s (%s) not found", tableName, patchSet.PrimaryKey().String())
		}

		return nil, errors.Wrap(err, "spxscan.Get()")
	}

	changeSet, err := p.Diff(oldValues, patchSet)
	if err != nil {
		return nil, errors.Wrap(err, "Diff()")
	}

	if len(changeSet) == 0 {
		return nil, httpio.NewBadRequestMessagef("No changes to apply on %s (%s)", tableName, patchSet.PrimaryKey().String())
	}

	jsonBytes, err := json.Marshal(changeSet)
	if err != nil {
		return nil, errors.Wrap(err, "json.Marshal()")
	}

	return jsonBytes, nil
}

func (p *SpannerPatcher) jsonDeleteSet(
	ctx context.Context, txn *spanner.ReadWriteTransaction, patchSet *resource.PatchSet,
) ([]byte, error) {
	columns, err := p.AllColumns(patchSet.Row())
	if err != nil {
		return nil, errors.Wrap(err, "SpannerPatcher.Columns()")
	}

	where, params, err := p.Where(patchSet.PrimaryKey(), patchSet.Row())
	if err != nil {
		return nil, errors.Wrap(err, "patcher.Where()")
	}

	stmt := spanner.NewStatement(fmt.Sprintf(`
			SELECT
				%s
			FROM %s 
			%s`, columns, patchSet.Resource(), where,
	))
	for param, value := range params {
		stmt.Params[param] = value
	}

	oldValues := patchSet.Row()
	if err := spxscan.Get(ctx, txn, oldValues, stmt); err != nil {
		if errors.Is(err, spxscan.ErrNotFound) {
			return nil, httpio.NewNotFoundMessagef("%s (%s) not found", patchSet.Resource(), patchSet.PrimaryKey().RowID())
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
