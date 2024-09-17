package patcher

import "reflect"

type PostgresPatcher struct {
	changeTrackingTable string
	*patcher
}

func NewPostgresPatcher() *PostgresPatcher {
	return &PostgresPatcher{
		changeTrackingTable: "DataChangeEvents",
		patcher: &patcher{
			cache:   make(map[reflect.Type]map[string]cacheEntry),
			tagName: "db",
			dbType:  postgresdbType,
		},
	}
}

func (p *PostgresPatcher) WithDataChangeTableName(tableName string) *PostgresPatcher {
	p.changeTrackingTable = tableName

	return p
}
