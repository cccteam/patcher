package patcher

import (
	"reflect"
	"testing"

	"github.com/cccteam/ccc/accesstypes"
)

func TestNewPrimaryKeyFromMap(t *testing.T) {
	t.Parallel()

	type args struct {
		keyMap map[accesstypes.Field]any
	}
	tests := []struct {
		name string
		args args
		want PrimaryKey
	}{
		{
			name: "NewPrimaryKeyFromMap",
			args: args{
				keyMap: map[accesstypes.Field]any{
					"field1": "1",
					"field2": "2",
				},
			},
			want: PrimaryKey{
				keyParts: []keyPart{
					{
						key:   "field1",
						value: "1",
					},
					{
						key:   "field2",
						value: "2",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := NewPrimaryKeyFromMap(tt.args.keyMap); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewPrimaryKeyFromMap() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewPrimaryKey(t *testing.T) {
	t.Parallel()

	type args struct {
		key   accesstypes.Field
		value any
	}
	tests := []struct {
		name string
		args args
		want PrimaryKey
	}{
		{
			name: "NewPrimaryKey",
			args: args{
				key:   "field1",
				value: "1",
			},
			want: PrimaryKey{
				keyParts: []keyPart{
					{
						key:   "field1",
						value: "1",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := NewPrimaryKey(tt.args.key, tt.args.value); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewPrimaryKey() = %v, want %v", got, tt.want)
			}
		})
	}
}
