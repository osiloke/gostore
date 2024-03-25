package badger

import (
	"reflect"
	"testing"
)

func Test_getPath(t *testing.T) {
	type args struct {
		key string
		s   interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantV   interface{}
		wantErr bool
	}{
		{
			"test nested",
			args{
				key: "home",
				s: map[string]interface{}{
					"id":    "key",
					"name":  "osiloke emoekpere",
					"type":  "person",
					"mode":  "shirt",
					"count": "12",
					"home": map[string]interface{}{
						"location": map[string]interface{}{
							"accuracy": "APPROXIMATE",
							"lat":      37.5483,
							"lon":      -121.989,
						}},
				},
			},
			map[string]interface{}{
				"location": map[string]interface{}{
					"accuracy": "APPROXIMATE",
					"lat":      37.5483,
					"lon":      -121.989,
				}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotV, err := getPath(tt.args.key, tt.args.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("getPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotV, tt.wantV) {
				t.Errorf("getPath() = %v, want %v", gotV, tt.wantV)
			}
		})
	}
}

func Test_valForPath(t *testing.T) {
	type args struct {
		key string
		s   interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantV   interface{}
		wantErr bool
	}{
		{
			"test nested",
			args{
				key: "home.location",
				s: map[string]interface{}{
					"id":    "key",
					"name":  "osiloke emoekpere",
					"type":  "person",
					"mode":  "shirt",
					"count": "12",
					"home": map[string]interface{}{
						"location": map[string]interface{}{
							"accuracy": "APPROXIMATE",
							"lat":      37.5483,
							"lon":      -121.989,
						}},
				},
			},
			map[string]interface{}{
				"accuracy": "APPROXIMATE",
				"lat":      37.5483,
				"lon":      -121.989,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotV, err := valForPath(tt.args.key, tt.args.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("valForPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotV, tt.wantV) {
				t.Errorf("valForPath() = %v, want %v", gotV, tt.wantV)
			}
		})
	}
}
