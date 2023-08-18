package mysql

import "testing"

func Test_decodeHex(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "no hex",
			args: args{
				s: "no hex",
			},
			want:    "no hex",
			wantErr: false,
		},
		{
			name: "with comma",
			args: args{
				s: "hello 0x2c world",
			},
			want:    "hello , world",
			wantErr: false,
		},
		{
			name: "invalid hex",
			args: args{
				s: "hello 0x2g world",
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "3 chars",
			args: args{
				s: "hello 0x2c3 world",
			},
			want:    "hello ,3 world",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeHex(tt.args.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("decodeHex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("decodeHex() = %v, want %v", got, tt.want)
			}
		})
	}
}
