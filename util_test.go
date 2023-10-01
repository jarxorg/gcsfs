package gcsfs

import (
	"io/fs"
	"reflect"
	"testing"

	"cloud.google.com/go/storage"
)

func TestIsNotExist(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{
			err:  fs.ErrNotExist,
			want: true,
		}, {
			err:  &fs.PathError{Err: fs.ErrNotExist},
			want: true,
		}, {
			err:  fs.ErrExist,
			want: false,
		},
	}
	for _, test := range tests {
		got := isNotExist(test.err)
		if got != test.want {
			t.Errorf(`Error isNotExist(%v) returns %v; want %v`, test.err, got, test.want)
		}
	}
}

func TestIsObjectNotExist(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{
			err:  storage.ErrObjectNotExist,
			want: true,
		}, {
			err:  fs.ErrNotExist,
			want: false,
		},
	}
	for _, test := range tests {
		got := isObjectNotExist(test.err)
		if got != test.want {
			t.Errorf(`Error isObjectNotExist(%v) returns %v; want %v`, test.err, got, test.want)
		}
	}
}

func TestToPathError(t *testing.T) {
	op := "open"
	name := "test.txt"

	tests := []struct {
		err  error
		want error
	}{
		{
			err:  storage.ErrObjectNotExist,
			want: &fs.PathError{Op: op, Path: name, Err: fs.ErrNotExist},
		}, {
			err:  fs.ErrNotExist,
			want: &fs.PathError{Op: op, Path: name, Err: fs.ErrNotExist},
		}, {
			err:  fs.ErrExist,
			want: &fs.PathError{Op: op, Path: name, Err: fs.ErrExist},
		}, {
			err:  nil,
			want: nil,
		},
	}
	for _, test := range tests {
		got := toPathError(test.err, op, name)
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf(`Error toPathError(%v) returns %v; want %v`, test.err, got, test.want)
		}
	}
}

func TestToObjectNotExistIfNoExist(t *testing.T) {
	tests := []struct {
		err  error
		want error
	}{
		{
			err:  fs.ErrNotExist,
			want: storage.ErrObjectNotExist,
		}, {
			err:  &fs.PathError{Err: fs.ErrNotExist},
			want: storage.ErrObjectNotExist,
		}, {
			err:  fs.ErrExist,
			want: fs.ErrExist,
		}, {
			err:  nil,
			want: nil,
		},
	}
	for _, test := range tests {
		got := toObjectNotExistIfNoExist(test.err)
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf(`Error toS3NoSuckKeyIfNoExist(%v) returns %v; want %v`, test.err, got, test.want)
		}
	}
}

func TestNormalizePrefix(t *testing.T) {
	tests := []struct {
		prefix string
		want   string
	}{
		{
			prefix: ".",
			want:   "",
		}, {
			prefix: "/.",
			want:   "",
		}, {
			prefix: "dir",
			want:   "dir/",
		}, {
			prefix: "dir/",
			want:   "dir/",
		}, {
			prefix: "dir/.",
			want:   "dir/",
		},
	}
	for _, test := range tests {
		got := normalizePrefix(test.prefix)
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf(`Error normalizePrefix(%s) returns %s; want %s`, test.prefix, got, test.want)
		}
	}
}

func TestNormalizePrefixPattemr(t *testing.T) {
	tests := []struct {
		prefix  string
		pattern string
		want    string
	}{
		{
			prefix: ".",
			want:   "",
		}, {
			prefix: "/.",
			want:   "",
		}, {
			prefix:  "dir",
			pattern: "",
			want:    "dir/",
		}, {
			prefix:  "dir",
			pattern: "*.txt",
			want:    "dir/",
		}, {
			prefix:  "",
			pattern: "d*",
			want:    "d",
		}, {
			prefix:  "",
			pattern: "d?",
			want:    "d",
		}, {
			prefix:  "",
			pattern: "d[a-z]r",
			want:    "d",
		}, {
			prefix:  "",
			pattern: "d\\",
			want:    "d",
		}, {
			prefix:  "dir",
			pattern: "file*.txt",
			want:    "dir/file",
		}, {
			prefix:  "dir",
			pattern: "sub-dir/",
			want:    "dir/sub-dir/",
		},
	}
	for _, test := range tests {
		got := normalizePrefixPattern(test.prefix, test.pattern)
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf(`Error normalizePrefixPattern(%s, %s) returns %s; want %s`, test.prefix, test.pattern, got, test.want)
		}
	}
}

func TestNewQuery(t *testing.T) {
	want := &storage.Query{
		Delimiter:                "/",
		Prefix:                   "prefix",
		StartOffset:              "offset",
		IncludeTrailingDelimiter: true,
	}
	want.SetAttrSelection([]string{"Prefix", "Name", "Size", "Updated"})

	got := newQuery(want.Delimiter, want.Prefix, want.StartOffset)
	if !reflect.DeepEqual(got, want) {
		t.Errorf(`Error newQuery returns %v; want %v`, want, got)
	}
}

func TestContains(t *testing.T) {
	tests := []struct {
		keys []string
		key  string
		want bool
	}{
		{
			keys: []string{"abc", "def", "ghi"},
			key:  "def",
			want: true,
		}, {
			keys: []string{"abc", "def", "ghi"},
			key:  "xyz",
			want: false,
		},
	}
	for _, test := range tests {
		got := contains(test.keys, test.key)
		if got != test.want {
			t.Errorf(`Error contains(%v, %v) returns %v; want %v`, test.keys, test.key, got, test.want)
		}
	}
}
