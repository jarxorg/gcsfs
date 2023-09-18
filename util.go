package gcsfs

import (
	"errors"
	"io/fs"
	"path"
	"strings"

	"cloud.google.com/go/storage"
)

func isNotExist(err error) bool {
	if errors.Is(err, fs.ErrNotExist) {
		return true
	}
	var pathErr *fs.PathError
	return errors.As(err, &pathErr) && pathErr.Err == fs.ErrNotExist
}

func isObjectNotExist(err error) bool {
	return errors.Is(err, storage.ErrObjectNotExist)
}

func toPathError(err error, op, name string) error {
	if err == nil {
		return nil
	}
	if isObjectNotExist(err) {
		err = fs.ErrNotExist
	}
	return &fs.PathError{Op: op, Path: name, Err: err}
}

func toObjectNotExistIfNoExist(err error) error {
	if err == nil {
		return nil
	}
	if isNotExist(err) {
		return storage.ErrObjectNotExist
	}
	return err
}

func normalizePrefix(prefix string) string {
	prefix = path.Clean(prefix)
	if prefix == "." || prefix == "/" {
		return ""
	}
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}
	return prefix
}

func normalizePrefixPattern(prefix, pattern string) string {
	prefix = normalizePrefix(prefix)
LOOP:
	for i, c := range pattern {
		switch c {
		case '*', '?', '[', '\\':
			pattern = pattern[:i]
			break LOOP
		}
	}
	joined := path.Join(prefix, pattern)
	if strings.HasSuffix(pattern, "/") {
		return joined + "/"
	}
	return joined
}

func newQuery(delim, prefix, offset string) *storage.Query {
	query := &storage.Query{
		Delimiter:   delim,
		Prefix:      prefix,
		StartOffset: offset,
	}
	query.SetAttrSelection([]string{"Prefix", "Name", "Size", "Updated"})
	return query
}
