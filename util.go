package gcsfs

import (
	"errors"
	"io/fs"
	"path"
	"strings"

	"cloud.google.com/go/storage"
)

func isNotExist(err error) bool {
	if err == fs.ErrNotExist {
		return true
	}
	var pathErr *fs.PathError
	return errors.As(err, &pathErr) && pathErr.Err == fs.ErrNotExist
}

func isObjectNotFound(err error) bool {
	return errors.Is(err, storage.ErrObjectNotExist)
}

func toPathError(err error, op, name string) error {
	if err == nil {
		return nil
	}
	if isObjectNotFound(err) {
		err = fs.ErrNotExist
	}
	return &fs.PathError{Op: op, Path: name, Err: err}
}

func normalizePrefix(prefix string) string {
	prefix = path.Clean(prefix)
	if prefix == "." || prefix == "/" {
		prefix = ""
	}
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}
	return prefix
}
