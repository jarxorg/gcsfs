package gcsfs

import (
	"io/fs"

	"cloud.google.com/go/storage"
)

type gcsFile struct {
	*content
	fsys  *GCSFS
	obj   *storage.ObjectHandle
	attrs *storage.ObjectAttrs
	in    *storage.Reader
}

var (
	_ fs.File     = (*gcsFile)(nil)
	_ fs.FileInfo = (*gcsFile)(nil)
)

func newGcsFile(fsys *GCSFS, obj *storage.ObjectHandle, attrs *storage.ObjectAttrs) *gcsFile {
	return &gcsFile{
		content: newFileContent(attrs),
		fsys:    fsys,
		obj:     obj,
		attrs:   attrs,
	}
}

// Read reads bytes from this file.
func (f *gcsFile) Read(p []byte) (int, error) {
	if f.in == nil {
		var err error
		f.in, err = f.obj.NewReader(f.fsys.Context())
		if err != nil {
			return 0, err
		}
	}
	return f.in.Read(p)
}

// Stat returns the fs.FileInfo of this file.
func (f *gcsFile) Stat() (fs.FileInfo, error) {
	return f, nil
}

// Close closes streams.
func (f *gcsFile) Close() error {
	var err error
	if f.in != nil {
		err = f.in.Close()
		f.in = nil
	}
	return err
}
