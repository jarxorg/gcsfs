package gcsfs

import (
	"io/fs"
	"path"

	"cloud.google.com/go/storage"
	"github.com/jarxorg/wfs"
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
			return 0, toPathError(err, "Read", f.attrs.Name)
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

type gcsWriterFile struct {
	*content
	fsys *GCSFS
	name string
	obj  *storage.ObjectHandle
	out  *storage.Writer
}

var (
	_ wfs.WriterFile = (*gcsWriterFile)(nil)
	_ fs.FileInfo    = (*gcsWriterFile)(nil)
)

func newGcsWriterFile(fsys *GCSFS, obj *storage.ObjectHandle, name string) *gcsWriterFile {
	return &gcsWriterFile{
		content: &content{
			name: path.Base(name),
		},
		fsys: fsys,
		obj:  obj,
		name: name,
	}
}

// Write writes the specified bytes to this file.
func (f *gcsWriterFile) Write(p []byte) (int, error) {
	if f.out == nil {
		f.out = f.obj.NewWriter(f.fsys.Context())
	}
	return f.out.Write(p)
}

// Close closes streams.
func (f *gcsWriterFile) Close() error {
	return nil
}

// Read reads bytes from this file.
func (f *gcsWriterFile) Read(p []byte) (int, error) {
	return 0, nil
}

// Stat returns the fs.FileInfo of this file.
func (f *gcsWriterFile) Stat() (fs.FileInfo, error) {
	return f, nil
}
