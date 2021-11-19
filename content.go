package gcsfs

import (
	"io/fs"
	"path"
	"strings"
	"time"

	"cloud.google.com/go/storage"
)

type content struct {
	name    string
	isDir   bool
	size    int64
	modTime time.Time
}

var (
	_ fs.DirEntry = (*content)(nil)
	_ fs.FileInfo = (*content)(nil)
)

func newContent(attrs *storage.ObjectAttrs) *content {
	if attrs.Name == "" {
		return newDirContent(attrs.Prefix)
	}
	return newFileContent(attrs)
}

func newDirContent(prefix string) *content {
	return &content{
		name:  path.Base(strings.TrimSuffix(prefix, "/")),
		isDir: true,
	}
}

func newFileContent(attrs *storage.ObjectAttrs) *content {
	return &content{
		name:    path.Base(attrs.Name),
		size:    attrs.Size,
		modTime: attrs.Updated,
	}
}

func (c *content) Name() string {
	return c.name
}

func (c *content) Size() int64 {
	return c.size
}

// Mode returns if this content is directory then fs.ModePerm | fs.ModeDir otherwise fs.ModePerm.
func (c *content) Mode() fs.FileMode {
	if c.isDir {
		return fs.ModePerm | fs.ModeDir
	}
	return fs.ModePerm
}

func (c *content) ModTime() time.Time {
	return c.modTime
}

func (c *content) IsDir() bool {
	return c.isDir
}

func (c *content) Sys() interface{} {
	return nil
}

func (c *content) Type() fs.FileMode {
	return c.Mode() & fs.ModeType
}

func (c *content) Info() (fs.FileInfo, error) {
	return c, nil
}
