package gcsfs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"testing/fstest"

	"cloud.google.com/go/storage"
	"github.com/jarxorg/io2"
	"github.com/jarxorg/wfs"
	"github.com/jarxorg/wfs/memfs"
	"github.com/jarxorg/wfs/osfs"
	"github.com/jarxorg/wfs/wfstest"
	"google.golang.org/api/iterator"
)

type fsClient struct {
	fsys fs.FS
}

var _ gcsClient = (*fsClient)(nil)

func (c *fsClient) bucket(name string) gcsBucket {
	return &fsBucket{fsys: c.fsys, dir: name}
}

func (c *fsClient) close() error {
	return nil
}

type fsBucket struct {
	fsys fs.FS
	dir  string
}

func (b *fsBucket) object(name string) gcsObject {
	return &fsObject{fsys: b.fsys, dir: b.dir, name: name}
}

func (b *fsBucket) objects(ctx context.Context, q *storage.Query) gcsObjectItetator {
	return &fsObjects{fsys: b.fsys, dir: b.dir, query: q}
}

type fsObject struct {
	fsys fs.FS
	dir  string
	name string
}

func (o *fsObject) newReader(ctx context.Context) (io.ReadCloser, error) {
	in, err := o.fsys.Open(path.Join(o.dir, o.name))
	if err != nil {
		return nil, toObjectNotExistIfNoExist(err)
	}
	return in, nil
}

func (o *fsObject) newWriter(ctx context.Context) io.WriteCloser {
	f, createErr := wfs.CreateFile(o.fsys, path.Join(o.dir, o.name), fs.ModePerm)

	return &io2.Delegator{
		WriteFunc: func(p []byte) (int, error) {
			if createErr != nil {
				return 0, createErr
			}
			return f.Write(p)
		},
		CloseFunc: func() error {
			if f == nil {
				return nil
			}
			return f.Close()
		},
	}
}

func (o *fsObject) attrs(ctx context.Context) (*storage.ObjectAttrs, error) {
	info, err := fs.Stat(o.fsys, path.Join(o.dir, o.name))
	if err != nil {
		return nil, toObjectNotExistIfNoExist(err)
	}
	if info.IsDir() {
		return nil, storage.ErrObjectNotExist
	}
	return &storage.ObjectAttrs{
		Bucket:  o.dir,
		Name:    o.name,
		Size:    info.Size(),
		Updated: info.ModTime(),
	}, nil
}

func (o *fsObject) delete(ctx context.Context) error {
	return wfs.RemoveFile(o.fsys, path.Join(o.dir, o.name))
}

type fsObjects struct {
	fsys      fs.FS
	dir       string
	query     *storage.Query
	attrsList []*storage.ObjectAttrs
	off       int
}

func (o *fsObjects) initAttrs() error {
	if o.attrsList != nil {
		return nil
	}
	if o.query.Delimiter == "/" {
		return o.readDir()
	}
	return o.walkDir()
}

func (o *fsObjects) namePrefixes() (string, string, error) {
	namePrefix := ""
	prefix := o.query.Prefix
	dirWithPrefix := path.Join(o.dir, prefix)
	info, err := fs.Stat(o.fsys, dirWithPrefix)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return "", "", err
		}
		if dirSlash := strings.LastIndex(prefix, "/"); dirSlash != -1 {
			namePrefix = prefix[dirSlash+1:]
			prefix = prefix[:dirSlash]
		} else {
			namePrefix = prefix
			prefix = ""
		}
	} else if !info.IsDir() {
		return "", "", &fs.PathError{Op: "readDir", Path: dirWithPrefix, Err: syscall.ENOTDIR}
	}
	return prefix, namePrefix, nil
}

func (o *fsObjects) readDir() error {
	prefix, namePrefix, err := o.namePrefixes()
	if err != nil {
		return err
	}

	ds, err := fs.ReadDir(o.fsys, path.Join(o.dir, prefix))
	if err != nil {
		return toObjectNotExistIfNoExist(err)
	}
	for _, d := range ds {
		name := path.Join(prefix, d.Name())
		if !strings.HasPrefix(name, namePrefix) {
			continue
		}
		if d.IsDir() {
			name = name + "/"
		}
		if o.query.StartOffset != "" && o.query.StartOffset > name {
			continue
		}
		if d.IsDir() {
			o.attrsList = append(o.attrsList, &storage.ObjectAttrs{
				Bucket: o.dir,
				Prefix: name,
			})
			continue
		}
		info, err := d.Info()
		if err != nil {
			return toObjectNotExistIfNoExist(err)
		}

		o.attrsList = append(o.attrsList, &storage.ObjectAttrs{
			Bucket:  o.dir,
			Name:    name,
			Size:    info.Size(),
			Updated: info.ModTime(),
		})
	}
	return nil
}

func (o *fsObjects) walkDir() error {
	prefix, namePrefix, err := o.namePrefixes()
	if err != nil {
		return err
	}

	root := path.Join(o.dir, prefix)
	namePrefix = path.Join(root, namePrefix)

	return fs.WalkDir(o.fsys, root, func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if name == root || !strings.HasPrefix(name, namePrefix) {
			return nil
		}
		name, err = filepath.Rel(o.dir, name)
		if err != nil {
			return err
		}
		if d.IsDir() {
			name = name + "/"
		}
		if o.query.StartOffset != "" && o.query.StartOffset > name {
			return nil
		}
		if d.IsDir() {
			o.attrsList = append(o.attrsList, &storage.ObjectAttrs{
				Bucket: o.dir,
				Prefix: name,
			})
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return toObjectNotExistIfNoExist(err)
		}
		o.attrsList = append(o.attrsList, &storage.ObjectAttrs{
			Bucket:  o.dir,
			Name:    name,
			Size:    info.Size(),
			Updated: info.ModTime(),
		})
		return nil
	})
}

func (o *fsObjects) nextAttrs() (*storage.ObjectAttrs, error) {
	if err := o.initAttrs(); err != nil {
		return nil, err
	}
	if o.off >= len(o.attrsList) {
		return nil, iterator.Done
	}
	attrs := o.attrsList[o.off]
	o.off++

	return attrs, nil
}

func TestFS(t *testing.T) {
	fsys := &GCSFS{
		bucket: "testdata",
		c:      &fsClient{fsys: osfs.New(".")},
	}
	if err := fstest.TestFS(fsys, "dir0", "dir0/file01.txt"); err != nil {
		t.Errorf("Error testing/fstest: %+v", err)
	}
}

func TestWriteFileFS(t *testing.T) {
	fsys := &GCSFS{
		bucket: "testdata",
		c:      &fsClient{fsys: memfs.New()},
	}
	tmpDir := "test"
	if err := wfs.MkdirAll(fsys, tmpDir, fs.ModePerm); err != nil {
		t.Fatal(err)
	}
	if err := wfstest.TestWriteFileFS(fsys, tmpDir); err != nil {
		t.Errorf("Error wfstest: %+v", err)
	}
}
