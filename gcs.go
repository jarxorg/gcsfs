package gcsfs

import (
	"context"
	"io"
	"io/fs"
	"path"

	"cloud.google.com/go/storage"
	"github.com/jarxorg/io2"
	"github.com/jarxorg/wfs"
	"google.golang.org/api/iterator"
)

type client interface {
	bucket(name string) bucket
	close() error
}

type bucket interface {
	object(name string) object
	objects(ctx context.Context, q *storage.Query) objectItetator
}

type object interface {
	attrs(ctx context.Context) (*storage.ObjectAttrs, error)
	newReader(ctx context.Context) (io.ReadCloser, error)
	newWriter(ctx context.Context) io.WriteCloser
	delete(ctx context.Context) error
}

type objectItetator interface {
	nextAttrs() (*storage.ObjectAttrs, error)
}

// gcs implementations

type gcsClient struct {
	cl *storage.Client
}

func (c *gcsClient) bucket(name string) bucket {
	return &gcsBucket{bkt: c.cl.Bucket(name)}
}

func (c *gcsClient) close() error {
	return c.cl.Close()
}

type gcsBucket struct {
	bkt *storage.BucketHandle
}

func (b *gcsBucket) object(name string) object {
	return &gcsObject{obj: b.bkt.Object(name)}
}

func (b *gcsBucket) objects(ctx context.Context, q *storage.Query) objectItetator {
	return &gcsObjectIterator{itr: b.bkt.Objects(ctx, q)}
}

type gcsObject struct {
	obj *storage.ObjectHandle
}

func (o *gcsObject) newReader(ctx context.Context) (io.ReadCloser, error) {
	return o.obj.NewReader(ctx)
}

func (o *gcsObject) newWriter(ctx context.Context) io.WriteCloser {
	return o.obj.NewWriter(ctx)
}

func (o *gcsObject) attrs(ctx context.Context) (*storage.ObjectAttrs, error) {
	return o.obj.Attrs(ctx)
}

func (o *gcsObject) delete(ctx context.Context) error {
	return o.obj.Delete(ctx)
}

type gcsObjectIterator struct {
	itr *storage.ObjectIterator
}

func (i *gcsObjectIterator) nextAttrs() (*storage.ObjectAttrs, error) {
	return i.itr.Next()
}

// filesystem implementations

type fsClient struct {
	fsys fs.FS
}

func (c *fsClient) bucket(name string) bucket {
	return &fsBucket{fsys: c.fsys, dir: name}
}

func (c *fsClient) close() error {
	return nil
}

type fsBucket struct {
	fsys fs.FS
	dir  string
}

func (b *fsBucket) object(name string) object {
	return &fsObject{fsys: b.fsys, dir: b.dir, name: name}
}

func (b *fsBucket) objects(ctx context.Context, q *storage.Query) objectItetator {
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

func (o *fsObjects) readDir() error {
	ds, err := fs.ReadDir(o.fsys, path.Join(o.dir, o.query.Prefix))
	if err != nil {
		return toObjectNotExistIfNoExist(err)
	}
	for _, d := range ds {
		name := path.Join(o.query.Prefix, d.Name())
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
	root := path.Join(o.dir, o.query.Prefix)

	return fs.WalkDir(o.fsys, root, func(dir string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := path.Join(o.query.Prefix, d.Name())
		if name == root {
			return nil
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
