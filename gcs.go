package gcsfs

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
)

type gcsClient interface {
	bucket(name string) gcsBucket
	close() error
}

type gcsBucket interface {
	object(name string) gcsObject
	objects(ctx context.Context, q *storage.Query) gcsObjectItetator
}

type gcsObject interface {
	attrs(ctx context.Context) (*storage.ObjectAttrs, error)
	newReader(ctx context.Context) (io.ReadCloser, error)
	newWriter(ctx context.Context) io.WriteCloser
	delete(ctx context.Context) error
}

type gcsObjectItetator interface {
	nextAttrs() (*storage.ObjectAttrs, error)
}

type storageClient struct {
	c *storage.Client
}

var _ gcsClient = (*storageClient)(nil)

func (c *storageClient) bucket(name string) gcsBucket {
	return &storageBucket{b: c.c.Bucket(name)}
}

func (c *storageClient) close() error {
	return c.c.Close()
}

type storageBucket struct {
	b *storage.BucketHandle
}

func (b *storageBucket) object(name string) gcsObject {
	return &storageObject{obj: b.b.Object(name)}
}

func (b *storageBucket) objects(ctx context.Context, q *storage.Query) gcsObjectItetator {
	return &storageObjectIterator{itr: b.b.Objects(ctx, q)}
}

type storageObject struct {
	obj *storage.ObjectHandle
}

func (o *storageObject) newReader(ctx context.Context) (io.ReadCloser, error) {
	return o.obj.NewReader(ctx)
}

func (o *storageObject) newWriter(ctx context.Context) io.WriteCloser {
	return o.obj.NewWriter(ctx)
}

func (o *storageObject) attrs(ctx context.Context) (*storage.ObjectAttrs, error) {
	return o.obj.Attrs(ctx)
}

func (o *storageObject) delete(ctx context.Context) error {
	return o.obj.Delete(ctx)
}

type storageObjectIterator struct {
	itr *storage.ObjectIterator
}

func (i *storageObjectIterator) nextAttrs() (*storage.ObjectAttrs, error) {
	return i.itr.Next()
}
