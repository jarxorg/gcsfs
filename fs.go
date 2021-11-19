package gcsfs

import (
	"context"
	"io/fs"
	"io/ioutil"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

const (
	defaultDirOpenBufferSize = 100
)

// GCSFS represents a filesystem on GCS (Google Cloud Storage).
type GCSFS struct {
	// DirOpenBufferSize is the buffer size for using objects as the directory. (Default 100)
	DirOpenBufferSize int
	bucket         string
	dir            string
	ctx            context.Context
	client         *storage.Client
}

var (
	_ fs.FS         = (*GCSFS)(nil)
	_ fs.StatFS     = (*GCSFS)(nil)
	_ fs.ReadDirFS  = (*GCSFS)(nil)
	_ fs.ReadFileFS = (*GCSFS)(nil)
	_ fs.SubFS      = (*GCSFS)(nil)
	_ fs.GlobFS     = (*GCSFS)(nil)
)

// New returns a filesystem for the tree of objects rooted at the specified bucket.
func New(bucket string) *GCSFS {
	return NewWithClient(bucket, nil)
}

// NewWithClient returns a filesystem for the tree of objects rooted at the specified bucket with *storage.Client.
// The specified client will be closed by Close.
//
//   ctx := context.Background()
//   client, err := storage.NewClient(ctx)
//   if err != nil {
//     log.Fatal(err)
//   }
//   fsys := gcsfs.NewWithClient("<your-bucket>", client).WithContext(ctx)
//   defer fsys.Close() // Close closes the specified client.
func NewWithClient(bucket string, client *storage.Client) *GCSFS {
	return &GCSFS{
		DirOpenBufferSize: defaultDirOpenBufferSize,
		bucket:            bucket,
		client:            client,
	}
}

// WithClient holds the specified client. The specified client is closed by Close.
func (fsys *GCSFS) WithClient(client *storage.Client) *GCSFS {
	fsys.client = client
	return fsys
}

// WithContext holds the specified context.
func (fsys *GCSFS) WithContext(ctx context.Context) *GCSFS {
	fsys.ctx = ctx
	return fsys
}

// Close closes holded storage client.
func (fsys *GCSFS) Close() error {
	if fsys.client == nil {
		return nil
	}
	err := fsys.client.Close()
	fsys.client = nil
	return err
}

// Context returns a holded context. If this filesystem has no context then
// context.Background() will use.
func (fsys *GCSFS) Context() context.Context {
	if fsys.ctx == nil {
		fsys.ctx = context.Background()
	}
	return fsys.ctx
}

// Client returns a holded storage client. If this filesystem has no client then
// storage.NewClient(fsys.Context()) will call.
func (fsys *GCSFS) Client() (*storage.Client, error) {
	if fsys.client == nil {
		var err error
		fsys.client, err = storage.NewClient(fsys.Context())
		if err != nil {
			return nil, err
		}
	}
	return fsys.client, nil
}

func (fsys *GCSFS) key(name string) string {
	return path.Clean(path.Join(fsys.dir, name))
}

func (fsys *GCSFS) rel(name string) string {
	return strings.TrimPrefix(name, normalizePrefix(fsys.dir))
}

func (fsys *GCSFS) openFile(name string) (*gcsFile, error) {
	if !fs.ValidPath(name) {
		return nil, toPathError(fs.ErrInvalid, "Open", name)
	}
	client, err := fsys.Client()
	if err != nil {
		return nil, toPathError(err, "Open", name)
	}
	obj := client.Bucket(fsys.bucket).Object(fsys.key(name))
	attrs, err := obj.Attrs(fsys.ctx)
	if err != nil {
		return nil, toPathError(err, "Open", name)
	}
	if attrs.Name == "" && attrs.Prefix == "" {
		return nil, toPathError(storage.ErrObjectNotExist, "Open", name)
	}
	return newGcsFile(fsys, obj, attrs), nil
}

// Open opens the named file or directory.
func (fsys *GCSFS) Open(name string) (fs.File, error) {
	f, err := fsys.openFile(name)
	if err != nil && isNotExist(err) {
		return newGcsDir(fsys, name).open(fsys.DirOpenBufferSize)
	}
	return f, err
}

// Stat returns a FileInfo describing the file. If there is an error, it should be
// of type *PathError.
func (fsys *GCSFS) Stat(name string) (fs.FileInfo, error) {
	f, err := fsys.openFile(name)
	if err != nil && isNotExist(err) {
		return newGcsDir(fsys, name).open(1)
	}
	return f, err
}

// ReadDir reads the named directory and returns a list of directory entries
// sorted by filename.
func (fsys *GCSFS) ReadDir(dir string) ([]fs.DirEntry, error) {
	if !fs.ValidPath(dir) {
		return nil, toPathError(fs.ErrInvalid, "ReadDir", dir)
	}
	entries, err := newGcsDir(fsys, dir).ReadDir(-1)
	return entries, err
}

// ReadFile reads the named file and returns its contents.
func (fsys *GCSFS) ReadFile(name string) ([]byte, error) {
	f, err := fsys.openFile(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return ioutil.ReadAll(f)
}

// Sub returns an FS corresponding to the subtree rooted at dir.
func (fsys *GCSFS) Sub(dir string) (fs.FS, error) {
	if !fs.ValidPath(dir) {
		return nil, toPathError(fs.ErrInvalid, "Sub", dir)
	}
	subFsys := NewWithClient(fsys.bucket, fsys.client).WithContext(fsys.Context())
	subFsys.dir = path.Join(fsys.dir, dir)
	return subFsys, nil
}

// Glob returns the names of all files matching pattern, providing an implementation
// of the top-level Glob function.
func (fsys *GCSFS) Glob(pattern string) ([]string, error) {
	client, err := fsys.Client()
	if err != nil {
		return nil, err
	}
	query := &storage.Query{
		Prefix: normalizePrefix(fsys.dir),
	}
	it := client.Bucket(fsys.bucket).Objects(fsys.Context(), query)

	var names []string
	matchAppend := func(name string) error {
		ok, err := path.Match(pattern, name)
		if err != nil {
			return toPathError(err, "Glob", pattern)
		}
		if ok {
			names = append(names, name)
		}
		return nil
	}

	lastDir := ""
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, toPathError(err, "Glob", pattern)
		}
		name := attrs.Name
		if name == "" {
			name = strings.TrimSuffix(attrs.Prefix, "/")
		}
		name = fsys.rel(name)
		if dir := path.Dir(name); dir != lastDir {
			if err := matchAppend(dir); err != nil {
				return nil, err
			}
			lastDir = dir
		}
		if err := matchAppend(name); err != nil {
			return nil, err
		}
	}
	return names, nil
}
