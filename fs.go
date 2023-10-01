package gcsfs

import (
	"context"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"
	"syscall"

	"cloud.google.com/go/storage"
	"github.com/jarxorg/wfs"
	"google.golang.org/api/iterator"
)

const (
	defaultDirOpenBufferSize = 100
)

// GCSFS represents a filesystem on GCS (Google Cloud Storage).
type GCSFS struct {
	// DirOpenBufferSize is the buffer size for using objects as the directory. (Default 100)
	DirOpenBufferSize int
	bucket            string
	dir               string
	ctx               context.Context
	c                 gcsClient
}

var (
	_ fs.FS            = (*GCSFS)(nil)
	_ fs.StatFS        = (*GCSFS)(nil)
	_ fs.ReadDirFS     = (*GCSFS)(nil)
	_ fs.ReadFileFS    = (*GCSFS)(nil)
	_ fs.SubFS         = (*GCSFS)(nil)
	_ fs.GlobFS        = (*GCSFS)(nil)
	_ wfs.WriteFileFS  = (*GCSFS)(nil)
	_ wfs.RemoveFileFS = (*GCSFS)(nil)
)

// New returns a filesystem for the tree of objects rooted at the specified bucket.
func New(bucket string) *GCSFS {
	return &GCSFS{
		DirOpenBufferSize: defaultDirOpenBufferSize,
		bucket:            bucket,
	}
}

// NewWithClient returns a filesystem for the tree of objects rooted at the specified bucket with *storage.Client.
// The specified client will be closed by Close.
//
//	ctx := context.Background()
//	client, err := storage.NewClient(ctx)
//	if err != nil {
//	  log.Fatal(err)
//	}
//	fsys := gcsfs.NewWithClient("<your-bucket>", client).WithContext(ctx)
//	defer fsys.Close() // Close closes the specified client.
func NewWithClient(bucket string, client *storage.Client) *GCSFS {
	return New(bucket).WithClient(client)
}

// WithClient holds the specified client. The specified client is closed by Close.
func (fsys *GCSFS) WithClient(client *storage.Client) *GCSFS {
	fsys.c = &storageClient{c: client}
	return fsys
}

// WithContext holds the specified context.
func (fsys *GCSFS) WithContext(ctx context.Context) *GCSFS {
	fsys.ctx = ctx
	return fsys
}

// Close closes holded storage client.
func (fsys *GCSFS) Close() error {
	if fsys.c == nil {
		return nil
	}
	err := fsys.c.close()
	fsys.c = nil
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

func (fsys *GCSFS) client() (gcsClient, error) {
	if fsys.c == nil {
		client, err := storage.NewClient(fsys.Context())
		if err != nil {
			return nil, err
		}
		fsys.c = &storageClient{c: client}
	}
	return fsys.c, nil
}

func (fsys *GCSFS) key(name string) string {
	return path.Join(fsys.dir, name)
}

func (fsys *GCSFS) rel(name string) string {
	return strings.TrimPrefix(name, normalizePrefix(fsys.dir))
}

func (fsys *GCSFS) openFile(name string) (*gcsFile, error) {
	if !fs.ValidPath(name) {
		return nil, toPathError(fs.ErrInvalid, "Open", name)
	}
	c, err := fsys.client()
	if err != nil {
		return nil, toPathError(err, "Open", name)
	}

	obj := c.bucket(fsys.bucket).object(fsys.key(name))
	attrs, err := obj.attrs(fsys.ctx)
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
	return newGcsDir(fsys, dir).ReadDir(-1)
}

// ReadFile reads the named file and returns its contents.
func (fsys *GCSFS) ReadFile(name string) ([]byte, error) {
	f, err := fsys.openFile(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return io.ReadAll(f)
}

// Sub returns an FS corresponding to the subtree rooted at dir.
func (fsys *GCSFS) Sub(dir string) (fs.FS, error) {
	if !fs.ValidPath(dir) {
		return nil, toPathError(fs.ErrInvalid, "Sub", dir)
	}
	cl, err := fsys.client()
	if err != nil {
		return nil, err
	}

	return &GCSFS{
		bucket: fsys.bucket,
		c:      cl,
		ctx:    fsys.Context(),
		dir:    path.Join(fsys.dir, dir),
	}, nil
}

// Glob returns the names of all files matching pattern, providing an implementation
// of the top-level Glob function.
func (fsys *GCSFS) Glob(pattern string) ([]string, error) {
	if pattern == "" || pattern == "*" {
		entries, err := fsys.ReadDir("")
		if err != nil {
			return nil, err
		}
		var names []string
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		return names, nil
	}
	// NOTE: Validate pattern
	if _, err := path.Match(pattern, ""); err != nil {
		return nil, err
	}
	names, err := fsys.glob([]string{""}, strings.Split(pattern, "/"), nil)
	if err != nil {
		return nil, err
	}
	var matches []string
	for _, name := range names {
		matches = appendIfMatch(matches, name, pattern)
	}
	sort.Strings(matches)
	return matches, nil
}

func (fsys *GCSFS) glob(dirs, patterns []string, matches []string) ([]string, error) {
	dirOnly := len(patterns) > 1
	var subDirs []string
	for _, dir := range dirs {
		keys, err := fsys.listForGlob(path.Join(dir, patterns[0]), dirOnly)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			if dirOnly {
				subDirs = append(subDirs, key)
			}
			matches = append(matches, key)
		}
	}
	if len(subDirs) > 0 && dirOnly {
		return fsys.glob(subDirs, patterns[1:], matches)
	}
	return matches, nil
}

func (fsys *GCSFS) listForGlob(pattern string, dirOnly bool) ([]string, error) {
	c, err := fsys.client()
	if err != nil {
		return nil, err
	}
	query := newQuery("/", normalizePrefixPattern(fsys.dir, pattern), "")
	it := c.bucket(fsys.bucket).objects(fsys.Context(), query)

	var names []string
	for {
		attrs, err := it.nextAttrs()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, toPathError(err, "Glob", pattern)
		}
		if attrs.Name == "" {
			name := fsys.rel(strings.TrimSuffix(attrs.Prefix, "/"))
			names = appendIfMatch(names, name, pattern)
			continue
		}
		if dirOnly {
			continue
		}
		name := fsys.rel(attrs.Name)
		names = appendIfMatch(names, name, pattern)
	}
	return names, nil
}

// MkdirAll always do nothing.
func (fsys *GCSFS) MkdirAll(dir string, mode fs.FileMode) error {
	return nil
}

func (fsys *GCSFS) createFile(name string) (*gcsWriterFile, error) {
	if !fs.ValidPath(name) {
		return nil, toPathError(fs.ErrInvalid, "Create", name)
	}
	c, err := fsys.client()
	if err != nil {
		return nil, toPathError(err, "Create", name)
	}

	if _, err := fsys.openFile(name); err != nil {
		if !isNotExist(err) {
			return nil, toPathError(err, "CreateFile", name)
		}
		if _, err := newGcsDir(fsys, name).open(1); err == nil {
			return nil, toPathError(syscall.EISDIR, "CreateFile", name)
		}
	}
	dir := path.Dir(name)
	if _, err := fsys.openFile(dir); err == nil {
		return nil, toPathError(syscall.ENOTDIR, "CreateFile", dir)
	}

	obj := c.bucket(fsys.bucket).object(fsys.key(name))
	return newGcsWriterFile(fsys, obj, name), nil
}

// CreateFile creates the named file.
// The specified mode is ignored.
func (fsys *GCSFS) CreateFile(name string, mode fs.FileMode) (wfs.WriterFile, error) {
	return fsys.createFile(name)
}

// WriteFile writes the specified bytes to the named file.
// The specified mode is ignored.
func (fsys *GCSFS) WriteFile(name string, p []byte, mode fs.FileMode) (int, error) {
	f, err := fsys.createFile(name)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	n, err := f.Write(p)
	if err != nil {
		return 0, toPathError(err, "WriteFile", name)
	}
	return n, nil
}

// RemoveFile removes the specified named file.
func (fsys *GCSFS) RemoveFile(name string) error {
	if !fs.ValidPath(name) {
		return toPathError(fs.ErrInvalid, "RemoveFile", name)
	}
	c, err := fsys.client()
	if err != nil {
		return toPathError(err, "RemoveFile", name)
	}

	obj := c.bucket(fsys.bucket).object(fsys.key(name))
	return toPathError(obj.delete(fsys.Context()), "RemoveFile", name)
}

// RemoveAll removes path and any children it contains.
func (fsys *GCSFS) RemoveAll(dir string) error {
	if !fs.ValidPath(dir) {
		return toPathError(fs.ErrInvalid, "RemoveAll", dir)
	}
	c, err := fsys.client()
	if err != nil {
		return toPathError(err, "RemoveAll", dir)
	}

	b := c.bucket(fsys.bucket)
	ctx := fsys.Context()
	query := newQuery("", normalizePrefix(fsys.key(dir)), "")
	it := b.objects(fsys.Context(), query)
	for {
		attrs, err := it.nextAttrs()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return toPathError(err, "RemoveAll", dir)
		}
		name := path.Join(attrs.Prefix, attrs.Name)
		obj := b.object(name)
		if err := obj.delete(ctx); err != nil {
			return toPathError(err, "RemoveAll", name)
		}
	}
	return nil
}
