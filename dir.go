package gcsfs

import (
	"io"
	"io/fs"
	"sort"
	"syscall"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type gcsDir struct {
	*content
	fsys   *GCSFS
	prefix string
	offset string
	cache  []fs.DirEntry
	eof    bool
}

var _ fs.ReadDirFile = (*gcsDir)(nil)

func newGcsDir(fsys *GCSFS, prefix string) *gcsDir {
	prefix = normalizePrefix(fsys.key(prefix))
	return &gcsDir{
		content: newDirContent(prefix),
		fsys:    fsys,
		prefix:  prefix,
	}
}

// Read reads bytes from this file.
func (d *gcsDir) Read(p []byte) (int, error) {
	return 0, &fs.PathError{Op: "Read", Path: d.prefix, Err: syscall.EISDIR}
}

// Stat returns the fs.FileInfo of this file.
func (d *gcsDir) Stat() (fs.FileInfo, error) {
	return d, nil
}

// Close closes streams.
func (d *gcsDir) Close() error {
	return nil
}

// ReadDir reads the contents of the directory and returns a slice of up to n
// DirEntry values in ascending sorted by filename.
func (d *gcsDir) ReadDir(n int) ([]fs.DirEntry, error) {
	entries, err := d.list(n)
	if err != nil {
		if n <= 0 && err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	if n <= 0 {
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Name() < entries[j].Name()
		})
	}
	return entries, nil
}

func (d *gcsDir) readCache(n int) []fs.DirEntry {
	var entries []fs.DirEntry
	if n > 0 {
		if n >= len(d.cache) {
			entries = d.cache
			d.cache = nil
		} else {
			entries = d.cache[0:n]
			d.cache = d.cache[n:]
		}
	} else {
		entries = d.cache
		d.cache = nil
	}
	return entries
}

func (d *gcsDir) list(n int) ([]fs.DirEntry, error) {
	var entries []fs.DirEntry
	if cacheCount := len(d.cache); cacheCount > 0 {
		entries = d.readCache(n)
		if d.eof || (n > 0 && n <= cacheCount) {
			return entries, nil
		}
		n = n - cacheCount
	}

	if d.eof {
		return nil, io.EOF
	}

	client, err := d.fsys.Client()
	if err != nil {
		return nil, err
	}
	query := &storage.Query{
		Delimiter:   "/",
		Prefix:      d.prefix,
		StartOffset: d.offset,
	}
	it := client.Bucket(d.fsys.bucket).Objects(d.fsys.Context(), query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			d.eof = true
			break
		}
		if err != nil {
			return nil, err
		}
		content := newContent(attrs)
		if d.offset >= content.Name() {
			continue
		}
		entries = append(entries, content)
		if n > 0 && len(entries) >= n {
			break
		}
	}
	if count := len(entries); count > 0 {
		d.offset = entries[count-1].Name()
	}
	return entries, nil
}

// Open called by GCSFS.Open(name string).
// Open calls d.list(n), if the results is empty then returns a PathError
// otherwise sets the results as d.cache.
func (d *gcsDir) open(n int) (*gcsDir, error) {
	entries, err := d.list(n)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, &fs.PathError{Op: "Open", Path: d.prefix, Err: fs.ErrNotExist}
	}
	d.cache = entries
	return d, nil
}
