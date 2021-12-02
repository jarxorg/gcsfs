package gcsfs

import (
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/jarxorg/wfs"
	"github.com/jarxorg/wfs/memfs"
	"github.com/jarxorg/wfs/osfs"
	"github.com/jarxorg/wfs/wfstest"
)

func TestFS(t *testing.T) {
	fsys := &GCSFS{
		bucket: "testdata",
		cl:     &fsClient{fsys: osfs.New(".")},
	}
	if err := fstest.TestFS(fsys, "dir0", "dir0/file01.txt"); err != nil {
		t.Errorf("Error testing/fstest: %+v", err)
	}
}

func TestWriteFileFS(t *testing.T) {
	fsys := &GCSFS{
		bucket: "testdata",
		cl:     &fsClient{fsys: memfs.New()},
	}
	tmpDir := "test"
	if err := wfs.MkdirAll(fsys, tmpDir, fs.ModePerm); err != nil {
		t.Fatal(err)
	}
	if err := wfstest.TestWriteFileFS(fsys, tmpDir); err != nil {
		t.Errorf("Error wfstest: %+v", err)
	}
}
