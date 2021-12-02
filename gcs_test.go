package gcsfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"net/http"
	"path"
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/jarxorg/wfs"
	"github.com/jarxorg/wfs/memfs"
	"github.com/jarxorg/wfs/osfs"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type mockTransport struct {
	gotReq  *http.Request
	gotBody []byte
	results []transportResult
}

type transportResult struct {
	res *http.Response
	err error
}

func (t *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.gotReq = req
	t.gotBody = nil
	if req.Body != nil {
		bytes, err := ioutil.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		t.gotBody = bytes
	}
	if len(t.results) == 0 {
		return nil, fmt.Errorf("error handling request")
	}
	result := t.results[0]
	t.results = t.results[1:]
	return result.res, result.err
}

func mockClient(t *testing.T, m *mockTransport) *storage.Client {
	cl, err := storage.NewClient(context.Background(), option.WithHTTPClient(&http.Client{Transport: m}))
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

func bodyReader(s string) io.ReadCloser {
	return ioutil.NopCloser(strings.NewReader(s))
}

func TestGCSRead(t *testing.T) {
	want := []byte(`test`)

	cl := gcsClient{cl: mockClient(t, &mockTransport{
		results: []transportResult{
			{res: &http.Response{
				StatusCode: http.StatusOK,
				Body:       ioutil.NopCloser(bytes.NewBuffer(want)),
			}},
		},
	})}
	defer cl.close()

	ctx := context.Background()
	in, err := cl.bucket("bucket").object("test.txt").newReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()

	got, err := ioutil.ReadAll(in)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Error got %v; want %v", want, got)
	}
}

func TestGCSAttrs(t *testing.T) {
	cl := gcsClient{cl: mockClient(t, &mockTransport{
		results: []transportResult{
			{res: &http.Response{
				StatusCode: http.StatusOK,
				Body:       ioutil.NopCloser(strings.NewReader("{}")),
			}},
		},
	})}
	defer cl.close()

	ctx := context.Background()
	_, err := cl.bucket("bucket").object("test.txt").attrs(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGCSWrite(t *testing.T) {
	cl := gcsClient{cl: mockClient(t, &mockTransport{
		results: []transportResult{
			{res: &http.Response{
				StatusCode: http.StatusOK,
				Body:       ioutil.NopCloser(strings.NewReader("{}")),
			}},
		},
	})}
	defer cl.close()

	ctx := context.Background()
	out := cl.bucket("bucket").object("test.txt").newWriter(ctx)
	defer out.Close()

	_, err := out.Write([]byte("test"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestGCSDelete(t *testing.T) {
	cl := gcsClient{cl: mockClient(t, &mockTransport{
		results: []transportResult{
			{res: &http.Response{
				StatusCode: http.StatusOK,
				Body:       ioutil.NopCloser(strings.NewReader("{}")),
			}},
		},
	})}
	defer cl.close()

	ctx := context.Background()
	err := cl.bucket("bucket").object("test.txt").delete(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGCSObjects(t *testing.T) {
	cl := gcsClient{cl: mockClient(t, &mockTransport{
		results: []transportResult{
			{res: &http.Response{
				StatusCode: http.StatusOK,
				Body:       ioutil.NopCloser(strings.NewReader("{}")),
			}},
		},
	})}
	defer cl.close()

	ctx := context.Background()
	it := cl.bucket("bucket").objects(ctx, &storage.Query{})

	_, err := it.nextAttrs()
	if err != iterator.Done {
		t.Errorf(`Unknown response: %v`, err)
	}
}

func TestFSRead(t *testing.T) {
	want := []byte("content0\n")

	cl := &fsClient{fsys: osfs.New(".")}
	defer cl.close()

	ctx := context.Background()
	in, err := cl.bucket("testdata").object("file0.txt").newReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()

	got, err := ioutil.ReadAll(in)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Error got %v; want %v", want, got)
	}
}

func TestFSRead_ObjectNotExist(t *testing.T) {
	cl := &fsClient{fsys: osfs.New(".")}
	defer cl.close()

	ctx := context.Background()
	in, err := cl.bucket("testdata").object("not-exist.txt").newReader(ctx)
	if err == nil {
		in.Close()
	}
	if err != storage.ErrObjectNotExist {
		t.Fatalf("Error got %v; want %v", err, storage.ErrObjectNotExist)
	}
}

func TestFSWrite(t *testing.T) {
	cl := &fsClient{fsys: memfs.New()}
	defer cl.close()

	ctx := context.Background()
	out := cl.bucket("bucket").object("test.txt").newWriter(ctx)
	defer out.Close()

	want := []byte("test")
	_, err := out.Write(want)
	if err != nil {
		t.Fatal(err)
	}
	out.Close()

	in, err := cl.bucket("bucket").object("test.txt").newReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()

	got, err := ioutil.ReadAll(in)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Error write/read got %v; want %v", got, want)
	}
}

func TestFSWrite_Error(t *testing.T) {
	want := errors.New("test")
	cl := &fsClient{
		fsys: &wfs.FSDelegator{
			CreateFileFunc: func(name string, mode fs.FileMode) (wfs.WriterFile, error) {
				return nil, want
			},
		},
	}
	defer cl.close()

	ctx := context.Background()
	out := cl.bucket("bucket").object("test.txt").newWriter(ctx)
	defer out.Close()

	_, got := out.Write([]byte{})
	if got != want {
		t.Fatalf("Error write got %v; want %v", got, want)
	}
}

func TestFSAttrs(t *testing.T) {
	fsys := osfs.New(".")
	info, err := fs.Stat(fsys, "testdata/file0.txt")
	if err != nil {
		t.Fatal(err)
	}
	want := &storage.ObjectAttrs{
		Bucket:  "testdata",
		Name:    "file0.txt",
		Size:    info.Size(),
		Updated: info.ModTime(),
	}

	cl := &fsClient{fsys: fsys}
	defer cl.close()

	ctx := context.Background()
	got, err := cl.bucket("testdata").object("file0.txt").attrs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Error attrs got %v; want %v", got, want)
	}
}

func TestFSAttrs_ObjectNotExist(t *testing.T) {
	fsys := osfs.New(".")
	cl := &fsClient{fsys: fsys}
	defer cl.close()

	ctx := context.Background()
	_, got := cl.bucket("testdata").object("not-exist.txt").attrs(ctx)
	if got != storage.ErrObjectNotExist {
		t.Fatalf("Error attrs got %v; want %v", got, storage.ErrObjectNotExist)
	}
}

func TestFSAttrs_Dir_ObjectNotExist(t *testing.T) {
	fsys := osfs.New(".")
	cl := &fsClient{fsys: fsys}
	defer cl.close()

	ctx := context.Background()
	_, got := cl.bucket("testdata").object("dir0").attrs(ctx)
	if got != storage.ErrObjectNotExist {
		t.Fatalf("Error attrs got %v; want %v", got, storage.ErrObjectNotExist)
	}
}

func TestFSDelete(t *testing.T) {
	cl := &fsClient{fsys: memfs.New()}
	defer cl.close()

	ctx := context.Background()
	err := cl.bucket("bucket").object("file.txt").delete(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestFSAttrs_ReadDir(t *testing.T) {
	cl := &fsClient{fsys: osfs.New(".")}
	defer cl.close()

	ctx := context.Background()
	query := &storage.Query{Prefix: "", Delimiter: "/", StartOffset: "file1.txt"}
	it := cl.bucket("testdata").objects(ctx, query)
	var got []string
	for {
		attrs, err := it.nextAttrs()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, path.Join(attrs.Prefix, attrs.Name))
	}

	want := []string{"file1.txt", "file2.txt"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Error objects got %v; want %v", got, want)
	}
}

func TestFSAttrs_ReadDirError(t *testing.T) {
	want := errors.New("test")
	fsys := wfs.DelegateFS(osfs.New("."))
	fsys.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
		return nil, want
	}

	cl := &fsClient{fsys: fsys}
	defer cl.close()

	ctx := context.Background()
	query := &storage.Query{Prefix: "dir0/", Delimiter: "/"}
	it := cl.bucket("testdata").objects(ctx, query)
	var got error
	for {
		_, err := it.nextAttrs()
		if err == iterator.Done {
			break
		}
		if err != nil {
			got = err
			break
		}
	}
	if got != want {
		t.Fatalf("Error attrs got %v; want %v", got, want)
	}
}

func TestFSAttrs_ReadDir_InfoError(t *testing.T) {
	want := errors.New("test")
	ds := []fs.DirEntry{
		&wfs.DirEntryDelegator{
			InfoFunc: func() (fs.FileInfo, error) {
				return nil, want
			},
		},
	}
	fsys := wfs.DelegateFS(osfs.New("."))
	fsys.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
		return ds, nil
	}

	cl := &fsClient{fsys: fsys}
	defer cl.close()

	ctx := context.Background()
	query := &storage.Query{Prefix: "dir0/", Delimiter: "/"}
	it := cl.bucket("testdata").objects(ctx, query)
	var got error
	for {
		_, err := it.nextAttrs()
		if err == iterator.Done {
			break
		}
		if err != nil {
			got = err
			break
		}
	}
	if got != want {
		t.Fatalf("Error attrs got %v; want %v", got, want)
	}
}

func TestFSAttrs_WalkDir(t *testing.T) {
	cl := &fsClient{fsys: osfs.New(".")}
	defer cl.close()

	ctx := context.Background()
	query := &storage.Query{StartOffset: "file1.txt"}
	it := cl.bucket("testdata").objects(ctx, query)
	var got []string
	for {
		attrs, err := it.nextAttrs()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, path.Join(attrs.Prefix, attrs.Name))
	}

	want := []string{"file1.txt", "file2.txt"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Error objects got %v; want %v", got, want)
	}
}

func TestFSAttrs_WalkDirError(t *testing.T) {
	want := errors.New("test")
	fsys := wfs.DelegateFS(osfs.New("."))
	fsys.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
		return nil, want
	}

	cl := &fsClient{fsys: fsys}
	defer cl.close()

	ctx := context.Background()
	query := &storage.Query{Prefix: "dir0/"}
	it := cl.bucket("testdata").objects(ctx, query)
	var got error
	for {
		_, err := it.nextAttrs()
		if err == iterator.Done {
			break
		}
		if err != nil {
			got = err
			break
		}
	}
	if got != want {
		t.Fatalf("Error attrs got %v; want %v", got, want)
	}
}

func TestFSAttrs_WalkDir_InfoError(t *testing.T) {
	want := errors.New("test")
	ds := []fs.DirEntry{
		&wfs.DirEntryDelegator{
			InfoFunc: func() (fs.FileInfo, error) {
				return nil, want
			},
		},
	}
	fsys := wfs.DelegateFS(osfs.New("."))
	fsys.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
		return ds, nil
	}

	cl := &fsClient{fsys: fsys}
	defer cl.close()

	ctx := context.Background()
	query := &storage.Query{Prefix: "dir0/"}
	it := cl.bucket("testdata").objects(ctx, query)
	var got error
	for {
		_, err := it.nextAttrs()
		if err == iterator.Done {
			break
		}
		if err != nil {
			got = err
			break
		}
	}
	if got != want {
		t.Fatalf("Error attrs got %v; want %v", got, want)
	}
}
