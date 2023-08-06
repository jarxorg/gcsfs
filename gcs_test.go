package gcsfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
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
		bytes, err := io.ReadAll(req.Body)
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
	return io.NopCloser(strings.NewReader(s))
}

func TestGCSRead(t *testing.T) {
	want := []byte(`test`)

	c := storageClient{c: mockClient(t, &mockTransport{
		results: []transportResult{
			{res: &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBuffer(want)),
			}},
		},
	})}
	defer c.close()

	ctx := context.Background()
	in, err := c.bucket("bucket").object("test.txt").newReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()

	got, err := io.ReadAll(in)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Error got %v; want %v", want, got)
	}
}

func TestGCSAttrs(t *testing.T) {
	c := storageClient{c: mockClient(t, &mockTransport{
		results: []transportResult{
			{res: &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("{}")),
			}},
		},
	})}
	defer c.close()

	ctx := context.Background()
	_, err := c.bucket("bucket").object("test.txt").attrs(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGCSWrite(t *testing.T) {
	c := storageClient{c: mockClient(t, &mockTransport{
		results: []transportResult{
			{res: &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("{}")),
			}},
		},
	})}
	defer c.close()

	ctx := context.Background()
	out := c.bucket("bucket").object("test.txt").newWriter(ctx)
	defer out.Close()

	_, err := out.Write([]byte("test"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestGCSDelete(t *testing.T) {
	c := storageClient{c: mockClient(t, &mockTransport{
		results: []transportResult{
			{res: &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("{}")),
			}},
		},
	})}
	defer c.close()

	ctx := context.Background()
	err := c.bucket("bucket").object("test.txt").delete(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGCSObjects(t *testing.T) {
	c := storageClient{c: mockClient(t, &mockTransport{
		results: []transportResult{
			{res: &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("{}")),
			}},
		},
	})}
	defer c.close()

	ctx := context.Background()
	it := c.bucket("bucket").objects(ctx, &storage.Query{})

	_, err := it.nextAttrs()
	if err != iterator.Done {
		t.Errorf(`Unknown response: %v`, err)
	}
}
