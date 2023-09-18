//go:build integtest
// +build integtest

package gcsfs

import (
	"context"
	"log"
	"os"
	"strings"
	"testing"
	"testing/fstest"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

func TestFSIntegration(t *testing.T) {
	bucket := os.Getenv("FSTEST_BUCKET")
	expected := os.Getenv("FSTEST_EXPECTED")
	if bucket == "" || expected == "" {
		t.Fatalf("Require ENV FSTEST_BUCKET=%s FSTEST_EXPECTED=%s", bucket, expected)
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		log.Fatal(err)
	}
	fsys := New(bucket).WithClient(client).WithContext(ctx)
	if err := fstest.TestFS(fsys, strings.Split(expected, ",")...); err != nil {
		t.Errorf("Error testing/fstest: %+v", err)
	}
}
