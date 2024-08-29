package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/minio/minio-go/v7"
)

func TestWriteBucket(t *testing.T) {
	runWithMinio(t, func(client *minio.Client) {
		const bucket = "test"
		ctx := context.Background()

		// Create the bucket
		err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		if err != nil {
			t.Fatalf("unable to create bucket: %s", err)
		}

		// Write the object
		size := int64(100 * 1024)
		r := newSizeReader(100 * 1024)
		info, err := client.PutObject(ctx, bucket, "prefix/test.bin", r, size, minio.PutObjectOptions{})
		if err != nil {
			t.Fatalf("unable to write object: %s", err)
		}
		const expectedETag = "44d088cef136d178e9c8ba84c3fdf6ca"
		if info.ETag != expectedETag {
			t.Fatalf("invalid ETag %s, expected %s", info.ETag, expectedETag)
		}

		// Read the object
		obj, err := client.GetObject(ctx, bucket, "prefix/test.bin", minio.GetObjectOptions{})
		if err != nil {
			t.Fatalf("unable to get object: %s", err)
		}
		hash, err := calcSHA(obj)
		if err != nil {
			t.Fatalf("unable to determine has: %s", err)
		}
		const expectedHash = "27783e87963a4efb6829b531c9ba57b44f45797f6770bd637fbf0d807cbdbae0"
		gotHash := hex.EncodeToString(hash)
		if gotHash != expectedHash {
			t.Fatalf("invalid ETag %s, expected %s", info.ETag, expectedETag)
		}
	})
}

func TestListBuckets(t *testing.T) {
	runWithMinio(t, func(client *minio.Client) {
		ctx := context.Background()

		// List the buckets
		buckets, err := client.ListBuckets(ctx)
		if err != nil {
			t.Fatalf("unable to list buckets: %s", err)
		}
		if len(buckets) != 0 {
			t.Fatal("didn't expect any buckets")
		}

		// Create three buckets
		for i := range 3 {
			// Create the bucket
			bucket := fmt.Sprintf("bucket-%d", i)
			err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
			if err != nil {
				t.Fatalf("unable to create bucket %s: %s", bucket, err)
			}
		}

		// List the buckets
		buckets, err = client.ListBuckets(ctx)
		if err != nil {
			t.Fatalf("unable to list buckets: %s", err)
		}
		if len(buckets) != 3 {
			t.Fatal("didn't expect any buckets")
		}
	})
}
