package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	MinioImage    = "quay.io/minio/minio:latest"
	MinioUser     = "minio"
	MinioPassword = "minio123"
)

func calcSHA(r io.Reader) ([]byte, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func newSizeReader(n int64) io.Reader {
	return &sizeReader{len: n}
}

type sizeReader struct {
	pos, len int64
}

// Read implements io.Reader.
func (s *sizeReader) Read(p []byte) (n int, err error) {
	if s.pos == s.len {
		return 0, io.EOF
	}
	len := int(len(p))
	if len > int(s.len-s.pos) {
		len = int(s.len - s.pos)
		p = p[:len]
	}
	for i := range p {
		p[i] = byte(s.pos & 255)
		s.pos++
	}
	return len, nil
}

func runWithMinio(t *testing.T, testFunc func(client *minio.Client)) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        MinioImage,
		ExposedPorts: []string{"9000/tcp"},
		WaitingFor:   wait.ForHTTP("/minio/health/live").WithPort("9000"),
		Env: map[string]string{
			"MINIO_ROOT_USER":     MinioUser,
			"MINIO_ROOT_PASSWORD": MinioPassword,
		},
		Cmd: []string{"server", "/data"},
	}
	minioContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("cannot create MinIO container: %s", err)
	}
	defer func() {
		err := minioContainer.Terminate(context.Background())
		if err != nil {
			t.Fatalf("cannot terminate MinIO container: %s", err)
		}
	}()

	host, err := minioContainer.Host(ctx)
	if err != nil {
		t.Fatalf("unable to determine MinIO host: %s", err)
	}
	port, err := minioContainer.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatalf("unable to determine MinIO port: %s", err)
	}
	endPoint := fmt.Sprintf("%s:%s", host, port.Port())

	minioClient, err := minio.New(endPoint, &minio.Options{
		Creds:  credentials.NewStaticV4(MinioUser, MinioPassword, ""),
		Secure: false,
	})
	if err != nil {
		t.Fatalf("cannot create MinIO client: %s", err)
	}

	testFunc(minioClient)
}
