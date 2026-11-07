package objectstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"messaging_service/internal/config"
)

func testConfig() *config.Config {
	return &config.Config{
		S3EndpointURL:     "http://127.0.0.1:9000",
		S3Region:          "us-east-1",
		S3Bucket:          "attachments",
		S3AccessKeyID:     "access",
		S3SecretAccessKey: "secret",
		S3UsePathStyle:    true,
	}
}

func fixedClock() time.Time { return time.Date(2026, 1, 14, 12, 0, 0, 0, time.UTC) }

func TestNewS3SignsURLs(t *testing.T) {
	t.Parallel()
	store, err := NewS3(context.Background(), testConfig(), fixedClock)
	require.NoError(t, err)
	require.NotNil(t, store)

	upload, err := store.PresignPut(context.Background(), "attachments/key.png", "image/png", 15*time.Minute)
	require.NoError(t, err)
	assert.Contains(t, upload.URL, "attachments/key.png")
	assert.Contains(t, upload.URL, "X-Amz-Signature")
	assert.Equal(t, fixedClock().Add(15*time.Minute), upload.ExpiresAt)

	download, err := store.PresignGet(context.Background(), "attachments/key.png", 5*time.Minute)
	require.NoError(t, err)
	assert.Contains(t, download.URL, "X-Amz-Signature")
	assert.Equal(t, fixedClock().Add(5*time.Minute), download.ExpiresAt)
}

func TestNewS3DefaultsClock(t *testing.T) {
	t.Parallel()
	store, err := NewS3(context.Background(), testConfig(), nil)
	require.NoError(t, err)
	signed, err := store.PresignGet(context.Background(), "key.png", time.Minute)
	require.NoError(t, err)
	assert.WithinDuration(t, time.Now().Add(time.Minute), signed.ExpiresAt, 5*time.Second)
}

func TestStorageUnavailableIsWrapped(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	// Nothing listens on port 1, so every network call fails fast.
	cfg.S3EndpointURL = "http://127.0.0.1:1"
	store, err := NewS3(context.Background(), cfg, fixedClock)
	require.NoError(t, err)

	_, err = store.Head(context.Background(), "attachments/key.png")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnavailable), "got %v", err)

	err = store.Delete(context.Background(), "attachments/key.png")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnavailable), "got %v", err)

	err = store.Check(context.Background())
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnavailable), "got %v", err)
}

func TestIsNotFound(t *testing.T) {
	t.Parallel()

	assert.True(t, isNotFound(&smithy.GenericAPIError{Code: "NotFound"}))
	assert.True(t, isNotFound(&smithy.GenericAPIError{Code: "NoSuchKey"}))
	assert.True(t, isNotFound(&smithy.GenericAPIError{Code: "NoSuchBucket"}))
	assert.True(t, isNotFound(errors.New("operation error S3: HeadObject, https response error StatusCode: 404")))
	assert.False(t, isNotFound(&smithy.GenericAPIError{Code: "AccessDenied"}))
	assert.False(t, isNotFound(errors.New("connection refused")))
}
