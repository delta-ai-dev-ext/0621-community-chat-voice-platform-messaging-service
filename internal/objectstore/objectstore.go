// Package objectstore wraps the S3-compatible storage used for image
// attachments. The service only ever signs URLs and inspects object metadata;
// image bytes never pass through this process (ADR-003).
package objectstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"messaging_service/internal/config"
)

// Errors returned by every ObjectStore implementation. The service layer maps
// ErrObjectNotFound to `422 attachment_invalid` and ErrUnavailable to `503`.
var (
	// ErrObjectNotFound means the object does not exist in the bucket.
	ErrObjectNotFound = errors.New("objectstore: object not found")
	// ErrUnavailable wraps any transport, authentication or service failure.
	ErrUnavailable = errors.New("objectstore: storage unavailable")
)

// SignedURL is a presigned URL together with its exact expiry, so responses and
// events can carry `url_expires_at`.
type SignedURL struct {
	URL       string
	ExpiresAt time.Time
}

// ObjectInfo is what a HEAD request reports about a stored object.
type ObjectInfo struct {
	SizeBytes   int64
	ContentType string
}

// ObjectStore is the storage port. Everything above this layer is tested with a
// stub implementation.
type ObjectStore interface {
	// PresignPut returns a presigned PUT URL bound to the given content type.
	PresignPut(ctx context.Context, key, contentType string, ttl time.Duration) (SignedURL, error)
	// PresignGet returns a short-lived download URL.
	PresignGet(ctx context.Context, key string, ttl time.Duration) (SignedURL, error)
	// Head returns the object's real size and stored content type.
	Head(ctx context.Context, key string) (*ObjectInfo, error)
	// Delete removes a stored object; deleting an absent object is not an error.
	Delete(ctx context.Context, key string) error
	// Check verifies storage reachability for the readiness probe.
	Check(ctx context.Context) error
}

// S3ObjectStore is the aws-sdk-go-v2 implementation of ObjectStore.
type S3 struct {
	client    *s3.Client
	presigner *s3.PresignClient
	bucket    string
	clock     func() time.Time
}

// NewS3 builds the S3-compatible object store from configuration. The endpoint
// is configurable so any S3-compatible provider can be used.
func NewS3(ctx context.Context, cfg *config.Config, clock func() time.Time) (*S3, error) {
	if clock == nil {
		clock = time.Now
	}
	options := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.S3Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.S3AccessKeyID, cfg.S3SecretAccessKey, "")),
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("objectstore: loading aws config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.S3EndpointURL != "" {
			o.BaseEndpoint = aws.String(cfg.S3EndpointURL)
		}
		o.UsePathStyle = cfg.S3UsePathStyle
	})
	return &S3{client: client, presigner: s3.NewPresignClient(client), bucket: cfg.S3Bucket, clock: clock}, nil
}

// PresignPut signs a PUT that a client executes directly against storage.
func (s *S3) PresignPut(ctx context.Context, key, contentType string, ttl time.Duration) (SignedURL, error) {
	request, err := s.presigner.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return SignedURL{}, fmt.Errorf("%w: presigning upload: %v", ErrUnavailable, err)
	}
	return SignedURL{URL: request.URL, ExpiresAt: s.clock().UTC().Add(ttl)}, nil
}

// PresignGet signs a short-lived download URL regenerated on every read.
func (s *S3) PresignGet(ctx context.Context, key string, ttl time.Duration) (SignedURL, error) {
	request, err := s.presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return SignedURL{}, fmt.Errorf("%w: presigning download: %v", ErrUnavailable, err)
	}
	return SignedURL{URL: request.URL, ExpiresAt: s.clock().UTC().Add(ttl)}, nil
}

// Head reports the authoritative size and content type used by verify-on-send.
func (s *S3) Head(ctx context.Context, key string) (*ObjectInfo, error) {
	output, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, ErrObjectNotFound
		}
		return nil, fmt.Errorf("%w: head object: %v", ErrUnavailable, err)
	}
	info := &ObjectInfo{}
	if output.ContentLength != nil {
		info.SizeBytes = *output.ContentLength
	}
	if output.ContentType != nil {
		info.ContentType = *output.ContentType
	}
	return info, nil
}

// Delete removes a stored object. Missing objects are treated as already gone so
// the GC sweep and message deletion stay idempotent.
func (s *S3) Delete(ctx context.Context, key string) error {
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}); err != nil {
		if isNotFound(err) {
			return nil
		}
		return fmt.Errorf("%w: delete object: %v", ErrUnavailable, err)
	}
	return nil
}

// Check verifies bucket reachability for /readyz.
func (s *S3) Check(ctx context.Context) error {
	if _, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(s.bucket)}); err != nil {
		return fmt.Errorf("%w: head bucket: %v", ErrUnavailable, err)
	}
	return nil
}

// isNotFound reports whether an SDK error means "no such key/bucket".
func isNotFound(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "NoSuchBucket", "404":
			return true
		}
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "status code: 404") || strings.Contains(message, "statuscode: 404")
}
