package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Default values applied when the corresponding environment variable is unset.
const (
	// DefaultPort is the HTTP listen port when PORT is unset.
	DefaultPort = "8080"
	// DefaultAttachmentMaxBytes is the 10 MB attachment ceiling.
	DefaultAttachmentMaxBytes int64 = 10485760
	// DefaultUploadURLTTL is the presigned PUT lifetime.
	DefaultUploadURLTTL = 15 * time.Minute
	// DefaultDownloadURLTTL is the presigned GET lifetime.
	DefaultDownloadURLTTL = 15 * time.Minute
	// DefaultPendingUploadGCAfter is the age after which a never-attached
	// upload is garbage collected.
	DefaultPendingUploadGCAfter = 24 * time.Hour
	// DefaultShutdownTimeout bounds the graceful drain.
	DefaultShutdownTimeout = 20 * time.Second
	// DefaultOutboxRelayInterval is the outbox relay poll interval.
	DefaultOutboxRelayInterval = 500 * time.Millisecond
	// DefaultGCInterval is the attachment GC sweep interval.
	DefaultGCInterval = 15 * time.Minute
	// DefaultAppealURLBase is where muted members are pointed for appeals.
	DefaultAppealURLBase = "https://support.platform.local/appeals"
	// DefaultConsumerGroup is the Kafka consumer group used for projections.
	DefaultConsumerGroup = "messaging_service.projections"
	// DefaultPageLimit and DefaultPageLimitMax mirror the OpenAPI Limit param.
	DefaultPageLimit    = 50
	DefaultPageLimitMax = 100
)

// Config is the typed, validated runtime configuration. It is built once at
// startup and injected; nothing reads the environment after that.
type Config struct {
	// ServiceName is the structured-logging service identifier.
	ServiceName string
	// Port is the HTTP listen port.
	Port string
	// DatabaseURL is the PostgreSQL connection string.
	DatabaseURL string
	// KafkaBrokers is the broker list.
	KafkaBrokers []string
	// KafkaConsumerGroup is the projection consumer group.
	KafkaConsumerGroup string
	// KafkaSASLUsername/Password enable SASL/PLAIN when set.
	KafkaSASLUsername string
	KafkaSASLPassword string

	// S3EndpointURL points at any S3-compatible endpoint.
	S3EndpointURL     string
	S3Region          string
	S3Bucket          string
	S3AccessKeyID     string
	S3SecretAccessKey string
	// S3UsePathStyle selects path-style addressing (non-AWS endpoints).
	S3UsePathStyle bool

	// IdentityIssuer and IdentityAudience validate access-token claims.
	IdentityIssuer   string
	IdentityAudience string
	// IdentityPublicKey is the current verification key (PEM or JWK).
	IdentityPublicKey string
	// IdentityNextPublicKey is the next key during rotation; may be empty.
	IdentityNextPublicKey string

	// InternalServiceToken is the expected bearer token for /internal/*.
	InternalServiceToken string

	// AttachmentMaxBytes is the upload size ceiling.
	AttachmentMaxBytes int64
	// UploadURLTTL and DownloadURLTTL are the presign lifetimes.
	UploadURLTTL   time.Duration
	DownloadURLTTL time.Duration
	// PendingUploadGCAfter is the never-attached upload age threshold.
	PendingUploadGCAfter time.Duration
	// ShutdownTimeout bounds the graceful drain.
	ShutdownTimeout time.Duration
	// OutboxRelayInterval is the relay poll interval.
	OutboxRelayInterval time.Duration
	// GCInterval is the attachment GC sweep interval.
	GCInterval time.Duration
	// AppealURLBase is the appeal pointer handed to muted members.
	AppealURLBase string
	// PageLimitDefault and PageLimitMax bound history page sizes.
	PageLimitDefault int
	PageLimitMax     int
}

// Load reads configuration through getenv, applies defaults and validates the
// result. It returns a descriptive error listing every missing requirement so
// startup fails fast with one structured log entry.
func Load(getenv func(string) string) (*Config, error) {
	if getenv == nil {
		return nil, errors.New("config: getenv function is required")
	}
	cfg := &Config{
		ServiceName:           "messaging_service",
		Port:                  firstNonEmpty(getenv("PORT"), DefaultPort),
		DatabaseURL:           strings.TrimSpace(getenv("DATABASE_URL")),
		KafkaBrokers:          splitList(getenv("KAFKA_BROKERS")),
		KafkaConsumerGroup:    firstNonEmpty(getenv("KAFKA_CONSUMER_GROUP"), DefaultConsumerGroup),
		KafkaSASLUsername:     getenv("KAFKA_SASL_USERNAME"),
		KafkaSASLPassword:     getenv("KAFKA_SASL_PASSWORD"),
		S3EndpointURL:         strings.TrimSpace(getenv("S3_ENDPOINT_URL")),
		S3Region:              firstNonEmpty(getenv("S3_REGION"), "us-east-1"),
		S3Bucket:              strings.TrimSpace(getenv("S3_BUCKET")),
		S3AccessKeyID:         getenv("S3_ACCESS_KEY_ID"),
		S3SecretAccessKey:     getenv("S3_SECRET_ACCESS_KEY"),
		S3UsePathStyle:        boolValue(getenv("S3_USE_PATH_STYLE"), true),
		IdentityIssuer:        strings.TrimSpace(getenv("IDENTITY_ISSUER")),
		IdentityAudience:      strings.TrimSpace(getenv("IDENTITY_AUDIENCE")),
		IdentityPublicKey:     getenv("IDENTITY_PUBLIC_KEY"),
		IdentityNextPublicKey: getenv("IDENTITY_NEXT_PUBLIC_KEY"),
		InternalServiceToken:  strings.TrimSpace(getenv("INTERNAL_SERVICE_TOKEN")),
		AppealURLBase:         firstNonEmpty(getenv("APPEAL_URL_BASE"), DefaultAppealURLBase),
		PageLimitDefault:      DefaultPageLimit,
		PageLimitMax:          DefaultPageLimitMax,
	}

	var err error
	if cfg.AttachmentMaxBytes, err = int64Value(getenv("ATTACHMENT_MAX_BYTES"), DefaultAttachmentMaxBytes); err != nil {
		return nil, err
	}
	if cfg.UploadURLTTL, err = durationValue(getenv("UPLOAD_URL_TTL"), DefaultUploadURLTTL); err != nil {
		return nil, err
	}
	if cfg.DownloadURLTTL, err = durationValue(getenv("DOWNLOAD_URL_TTL"), DefaultDownloadURLTTL); err != nil {
		return nil, err
	}
	if cfg.PendingUploadGCAfter, err = durationValue(getenv("GC_PENDING_AFTER"), DefaultPendingUploadGCAfter); err != nil {
		return nil, err
	}
	if cfg.ShutdownTimeout, err = durationValue(getenv("SHUTDOWN_TIMEOUT"), DefaultShutdownTimeout); err != nil {
		return nil, err
	}
	if cfg.OutboxRelayInterval, err = durationValue(getenv("OUTBOX_RELAY_INTERVAL"), DefaultOutboxRelayInterval); err != nil {
		return nil, err
	}
	if cfg.GCInterval, err = durationValue(getenv("GC_INTERVAL"), DefaultGCInterval); err != nil {
		return nil, err
	}
	if cfg.PageLimitDefault, err = intValue(getenv("PAGE_LIMIT_DEFAULT"), DefaultPageLimit); err != nil {
		return nil, err
	}
	if cfg.PageLimitMax, err = intValue(getenv("PAGE_LIMIT_MAX"), DefaultPageLimitMax); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// LoadFromEnv loads configuration from the process environment.
func LoadFromEnv() (*Config, error) { return Load(os.Getenv) }

// Validate reports the first invalid or missing setting.
func (c *Config) Validate() error {
	var problems []string
	if c.Port == "" {
		problems = append(problems, "PORT must not be empty")
	}
	if c.DatabaseURL == "" {
		problems = append(problems, "DATABASE_URL is required")
	}
	if len(c.KafkaBrokers) == 0 {
		problems = append(problems, "KAFKA_BROKERS is required")
	}
	if c.S3Bucket == "" {
		problems = append(problems, "S3_BUCKET is required")
	}
	if c.S3EndpointURL == "" {
		problems = append(problems, "S3_ENDPOINT_URL is required")
	}
	if c.IdentityIssuer == "" {
		problems = append(problems, "IDENTITY_ISSUER is required")
	}
	if c.IdentityAudience == "" {
		problems = append(problems, "IDENTITY_AUDIENCE is required")
	}
	if strings.TrimSpace(c.IdentityPublicKey) == "" {
		problems = append(problems, "IDENTITY_PUBLIC_KEY is required")
	}
	if c.InternalServiceToken == "" {
		problems = append(problems, "INTERNAL_SERVICE_TOKEN is required")
	}
	if c.AttachmentMaxBytes < 1 {
		problems = append(problems, "ATTACHMENT_MAX_BYTES must be >= 1")
	}
	if c.UploadURLTTL <= 0 || c.DownloadURLTTL <= 0 {
		problems = append(problems, "presign TTLs must be positive")
	}
	if c.PendingUploadGCAfter <= 0 {
		problems = append(problems, "GC_PENDING_AFTER must be positive")
	}
	if c.ShutdownTimeout <= 0 {
		problems = append(problems, "SHUTDOWN_TIMEOUT must be positive")
	}
	if c.PageLimitDefault < 1 || c.PageLimitDefault > c.PageLimitMax {
		problems = append(problems, "PAGE_LIMIT_DEFAULT must be within 1..PAGE_LIMIT_MAX")
	}
	if len(problems) > 0 {
		return fmt.Errorf("config: %s", strings.Join(problems, "; "))
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func splitList(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func durationValue(raw string, fallback time.Duration) (time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("config: invalid duration %q: %w", raw, err)
	}
	return parsed, nil
}

func int64Value(raw string, fallback int64) (int64, error) {
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("config: invalid integer %q: %w", raw, err)
	}
	return parsed, nil
}

func intValue(raw string, fallback int) (int, error) {
	parsed, err := int64Value(raw, int64(fallback))
	if err != nil {
		return 0, err
	}
	return int(parsed), nil
}

func boolValue(raw string, fallback bool) bool {
	if strings.TrimSpace(raw) == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return fallback
	}
	return parsed
}
