package unit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"messaging_service/internal/config"
)

// setMinimalEnv primes the process environment with a working configuration.
func setMinimalEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://messaging:messaging@localhost:5432/messaging")
	t.Setenv("KAFKA_BROKERS", "localhost:9092")
	t.Setenv("S3_ENDPOINT_URL", "http://localhost:9000")
	t.Setenv("S3_BUCKET", "attachments")
	t.Setenv("IDENTITY_ISSUER", "https://identity.internal")
	t.Setenv("IDENTITY_AUDIENCE", "messaging")
	t.Setenv("IDENTITY_PUBLIC_KEY", "-----BEGIN PUBLIC KEY-----")
	t.Setenv("INTERNAL_SERVICE_TOKEN", "edge-token")
}

func TestLoadFromEnvOverridesDefaults(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("PORT", "9090")
	t.Setenv("SERVICE_NAME", "messaging_service")
	t.Setenv("UPLOAD_URL_TTL", "5m")
	t.Setenv("DOWNLOAD_URL_TTL", "10m")
	t.Setenv("GC_PENDING_AFTER", "48h")
	t.Setenv("SHUTDOWN_TIMEOUT", "5s")
	t.Setenv("PAGE_LIMIT_DEFAULT", "20")
	t.Setenv("PAGE_LIMIT_MAX", "60")
	t.Setenv("ATTACHMENT_MAX_BYTES", "2048")
	t.Setenv("S3_USE_PATH_STYLE", "true")
	t.Setenv("APPEAL_URL_BASE", "https://support.local/appeals")
	t.Setenv("OUTBOX_RELAY_INTERVAL", "250ms")
	t.Setenv("GC_INTERVAL", "5m")
	t.Setenv("KAFKA_SASL_USERNAME", "user")
	t.Setenv("KAFKA_SASL_PASSWORD", "pass")

	cfg, err := config.LoadFromEnv()
	require.NoError(t, err)
	assert.Equal(t, "9090", cfg.Port)
	assert.Equal(t, "messaging_service", cfg.ServiceName)
	assert.Equal(t, 5*time.Minute, cfg.UploadURLTTL)
	assert.Equal(t, 10*time.Minute, cfg.DownloadURLTTL)
	assert.Equal(t, 48*time.Hour, cfg.PendingUploadGCAfter)
	assert.Equal(t, 5*time.Second, cfg.ShutdownTimeout)
	assert.Equal(t, 20, cfg.PageLimitDefault)
	assert.Equal(t, 60, cfg.PageLimitMax)
	assert.Equal(t, int64(2048), cfg.AttachmentMaxBytes)
	assert.True(t, cfg.S3UsePathStyle)
	assert.Equal(t, "https://support.local/appeals", cfg.AppealURLBase)
	assert.Equal(t, 250*time.Millisecond, cfg.OutboxRelayInterval)
	assert.Equal(t, 5*time.Minute, cfg.GCInterval)
	assert.Equal(t, "user", cfg.KafkaSASLUsername)
	assert.Equal(t, "pass", cfg.KafkaSASLPassword)
	require.NoError(t, cfg.Validate())
}

func TestValidateReportsEveryProblem(t *testing.T) {
	t.Parallel()
	broken := &config.Config{
		Port:                 "not-a-number",
		KafkaConsumerGroup:   "",
		IdentityPublicKey:    "",
		IdentityIssuer:       "",
		InternalServiceToken: "",
		AttachmentMaxBytes:   0,
		PageLimitDefault:     0,
		PageLimitMax:         0,
	}
	err := broken.Validate()
	require.Error(t, err)
	for _, fragment := range []string{"DATABASE_URL", "KAFKA_BROKERS", "S3_BUCKET", "INTERNAL_SERVICE_TOKEN"} {
		assert.Contains(t, err.Error(), fragment)
	}
}

func TestValidateAcceptsNormalisedConfig(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{
		Port:                 "8080",
		DatabaseURL:          "postgres://localhost:5432/messaging",
		KafkaBrokers:         []string{"localhost:9092"},
		KafkaConsumerGroup:   "messaging_service",
		S3Bucket:             "attachments",
		S3EndpointURL:        "http://localhost:9000",
		IdentityIssuer:       "https://identity.internal",
		IdentityAudience:     "messaging",
		IdentityPublicKey:    "-----BEGIN PUBLIC KEY-----",
		InternalServiceToken: "token",
		AttachmentMaxBytes:   10485760,
		PageLimitDefault:     50,
		PageLimitMax:         100,
		UploadURLTTL:         15 * time.Minute,
		DownloadURLTTL:       15 * time.Minute,
		PendingUploadGCAfter: 24 * time.Hour,
		ShutdownTimeout:      20 * time.Second,
	}
	require.NoError(t, cfg.Validate())
}
