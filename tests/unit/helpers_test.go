package unit

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"messaging_service/internal/eventbus"
	"messaging_service/internal/models"
	"messaging_service/internal/observability"
	"messaging_service/internal/service"
	"messaging_service/tests/mocks"
)

// base64URL mirrors the opaque cursor encoding for tampering tests.
func base64URL(raw []byte) string { return base64.RawURLEncoding.EncodeToString(raw) }

// matchContext matches any context argument in mock expectations.
func matchContext() any { return mock.Anything }

// harness wires the services against testify mocks, so service tests exercise
// real business logic with no database, broker or storage.
type harness struct {
	messages      *mocks.MessageRepository
	community     *mocks.CommunityProjectionRepository
	conversations *mocks.ConversationProjectionRepository
	attachments   *mocks.AttachmentRepository
	rates         *mocks.RateRepository
	outbox        *mocks.OutboxRepository
	tx            *mocks.TransactionManager
	objects       *mocks.ObjectStore
	now           time.Time
	deps          service.Deps
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{
		messages:      &mocks.MessageRepository{},
		community:     &mocks.CommunityProjectionRepository{},
		conversations: &mocks.ConversationProjectionRepository{},
		attachments:   &mocks.AttachmentRepository{},
		rates:         &mocks.RateRepository{},
		outbox:        &mocks.OutboxRepository{},
		tx:            &mocks.TransactionManager{},
		objects:       &mocks.ObjectStore{},
		now:           testNow,
	}
	h.deps = service.Deps{
		Messages:      h.messages,
		Community:     h.community,
		Conversations: h.conversations,
		Attachments:   h.attachments,
		Rates:         h.rates,
		Outbox:        h.outbox,
		Tx:            h.tx,
		Objects:       h.objects,
		Clock:         func() time.Time { return h.now },
		Logger:        zap.NewNop(),
		Metrics:       observability.NewMetrics(nil),
		Options: service.Options{
			MaxAttachmentBytes:   10485760,
			UploadURLTTL:         15 * time.Minute,
			DownloadURLTTL:       15 * time.Minute,
			PendingUploadGCAfter: 24 * time.Hour,
			AppealURLBase:        "https://support.local/appeals",
			PageLimitDefault:     50,
			PageLimitMax:         100,
		},
	}
	return h
}

// messagesService builds the message service on this harness.
func (h *harness) messagesService() *service.MessageService { return service.NewMessageService(h.deps) }

// attachmentService builds the attachment service on this harness.
func (h *harness) attachmentService() *service.AttachmentService {
	return service.NewAttachmentService(h.deps)
}

// scopeService builds the scope oracle on this harness.
func (h *harness) scopeService() *service.ScopeService { return service.NewScopeService(h.deps) }

// activeMembership returns a projection membership granting posting rights.
func activeMembership(communityID, userID uuid.UUID) *models.CommunityMember {
	return &models.CommunityMember{
		CommunityID: communityID, UserID: userID, Status: models.MemberStatusActive, MemberVersion: 1,
	}
}

// textChannel returns a projected text channel.
func textChannel(channelID, communityID uuid.UUID) *models.TextChannel {
	return &models.TextChannel{
		ChannelID: channelID, CommunityID: communityID, Name: "general", Position: 0, ChannelVersion: 1,
	}
}

// expectChannelAccess primes the mocks for an authorized channel request.
func (h *harness) expectChannelAccess(channelID, communityID, userID uuid.UUID, member *models.CommunityMember) {
	h.community.On("GetTextChannel", matchContext(), channelID).Return(textChannel(channelID, communityID), nil).Once()
	h.community.On("GetMembership", matchContext(), communityID, userID).Return(member, nil).Once()
}

// storedMessage builds a stored channel message for the given container.
func storedMessage(containerID uuid.UUID, content string) models.Message {
	return models.Message{
		ID: testMessageID, ContainerType: models.ContainerChannel, ContainerID: containerID,
		CommunityID: &testCommunity, AuthorUserID: testAuthorID, Content: &content,
		MessageVersion: 1, CreatedAt: testNow,
	}
}

// storedAggregate wraps a stored message into its aggregate form.
func storedAggregate(message models.Message) *models.MessageAggregate {
	return &models.MessageAggregate{
		Message:          message,
		MentionedUserIDs: []uuid.UUID{},
		Reactions:        []models.ReactionSummary{},
	}
}

// newRelayHarness builds an outbox relay over mocks.
func newRelayHarness(t *testing.T) (*eventbus.Relay, *mocks.OutboxRepository, *mocks.Publisher) {
	t.Helper()
	outbox := &mocks.OutboxRepository{}
	publisher := &mocks.Publisher{}
	relay := eventbus.NewRelay(outbox, publisher, observability.NewMetrics(nil), zap.NewNop(),
		func() time.Time { return testNow }, eventbus.RelayOptions{Interval: time.Millisecond, Batch: 10})
	return relay, outbox, publisher
}

// contextBackground is a small readability helper.
func contextBackground() context.Context { return context.Background() }

// fingerprintOf mirrors the automod duplicate-detection fingerprint so tests can
// build rate-counter states that trigger the duplicate rule.
func fingerprintOf(content string) string {
	normalised := strings.ToLower(strings.Join(strings.Fields(content), " "))
	sum := sha256.Sum256([]byte(normalised))
	return hex.EncodeToString(sum[:])
}

// jsonMarshal keeps test helpers free of repetitive error handling.
func jsonMarshal(value any) ([]byte, error) { return json.Marshal(value) }
