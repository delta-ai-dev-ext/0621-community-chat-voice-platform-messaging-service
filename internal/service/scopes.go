package service

import (
	"context"

	"github.com/google/uuid"

	"messaging_service/internal/models"
)

// ScopeService answers API-003 for the realtime gateway from local projections.
type ScopeService struct {
	deps Deps
}

// NewScopeService builds the scope oracle.
func NewScopeService(deps Deps) *ScopeService { return &ScopeService{deps: deps} }

// UserScopes returns the communities where the user is an active member (muted
// included, removed and banned excluded) and the conversations where the user is
// a current participant. An unknown user yields empty lists, never an error.
func (s *ScopeService) UserScopes(ctx context.Context, userID uuid.UUID) (*models.UserScopesResponse, error) {
	communities, err := s.deps.Community.ActiveCommunityIDs(ctx, userID)
	if err != nil {
		return nil, err
	}
	conversations, err := s.deps.Conversations.ConversationIDsForUser(ctx, userID)
	if err != nil {
		return nil, err
	}
	if communities == nil {
		communities = []uuid.UUID{}
	}
	if conversations == nil {
		conversations = []uuid.UUID{}
	}
	return &models.UserScopesResponse{
		UserID:          userID,
		CommunityIDs:    communities,
		ConversationIDs: conversations,
		GeneratedAt:     s.deps.now(),
	}, nil
}
