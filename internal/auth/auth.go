// Package auth verifies access tokens locally against identity's public
// verification keys and authenticates the per-edge service token. It never calls
// identity_service at request time.
package auth

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/subtle"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// Common verification failures. Callers translate all of them into a 401.
var (
	// ErrMissingToken means no bearer token was presented.
	ErrMissingToken = errors.New("auth: bearer token is missing")
	// ErrInvalidToken means signature, issuer, audience or claims were invalid.
	ErrInvalidToken = errors.New("auth: token is invalid")
	// ErrInvalidServiceToken means the internal service token did not match.
	ErrInvalidServiceToken = errors.New("auth: service token is invalid")
)

// Principal is the authenticated caller.
type Principal struct {
	// UserID is the token's `sub` claim parsed as a uuid.
	UserID uuid.UUID
	// SessionID is the token's `sid` claim.
	SessionID string
	// EmailVerified mirrors the identity claim.
	EmailVerified bool
}

// Claims is the access-token claim set issued by identity_service.
type Claims struct {
	SessionID     string `json:"sid"`
	EmailVerified bool   `json:"email_verified"`
	jwt.RegisteredClaims
}

// Config carries the verification configuration taken from deployment config.
type Config struct {
	Issuer        string
	Audience      string
	PublicKey     string
	NextPublicKey string
}

// Verifier validates access tokens against the configured key set. Both the
// current and the next key are accepted so rotation needs no downtime.
type Verifier struct {
	issuer   string
	audience string
	keys     []crypto.PublicKey
}

// NewVerifier parses the configured verification keys (PEM or JWK).
func NewVerifier(cfg Config) (*Verifier, error) {
	if cfg.Issuer == "" || cfg.Audience == "" {
		return nil, errors.New("auth: issuer and audience are required")
	}
	keys, err := parseKeys(cfg.PublicKey)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.NextPublicKey) != "" {
		next, nextErr := parseKeys(cfg.NextPublicKey)
		if nextErr != nil {
			return nil, nextErr
		}
		keys = append(keys, next...)
	}
	if len(keys) == 0 {
		return nil, errors.New("auth: at least one verification key is required")
	}
	return &Verifier{issuer: cfg.Issuer, audience: cfg.Audience, keys: keys}, nil
}

// Verify parses and validates an access token, returning the principal.
func (v *Verifier) Verify(token string) (Principal, error) {
	claims := &Claims{}
	parsed, err := jwt.ParseWithClaims(token, claims, func(parsed *jwt.Token) (any, error) {
		// Every configured key that can verify this signing method is offered so
		// key rotation works without a `kid` header.
		candidates := make([]jwt.VerificationKey, 0, len(v.keys))
		for _, key := range v.keys {
			if keyMatchesMethod(key, parsed.Method) {
				candidates = append(candidates, key)
			}
		}
		switch len(candidates) {
		case 0:
			return nil, fmt.Errorf("%w: no key matches signing method %s", ErrInvalidToken, parsed.Method.Alg())
		case 1:
			return candidates[0], nil
		default:
			return jwt.VerificationKeySet{Keys: candidates}, nil
		}
	},
		jwt.WithIssuer(v.issuer),
		jwt.WithAudience(v.audience),
		jwt.WithValidMethods([]string{"RS256", "RS384", "RS512", "ES256", "EdDSA"}),
		jwt.WithExpirationRequired(),
	)
	if err != nil || !parsed.Valid {
		return Principal{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	userID, err := uuid.Parse(claims.Subject)
	if err != nil || userID == uuid.Nil {
		return Principal{}, fmt.Errorf("%w: sub must be a user id", ErrInvalidToken)
	}
	return Principal{UserID: userID, SessionID: claims.SessionID, EmailVerified: claims.EmailVerified}, nil
}

// VerifyServiceToken compares the presented bearer token with the expected
// service token in constant time.
func VerifyServiceToken(presented, expected string) error {
	if expected == "" {
		return ErrInvalidServiceToken
	}
	if subtle.ConstantTimeCompare([]byte(presented), []byte(expected)) != 1 {
		return ErrInvalidServiceToken
	}
	return nil
}

// BearerToken extracts the token from an `Authorization: Bearer <token>` header.
func BearerToken(header string) (string, error) {
	const prefix = "Bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", ErrMissingToken
	}
	token := strings.TrimSpace(header[len(prefix):])
	if token == "" {
		return "", ErrMissingToken
	}
	return token, nil
}

// keyMatchesMethod rejects a key that cannot verify the token's algorithm, which
// keeps algorithm confusion (for example HS256 with a public key) impossible.
func keyMatchesMethod(key crypto.PublicKey, method jwt.SigningMethod) bool {
	switch method.Alg() {
	case "EdDSA":
		_, ok := key.(ed25519.PublicKey)
		return ok
	case "ES256":
		_, ok := key.(*ecdsa.PublicKey)
		return ok
	case "RS256", "RS384", "RS512":
		_, ok := key.(*rsa.PublicKey)
		return ok
	default:
		return false
	}
}

// parseKeys parses one configured key, accepting a PEM block, a single JWK or a
// JWK set.
func parseKeys(raw string) ([]crypto.PublicKey, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, errors.New("auth: public key is empty")
	}
	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		return parseJWK(trimmed)
	}
	block, _ := pem.Decode([]byte(trimmed))
	if block == nil {
		return nil, errors.New("auth: public key is neither PEM nor JWK material")
	}
	if key, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		return []crypto.PublicKey{key}, nil
	}
	if key, err := x509.ParsePKCS1PublicKey(block.Bytes); err == nil {
		return []crypto.PublicKey{key}, nil
	}
	if cert, err := x509.ParseCertificate(block.Bytes); err == nil {
		return []crypto.PublicKey{cert.PublicKey}, nil
	}
	return nil, errors.New("auth: unsupported PEM public key")
}

// jwk is the subset of RFC 7517 needed for RSA, EC and Ed25519 keys.
type jwk struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	N   string `json:"n"`
	E   string `json:"e"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

type jwkSet struct {
	Keys []jwk `json:"keys"`
}

func parseJWK(raw string) ([]crypto.PublicKey, error) {
	var (
		single jwk
		set    jwkSet
	)
	if strings.HasPrefix(strings.TrimSpace(raw), "[") {
		var keys []jwk
		if err := json.Unmarshal([]byte(raw), &keys); err != nil {
			return nil, fmt.Errorf("auth: parsing JWK array: %w", err)
		}
		return buildJWKs(keys)
	}
	if err := json.Unmarshal([]byte(raw), &set); err == nil && len(set.Keys) > 0 {
		return buildJWKs(set.Keys)
	}
	if err := json.Unmarshal([]byte(raw), &single); err != nil {
		return nil, fmt.Errorf("auth: parsing JWK: %w", err)
	}
	return buildJWKs([]jwk{single})
}

func buildJWKs(keys []jwk) ([]crypto.PublicKey, error) {
	parsed := make([]crypto.PublicKey, 0, len(keys))
	for _, key := range keys {
		publicKey, err := buildJWK(key)
		if err != nil {
			return nil, err
		}
		parsed = append(parsed, publicKey)
	}
	if len(parsed) == 0 {
		return nil, errors.New("auth: JWK material contained no keys")
	}
	return parsed, nil
}

func buildJWK(key jwk) (crypto.PublicKey, error) {
	switch key.Kty {
	case "RSA":
		modulus, err := base64.RawURLEncoding.DecodeString(key.N)
		if err != nil {
			return nil, fmt.Errorf("auth: decoding JWK modulus: %w", err)
		}
		exponent, err := base64.RawURLEncoding.DecodeString(key.E)
		if err != nil {
			return nil, fmt.Errorf("auth: decoding JWK exponent: %w", err)
		}
		return &rsa.PublicKey{N: new(big.Int).SetBytes(modulus), E: int(new(big.Int).SetBytes(exponent).Int64())}, nil
	case "EC":
		curve := elliptic.P256()
		if key.Crv != "" && key.Crv != "P-256" {
			return nil, fmt.Errorf("auth: unsupported JWK curve %q", key.Crv)
		}
		xBytes, err := base64.RawURLEncoding.DecodeString(key.X)
		if err != nil {
			return nil, fmt.Errorf("auth: decoding JWK x: %w", err)
		}
		yBytes, err := base64.RawURLEncoding.DecodeString(key.Y)
		if err != nil {
			return nil, fmt.Errorf("auth: decoding JWK y: %w", err)
		}
		return &ecdsa.PublicKey{Curve: curve, X: new(big.Int).SetBytes(xBytes), Y: new(big.Int).SetBytes(yBytes)}, nil
	case "OKP":
		if key.Crv != "" && key.Crv != "Ed25519" {
			return nil, fmt.Errorf("auth: unsupported JWK curve %q", key.Crv)
		}
		raw, err := base64.RawURLEncoding.DecodeString(key.X)
		if err != nil {
			return nil, fmt.Errorf("auth: decoding JWK x: %w", err)
		}
		if len(raw) != ed25519.PublicKeySize {
			return nil, errors.New("auth: invalid Ed25519 key length")
		}
		return ed25519.PublicKey(raw), nil
	default:
		return nil, fmt.Errorf("auth: unsupported JWK key type %q", key.Kty)
	}
}
