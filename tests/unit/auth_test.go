package unit

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"messaging_service/internal/auth"
)

const (
	authIssuer   = "https://identity.internal"
	authAudience = "messaging"
)

// keyMaterial holds a generated RSA key and its PEM encoding.
type keyMaterial struct {
	private *rsa.PrivateKey
	pem     string
}

var rsaKeys = mustRSAKeys()

func mustRSAKeys() keyMaterial {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		panic(err)
	}
	return keyMaterial{private: key, pem: string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))}
}

func rsaJWK(key *rsa.PublicKey) map[string]string {
	return map[string]string{
		"kty": "RSA",
		"n":   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}
}

// signRS256 mints an access token with the fixture key.
func signRS256(t *testing.T, mutate func(jwt.MapClaims)) string {
	t.Helper()
	claims := jwt.MapClaims{
		"sub":            uuid.New().String(),
		"sid":            "session-1",
		"email_verified": true,
		"iss":            authIssuer,
		"aud":            []string{authAudience},
		"exp":            time.Now().Add(time.Hour).Unix(),
		"iat":            time.Now().Add(-time.Minute).Unix(),
	}
	if mutate != nil {
		mutate(claims)
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signed, err := token.SignedString(rsaKeys.private)
	require.NoError(t, err)
	return signed
}

func newRSVerifier(t *testing.T) *auth.Verifier {
	t.Helper()
	verifier, err := auth.NewVerifier(auth.Config{
		Issuer: authIssuer, Audience: authAudience, PublicKey: rsaKeys.pem,
	})
	require.NoError(t, err)
	return verifier
}

func TestVerifierAcceptsValidTokens(t *testing.T) {
	t.Parallel()
	verifier := newRSVerifier(t)
	token := signRS256(t, nil)

	principal, err := verifier.Verify(token)
	require.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, principal.UserID)
	assert.Equal(t, "session-1", principal.SessionID)
	assert.True(t, principal.EmailVerified)
}

func TestVerifierRejectsInvalidTokens(t *testing.T) {
	t.Parallel()
	verifier := newRSVerifier(t)

	cases := map[string]string{
		"expired":            signRS256(t, func(c jwt.MapClaims) { c["exp"] = time.Now().Add(-time.Hour).Unix() }),
		"wrong issuer":       signRS256(t, func(c jwt.MapClaims) { c["iss"] = "https://evil.internal" }),
		"wrong audience":     signRS256(t, func(c jwt.MapClaims) { c["aud"] = []string{"other"} }),
		"subject not a uuid": signRS256(t, func(c jwt.MapClaims) { c["sub"] = "not-a-uuid" }),
		"missing expiry":     signRS256(t, func(c jwt.MapClaims) { delete(c, "exp") }),
		"garbage":            "not-a-jwt",
	}
	for name, token := range cases {
		token := token
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := verifier.Verify(token)
			require.Error(t, err)
			assert.True(t, errors.Is(err, auth.ErrInvalidToken), "got %v", err)
		})
	}
}

func TestVerifierRejectsAlgorithmConfusion(t *testing.T) {
	t.Parallel()
	verifier := newRSVerifier(t)
	// HS256 signed with the public key bytes must not verify: the verifier only
	// offers RSA keys to RSA signing methods.
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": uuid.New().String(), "iss": authIssuer, "aud": []string{authAudience},
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	signed, err := token.SignedString([]byte(rsaKeys.pem))
	require.NoError(t, err)

	_, err = verifier.Verify(signed)
	require.Error(t, err)
	assert.True(t, errors.Is(err, auth.ErrInvalidToken))
}

func TestVerifierAcceptsJWKMaterial(t *testing.T) {
	t.Parallel()

	encode := func(value any) string {
		body, err := jsonMarshal(value)
		require.NoError(t, err)
		return string(body)
	}

	single := encode(rsaJWK(&rsaKeys.private.PublicKey))
	set := encode(map[string]any{"keys": []any{rsaJWK(&rsaKeys.private.PublicKey)}})
	array := encode([]any{rsaJWK(&rsaKeys.private.PublicKey)})

	for name, material := range map[string]string{"single": single, "set": set, "array": array} {
		material := material
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			verifier, err := auth.NewVerifier(auth.Config{
				Issuer: authIssuer, Audience: authAudience, PublicKey: material,
			})
			require.NoError(t, err)
			_, err = verifier.Verify(signRS256(t, nil))
			require.NoError(t, err)
		})
	}
}

func TestVerifierAcceptsEd25519AndECKeys(t *testing.T) {
	t.Parallel()

	t.Run("ed25519", func(t *testing.T) {
		t.Parallel()
		public, private, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		jwkMaterial := map[string]string{
			"kty": "OKP", "crv": "Ed25519",
			"x": base64.RawURLEncoding.EncodeToString(public),
		}
		body, err := jsonMarshal(jwkMaterial)
		require.NoError(t, err)
		verifier, err := auth.NewVerifier(auth.Config{
			Issuer: authIssuer, Audience: authAudience, PublicKey: string(body),
		})
		require.NoError(t, err)

		token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{
			"sub": uuid.New().String(), "iss": authIssuer, "aud": []string{authAudience},
			"exp": time.Now().Add(time.Hour).Unix(),
		})
		signed, err := token.SignedString(private)
		require.NoError(t, err)
		_, err = verifier.Verify(signed)
		require.NoError(t, err)
	})

	t.Run("ecdsa p-256", func(t *testing.T) {
		t.Parallel()
		private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		jwkMaterial := map[string]string{
			"kty": "EC", "crv": "P-256",
			"x": base64.RawURLEncoding.EncodeToString(private.X.Bytes()),
			"y": base64.RawURLEncoding.EncodeToString(private.Y.Bytes()),
		}
		body, err := jsonMarshal(jwkMaterial)
		require.NoError(t, err)
		verifier, err := auth.NewVerifier(auth.Config{
			Issuer: authIssuer, Audience: authAudience, PublicKey: string(body),
		})
		require.NoError(t, err)

		token := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
			"sub": uuid.New().String(), "iss": authIssuer, "aud": []string{authAudience},
			"exp": time.Now().Add(time.Hour).Unix(),
		})
		signed, err := token.SignedString(private)
		require.NoError(t, err)
		_, err = verifier.Verify(signed)
		require.NoError(t, err)
	})
}

func TestNewVerifierRejectsBadConfiguration(t *testing.T) {
	t.Parallel()

	cases := map[string]auth.Config{
		"missing issuer":   {Audience: authAudience, PublicKey: rsaKeys.pem},
		"missing audience": {Issuer: authIssuer, PublicKey: rsaKeys.pem},
		"empty key":        {Issuer: authIssuer, Audience: authAudience},
		"bad pem":          {Issuer: authIssuer, Audience: authAudience, PublicKey: "not-a-key"},
		"bad jwk type": {Issuer: authIssuer, Audience: authAudience,
			PublicKey: `{"kty":"oct","k":"abcd"}`},
		"bad jwk json": {Issuer: authIssuer, Audience: authAudience, PublicKey: `{"keys":`},
		"bad next key": {Issuer: authIssuer, Audience: authAudience, PublicKey: rsaKeys.pem,
			NextPublicKey: "not-a-key"},
		"empty jwk set": {Issuer: authIssuer, Audience: authAudience, PublicKey: `{"keys":[]}`},
		"unsupported ec curve": {Issuer: authIssuer, Audience: authAudience,
			PublicKey: `{"kty":"EC","crv":"P-521","x":"ab","y":"cd"}`},
		"bad ed25519 length": {Issuer: authIssuer, Audience: authAudience,
			PublicKey: `{"kty":"OKP","crv":"Ed25519","x":"abcd"}`},
	}
	for name, cfg := range cases {
		cfg := cfg
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := auth.NewVerifier(cfg)
			require.Error(t, err)
		})
	}
}

func TestVerifierAcceptsRotatingNextKey(t *testing.T) {
	t.Parallel()
	next, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der, err := x509.MarshalPKIXPublicKey(&next.PublicKey)
	require.NoError(t, err)
	nextPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))

	verifier, err := auth.NewVerifier(auth.Config{
		Issuer: authIssuer, Audience: authAudience, PublicKey: rsaKeys.pem, NextPublicKey: nextPEM,
	})
	require.NoError(t, err)

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"sub": uuid.New().String(), "iss": authIssuer, "aud": []string{authAudience},
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	signed, err := token.SignedString(next)
	require.NoError(t, err)
	_, err = verifier.Verify(signed)
	require.NoError(t, err)
}

func TestServiceTokenAndBearerParsing(t *testing.T) {
	t.Parallel()

	require.NoError(t, auth.VerifyServiceToken("secret", "secret"))
	require.ErrorIs(t, auth.VerifyServiceToken("other", "secret"), auth.ErrInvalidServiceToken)
	require.ErrorIs(t, auth.VerifyServiceToken("secret", ""), auth.ErrInvalidServiceToken)
	require.ErrorIs(t, auth.VerifyServiceToken("", "secret"), auth.ErrInvalidServiceToken)

	token, err := auth.BearerToken("Bearer abc")
	require.NoError(t, err)
	assert.Equal(t, "abc", token)

	token, err = auth.BearerToken("bearer abc")
	require.NoError(t, err)
	assert.Equal(t, "abc", token)

	for _, header := range []string{"", "abc", "Bearer", "Bearer   ", "Basic abc"} {
		_, err := auth.BearerToken(header)
		require.ErrorIs(t, err, auth.ErrMissingToken)
	}
}
