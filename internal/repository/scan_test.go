package repository

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUUIDParamConversion(t *testing.T) {
	t.Parallel()
	id := uuid.MustParse("11111111-1111-4111-8111-111111111111")

	param := uuidParam(id)
	assert.True(t, param.Valid)
	assert.Equal(t, [16]byte(id), param.Bytes)

	optional := uuidParamPtr(nil)
	assert.False(t, optional.Valid)

	optional = uuidParamPtr(&id)
	assert.True(t, optional.Valid)
}

func TestUUIDStringHelpers(t *testing.T) {
	t.Parallel()
	first := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	second := uuid.MustParse("22222222-2222-4222-8222-222222222222")

	encoded := uuidStrings([]uuid.UUID{first, second})
	assert.Equal(t, []string{first.String(), second.String()}, encoded)
	assert.Empty(t, uuidStrings(nil))

	parsed, err := uuidList([]string{first.String(), second.String()})
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{first, second}, parsed)
	emptyList, err := uuidList(nil)
	require.NoError(t, err)
	assert.Empty(t, emptyList)

	_, err = uuidList([]string{"not-a-uuid"})
	require.Error(t, err)

	single, err := parseUUID(first.String())
	require.NoError(t, err)
	assert.Equal(t, first, single)

	_, err = parseUUID("nope")
	require.Error(t, err)

	optionalPtr, err := parseUUIDPtr(nil)
	require.NoError(t, err)
	assert.Nil(t, optionalPtr)

	empty := ""
	optionalPtr, err = parseUUIDPtr(&empty)
	require.NoError(t, err)
	assert.Nil(t, optionalPtr)

	secondRaw := second.String()
	optionalPtr, err = parseUUIDPtr(&secondRaw)
	require.NoError(t, err)
	require.NotNil(t, optionalPtr)
	assert.Equal(t, second, *optionalPtr)

	invalid := "nope"
	_, err = parseUUIDPtr(&invalid)
	require.Error(t, err)
}
