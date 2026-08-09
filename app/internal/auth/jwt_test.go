package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJWTSignAndVerify(t *testing.T) {
	mgr := NewJWTManager("test-secret")
	token, err := mgr.Sign("6f4b7fca-24f8-4233-bf7a-0f56737a847c", "alice", "buyer")
	require.NoError(t, err)
	assert.NotEmpty(t, token)

	claims, err := mgr.Verify(token)
	require.NoError(t, err)
	assert.Equal(t, "6f4b7fca-24f8-4233-bf7a-0f56737a847c", claims.ID)
	assert.Equal(t, "alice", claims.Name)
	assert.Equal(t, "buyer", claims.Role)
}

func TestJWTVerifyInvalidToken(t *testing.T) {
	mgr := NewJWTManager("test-secret")
	_, err := mgr.Verify("invalid-token-string")
	assert.Error(t, err)
}

func TestJWTVerifyWrongSecret(t *testing.T) {
	mgr1 := NewJWTManager("secret-one")
	mgr2 := NewJWTManager("secret-two")
	token, err := mgr1.Sign("6f4b7fca-24f8-4233-bf7a-0f56737a847c", "alice", "buyer")
	require.NoError(t, err)

	_, err = mgr2.Verify(token)
	assert.Error(t, err)
}
