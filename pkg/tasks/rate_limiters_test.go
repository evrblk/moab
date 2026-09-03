package tasks

import (
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/moab/pkg/corepb"
)

func TestRateLimitersTable_GetOrDefault(t *testing.T) {
	t.Run("returns existing state", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newRateLimitersTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		state := &corepb.RateLimiterState{Tokens: 5, LastRefilledAt: 1000}

		txn := badgerStore.Update()
		require.NoError(t, table.Set(txn, accountId, queueId, state))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		rateLimiting := &corepb.TokenBucketRateLimiting{MaxTokens: 10}
		retrieved, err := table.GetOrDefault(txn, accountId, queueId, rateLimiting, 2000)
		require.NoError(t, err)
		require.EqualValues(t, 5, retrieved.Tokens)
		require.EqualValues(t, 1000, retrieved.LastRefilledAt)
	})

	t.Run("returns a full bucket for a queue that has never dequeued", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newRateLimitersTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		txn := badgerStore.View()
		defer txn.Discard()

		rateLimiting := &corepb.TokenBucketRateLimiting{MaxTokens: 10}
		retrieved, err := table.GetOrDefault(txn, accountId, queueId, rateLimiting, 2000)
		require.NoError(t, err)
		require.EqualValues(t, 10, retrieved.Tokens)
		require.EqualValues(t, 2000, retrieved.LastRefilledAt)
	})
}

func TestRateLimitersTable_Set(t *testing.T) {
	t.Run("overwrites existing state", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newRateLimitersTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		require.NoError(t, table.Set(txn, accountId, queueId, &corepb.RateLimiterState{Tokens: 5}))
		require.NoError(t, txn.Commit())

		txn = badgerStore.Update()
		require.NoError(t, table.Set(txn, accountId, queueId, &corepb.RateLimiterState{Tokens: 9}))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.GetOrDefault(txn, accountId, queueId, &corepb.TokenBucketRateLimiting{MaxTokens: 10}, 0)
		require.NoError(t, err)
		require.EqualValues(t, 9, retrieved.Tokens)
	})

	t.Run("state for different queues are isolated", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newRateLimitersTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()
		queueId1, queueId2 := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		require.NoError(t, table.Set(txn, accountId, queueId1, &corepb.RateLimiterState{Tokens: 1}))
		require.NoError(t, table.Set(txn, accountId, queueId2, &corepb.RateLimiterState{Tokens: 2}))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		rateLimiting := &corepb.TokenBucketRateLimiting{MaxTokens: 10}

		retrieved1, err := table.GetOrDefault(txn, accountId, queueId1, rateLimiting, 0)
		require.NoError(t, err)
		require.EqualValues(t, 1, retrieved1.Tokens)

		retrieved2, err := table.GetOrDefault(txn, accountId, queueId2, rateLimiting, 0)
		require.NoError(t, err)
		require.EqualValues(t, 2, retrieved2.Tokens)
	})
}

func TestRateLimitersTable_Delete(t *testing.T) {
	t.Run("deletes state", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newRateLimitersTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		require.NoError(t, table.Set(txn, accountId, queueId, &corepb.RateLimiterState{Tokens: 5}))
		require.NoError(t, txn.Commit())

		txn = badgerStore.Update()
		require.NoError(t, table.Delete(txn, accountId, queueId))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		// Deleted state reads back as the default full bucket, same as never-set state.
		retrieved, err := table.GetOrDefault(txn, accountId, queueId, &corepb.TokenBucketRateLimiting{MaxTokens: 10}, 3000)
		require.NoError(t, err)
		require.EqualValues(t, 10, retrieved.Tokens)
		require.EqualValues(t, 3000, retrieved.LastRefilledAt)
	})

	t.Run("deleting nonexistent state is not an error", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newRateLimitersTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		require.NoError(t, table.Delete(txn, accountId, queueId))
		require.NoError(t, txn.Commit())
	})
}
