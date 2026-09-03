package tasks

import (
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/moab/pkg/corepb"
)

func TestQueueStateTable_Get(t *testing.T) {
	t.Run("get existing state", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueueStateTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		state := &corepb.QueueState{
			TaskIdSequence:        42,
			LastVisitedEnqueued:   100,
			LastVisitedInProgress: 200,
		}

		txn := badgerStore.Update()
		require.NoError(t, table.Set(txn, accountId, queueId, state))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, accountId, queueId)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.EqualValues(t, 42, retrieved.TaskIdSequence)
		require.EqualValues(t, 100, retrieved.LastVisitedEnqueued)
		require.EqualValues(t, 200, retrieved.LastVisitedInProgress)
	})

	t.Run("get nonexistent state returns zero value", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueueStateTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		txn := badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, accountId, queueId)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.EqualValues(t, 0, retrieved.TaskIdSequence)
		require.EqualValues(t, 0, retrieved.LastVisitedEnqueued)
		require.EqualValues(t, 0, retrieved.LastVisitedInProgress)
	})
}

func TestQueueStateTable_Set(t *testing.T) {
	t.Run("overwrites existing state", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueueStateTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		require.NoError(t, table.Set(txn, accountId, queueId, &corepb.QueueState{TaskIdSequence: 1}))
		require.NoError(t, txn.Commit())

		txn = badgerStore.Update()
		require.NoError(t, table.Set(txn, accountId, queueId, &corepb.QueueState{TaskIdSequence: 2}))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, accountId, queueId)
		require.NoError(t, err)
		require.EqualValues(t, 2, retrieved.TaskIdSequence)
	})

	t.Run("state for different queues are isolated", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueueStateTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()
		queueId1, queueId2 := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		require.NoError(t, table.Set(txn, accountId, queueId1, &corepb.QueueState{TaskIdSequence: 1}))
		require.NoError(t, table.Set(txn, accountId, queueId2, &corepb.QueueState{TaskIdSequence: 2}))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved1, err := table.Get(txn, accountId, queueId1)
		require.NoError(t, err)
		require.EqualValues(t, 1, retrieved1.TaskIdSequence)

		retrieved2, err := table.Get(txn, accountId, queueId2)
		require.NoError(t, err)
		require.EqualValues(t, 2, retrieved2.TaskIdSequence)
	})
}
