package tasks

import (
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/moab/pkg/corepb"
)

func TestCountersTable_Get(t *testing.T) {
	t.Run("get existing counters", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		counters := &corepb.TasksCounter{
			EnqueuedTasksCount:   10,
			InProgressTasksCount: 5,
			DeadTasksCount:       2,
		}

		txn := badgerStore.Update()
		require.NoError(t, table.Set(txn, accountId, queueId, counters))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, accountId, queueId)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.EqualValues(t, 10, retrieved.EnqueuedTasksCount)
		require.EqualValues(t, 5, retrieved.InProgressTasksCount)
		require.EqualValues(t, 2, retrieved.DeadTasksCount)
	})

	t.Run("get nonexistent counters returns zero value", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		txn := badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, accountId, queueId)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.EqualValues(t, 0, retrieved.EnqueuedTasksCount)
		require.EqualValues(t, 0, retrieved.InProgressTasksCount)
		require.EqualValues(t, 0, retrieved.DeadTasksCount)
	})
}

func TestCountersTable_Set(t *testing.T) {
	t.Run("set counters", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		counters := &corepb.TasksCounter{EnqueuedTasksCount: 3}

		txn := badgerStore.Update()
		require.NoError(t, table.Set(txn, accountId, queueId, counters))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, accountId, queueId)
		require.NoError(t, err)
		require.EqualValues(t, 3, retrieved.EnqueuedTasksCount)
	})

	t.Run("set overwrites existing counters", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		require.NoError(t, table.Set(txn, accountId, queueId, &corepb.TasksCounter{EnqueuedTasksCount: 3}))
		require.NoError(t, txn.Commit())

		txn = badgerStore.Update()
		require.NoError(t, table.Set(txn, accountId, queueId, &corepb.TasksCounter{EnqueuedTasksCount: 7}))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, accountId, queueId)
		require.NoError(t, err)
		require.EqualValues(t, 7, retrieved.EnqueuedTasksCount)
	})

	t.Run("counters for different queues are isolated", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()
		queueId1, queueId2 := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		require.NoError(t, table.Set(txn, accountId, queueId1, &corepb.TasksCounter{EnqueuedTasksCount: 10}))
		require.NoError(t, table.Set(txn, accountId, queueId2, &corepb.TasksCounter{EnqueuedTasksCount: 20}))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved1, err := table.Get(txn, accountId, queueId1)
		require.NoError(t, err)
		require.EqualValues(t, 10, retrieved1.EnqueuedTasksCount)

		retrieved2, err := table.Get(txn, accountId, queueId2)
		require.NoError(t, err)
		require.EqualValues(t, 20, retrieved2.EnqueuedTasksCount)
	})
}

func TestCountersTable_Delete(t *testing.T) {
	t.Run("deletes counters", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		require.NoError(t, table.Set(txn, accountId, queueId, &corepb.TasksCounter{EnqueuedTasksCount: 3}))
		require.NoError(t, txn.Commit())

		txn = badgerStore.Update()
		require.NoError(t, table.Delete(txn, accountId, queueId))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		// Deleted counters read back as the zero value, same as never-set ones.
		retrieved, err := table.Get(txn, accountId, queueId)
		require.NoError(t, err)
		require.EqualValues(t, 0, retrieved.EnqueuedTasksCount)
	})

	t.Run("deleting nonexistent counters is not an error", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		require.NoError(t, table.Delete(txn, accountId, queueId))
		require.NoError(t, txn.Commit())
	})
}
