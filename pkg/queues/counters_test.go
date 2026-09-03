package queues

import (
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/moab/pkg/corepb"
)

func TestCountersTable_Get(t *testing.T) {
	t.Run("get existing counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()

		counter := &corepb.QueuesCounter{
			NumberOfQueues: 42,
		}

		txn := badgerStore.Update()
		err = table.Set(txn, accountId, counter)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Get counter
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, accountId)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.EqualValues(t, 42, retrieved.NumberOfQueues)
	})

	t.Run("get nonexistent counter returns default", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()

		txn := badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, accountId)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.EqualValues(t, 0, retrieved.NumberOfQueues)
	})
}

func TestCountersTable_Set(t *testing.T) {
	t.Run("set counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()

		counter := &corepb.QueuesCounter{
			NumberOfQueues: 10,
		}

		txn := badgerStore.Update()
		err = table.Set(txn, accountId, counter)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify counter was set
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, accountId)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.EqualValues(t, 10, retrieved.NumberOfQueues)
	})

	t.Run("set overwrites existing counter", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()

		counter1 := &corepb.QueuesCounter{
			NumberOfQueues: 10,
		}

		txn := badgerStore.Update()
		err = table.Set(txn, accountId, counter1)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Overwrite with new value
		counter2 := &corepb.QueuesCounter{
			NumberOfQueues: 25,
		}

		txn = badgerStore.Update()
		err = table.Set(txn, accountId, counter2)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify new value
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, accountId)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.EqualValues(t, 25, retrieved.NumberOfQueues)
	})

	t.Run("set counters for different accounts are isolated", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newCountersTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()

		counter1 := &corepb.QueuesCounter{
			NumberOfQueues: 10,
		}

		counter2 := &corepb.QueuesCounter{
			NumberOfQueues: 20,
		}

		txn := badgerStore.Update()
		err = table.Set(txn, accountId1, counter1)
		require.NoError(t, err)
		err = table.Set(txn, accountId2, counter2)
		require.NoError(t, err)
		err = txn.Commit()
		require.NoError(t, err)

		// Verify both counters are independent
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved1, err := table.Get(txn, accountId1)
		require.NoError(t, err)
		require.EqualValues(t, 10, retrieved1.NumberOfQueues)

		retrieved2, err := table.Get(txn, accountId2)
		require.NoError(t, err)
		require.EqualValues(t, 20, retrieved2.NumberOfQueues)
	})
}
