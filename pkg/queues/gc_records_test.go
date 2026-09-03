package queues

import (
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/moab/pkg/corepb"
)

func TestGCRecordsTable_Create(t *testing.T) {
	t.Run("creates a gc record", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x77, 0x77, 0x77, 0x77})

		queueId := &corepb.QueueId{
			AccountId: rand.Uint64(),
			QueueId:   rand.Uint64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, &corepb.QueuesGarbageCollectionRecord{QueueId: queueId})
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify it was created by listing
		txn = badgerStore.View()
		records, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 1)
		require.Equal(t, queueId.AccountId, records[0].QueueId.AccountId)
		require.Equal(t, queueId.QueueId, records[0].QueueId.QueueId)
	})

	t.Run("creates multiple gc records", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()

		txn := badgerStore.Update()
		for i := range 5 {
			err = table.Create(txn, &corepb.QueuesGarbageCollectionRecord{
				QueueId: &corepb.QueueId{AccountId: accountId, QueueId: uint64(i + 1)},
			})
			require.NoError(t, err)
		}
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		records, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 5)
	})

	t.Run("create overwrites a record for the same queue", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x77, 0x77, 0x77, 0x77})

		queueId := &corepb.QueueId{
			AccountId: rand.Uint64(),
			QueueId:   rand.Uint64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, &corepb.QueuesGarbageCollectionRecord{QueueId: queueId})
		require.NoError(t, err)
		err = table.Create(txn, &corepb.QueuesGarbageCollectionRecord{QueueId: queueId})
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		records, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 1)
	})
}

func TestGCRecordsTable_Delete(t *testing.T) {
	t.Run("deletes a gc record", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x77, 0x77, 0x77, 0x77})

		record := &corepb.QueuesGarbageCollectionRecord{
			QueueId: &corepb.QueueId{AccountId: rand.Uint64(), QueueId: rand.Uint64()},
		}

		txn := badgerStore.Update()
		err = table.Create(txn, record)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.Update()
		err = table.Delete(txn, record)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		records, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 0)
	})

	t.Run("deletes a non-existent gc record", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x77, 0x77, 0x77, 0x77})

		record := &corepb.QueuesGarbageCollectionRecord{
			QueueId: &corepb.QueueId{AccountId: rand.Uint64(), QueueId: rand.Uint64()},
		}

		// Delete non-existent record - should succeed (idempotent)
		txn := badgerStore.Update()
		err = table.Delete(txn, record)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		records, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 0)
	})

	t.Run("deletes one of multiple gc records", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()

		records := make([]*corepb.QueuesGarbageCollectionRecord, 3)
		txn := badgerStore.Update()
		for i := range 3 {
			records[i] = &corepb.QueuesGarbageCollectionRecord{
				QueueId: &corepb.QueueId{AccountId: accountId, QueueId: uint64(i + 1)},
			}
			err = table.Create(txn, records[i])
			require.NoError(t, err)
		}
		require.NoError(t, txn.Commit())

		// Delete the middle record
		txn = badgerStore.Update()
		err = table.Delete(txn, records[1])
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		remaining, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, remaining, 2)
		require.EqualValues(t, 1, remaining[0].QueueId.QueueId)
		require.EqualValues(t, 3, remaining[1].QueueId.QueueId)
	})
}

func TestGCRecordsTable_List(t *testing.T) {
	t.Run("lists empty gc records", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x77, 0x77, 0x77, 0x77})

		txn := badgerStore.View()
		records, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 0)
	})

	t.Run("lists multiple gc records in key order", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()

		txn := badgerStore.Update()
		for i := range 5 {
			err = table.Create(txn, &corepb.QueuesGarbageCollectionRecord{
				QueueId: &corepb.QueueId{AccountId: accountId, QueueId: uint64(i + 1)},
			})
			require.NoError(t, err)
		}
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		records, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 5)
		for i := range 5 {
			require.EqualValues(t, i+1, records[i].QueueId.QueueId)
		}
	})

	t.Run("lists gc records with limit", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()

		txn := badgerStore.Update()
		for i := range 10 {
			err = table.Create(txn, &corepb.QueuesGarbageCollectionRecord{
				QueueId: &corepb.QueueId{AccountId: accountId, QueueId: uint64(i + 1)},
			})
			require.NoError(t, err)
		}
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		records, err := table.List(txn, 5)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 5)
		for i := range 5 {
			require.EqualValues(t, i+1, records[i].QueueId.QueueId)
		}
	})

	t.Run("lists gc records across accounts", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newGCRecordsTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()

		txn := badgerStore.Update()
		err = table.Create(txn, &corepb.QueuesGarbageCollectionRecord{
			QueueId: &corepb.QueueId{AccountId: accountId1, QueueId: rand.Uint64()},
		})
		require.NoError(t, err)
		err = table.Create(txn, &corepb.QueuesGarbageCollectionRecord{
			QueueId: &corepb.QueueId{AccountId: accountId2, QueueId: rand.Uint64()},
		})
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// List is not scoped to an account: both records show up in one page.
		txn = badgerStore.View()
		records, err := table.List(txn, 10)
		txn.Discard()

		require.NoError(t, err)
		require.Len(t, records, 2)
	})
}
