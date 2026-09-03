package tasks

import (
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/moab/pkg/corepb"
)

func TestExpirationIndex_Add(t *testing.T) {
	t.Run("adds a task to the index", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		index := newExpirationIndex([]byte{0x77, 0x77, 0x77, 0x77})

		taskId := &corepb.TaskId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), TaskId: rand.Uint64()}

		txn := badgerStore.Update()
		require.NoError(t, index.add(txn, taskId, 1000))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		refs, err := index.ListDue(txn, 1000, 10)
		require.NoError(t, err)
		require.Len(t, refs, 1)
		require.Equal(t, taskId.AccountId, refs[0].TaskId.AccountId)
		require.Equal(t, taskId.QueueId, refs[0].TaskId.QueueId)
		require.Equal(t, taskId.TaskId, refs[0].TaskId.TaskId)
		require.EqualValues(t, 1000, refs[0].ExpiresAt)
	})
}

func TestExpirationIndex_Remove(t *testing.T) {
	t.Run("removes a task from the index", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		index := newExpirationIndex([]byte{0x77, 0x77, 0x77, 0x77})

		taskId := &corepb.TaskId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), TaskId: rand.Uint64()}

		txn := badgerStore.Update()
		require.NoError(t, index.add(txn, taskId, 1000))
		require.NoError(t, txn.Commit())

		txn = badgerStore.Update()
		require.NoError(t, index.remove(txn, taskId, 1000))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		refs, err := index.ListDue(txn, 1000, 10)
		require.NoError(t, err)
		require.Len(t, refs, 0)
	})

	t.Run("removing a nonexistent entry is not an error", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		index := newExpirationIndex([]byte{0x77, 0x77, 0x77, 0x77})

		taskId := &corepb.TaskId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), TaskId: rand.Uint64()}

		txn := badgerStore.Update()
		require.NoError(t, index.remove(txn, taskId, 1000))
		require.NoError(t, txn.Commit())
	})
}

func TestExpirationIndex_ListDue(t *testing.T) {
	t.Run("returns tasks due before a given time", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		index := newExpirationIndex([]byte{0x77, 0x77, 0x77, 0x77})

		due := &corepb.TaskId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), TaskId: rand.Uint64()}
		notYetDue := &corepb.TaskId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), TaskId: rand.Uint64()}

		txn := badgerStore.Update()
		require.NoError(t, index.add(txn, due, 1000))
		require.NoError(t, index.add(txn, notYetDue, 5000))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		refs, err := index.ListDue(txn, 2000, 10)
		require.NoError(t, err)
		require.Len(t, refs, 1)
		require.Equal(t, due.TaskId, refs[0].TaskId.TaskId)
	})

	t.Run("a task due exactly at the cutoff is included", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		index := newExpirationIndex([]byte{0x77, 0x77, 0x77, 0x77})

		taskId := &corepb.TaskId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), TaskId: rand.Uint64()}

		txn := badgerStore.Update()
		require.NoError(t, index.add(txn, taskId, 1000))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		// "at or before": before == ExpiresAt must include it
		refs, err := index.ListDue(txn, 1000, 10)
		require.NoError(t, err)
		require.Len(t, refs, 1)

		// One unit earlier must not
		refs, err = index.ListDue(txn, 999, 10)
		require.NoError(t, err)
		require.Len(t, refs, 0)
	})

	t.Run("respects the limit", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		index := newExpirationIndex([]byte{0x77, 0x77, 0x77, 0x77})

		txn := badgerStore.Update()
		for i := range 5 {
			taskId := &corepb.TaskId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), TaskId: rand.Uint64()}
			require.NoError(t, index.add(txn, taskId, int64(1000+i)))
		}
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		refs, err := index.ListDue(txn, 10000, 3)
		require.NoError(t, err)
		require.Len(t, refs, 3)
	})

	t.Run("aggregates due tasks across queues in ascending ExpiresAt order", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		index := newExpirationIndex([]byte{0x77, 0x77, 0x77, 0x77})

		taskId1 := &corepb.TaskId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), TaskId: rand.Uint64()}
		taskId2 := &corepb.TaskId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), TaskId: rand.Uint64()}

		txn := badgerStore.Update()
		require.NoError(t, index.add(txn, taskId1, 2000))
		require.NoError(t, index.add(txn, taskId2, 1000))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		refs, err := index.ListDue(txn, 3000, 10)
		require.NoError(t, err)
		require.Len(t, refs, 2)
		require.Equal(t, taskId2.TaskId, refs[0].TaskId.TaskId)
		require.EqualValues(t, 1000, refs[0].ExpiresAt)
		require.Equal(t, taskId1.TaskId, refs[1].TaskId.TaskId)
		require.EqualValues(t, 2000, refs[1].ExpiresAt)
	})

	t.Run("returns nothing when no tasks are due", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		index := newExpirationIndex([]byte{0x77, 0x77, 0x77, 0x77})

		taskId := &corepb.TaskId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), TaskId: rand.Uint64()}

		txn := badgerStore.Update()
		require.NoError(t, index.add(txn, taskId, 5000))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		refs, err := index.ListDue(txn, 1000, 10)
		require.NoError(t, err)
		require.Len(t, refs, 0)
	})

	t.Run("returns tasks in ascending ExpiresAt order up to the limit", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		index := newExpirationIndex([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		for i := range 10 {
			taskId := &corepb.TaskId{AccountId: accountId, QueueId: queueId, TaskId: uint64(i)}
			require.NoError(t, index.add(txn, taskId, int64(1000+i)))
		}
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		refs, err := index.ListDue(txn, 10000, 4)
		require.NoError(t, err)
		require.Len(t, refs, 4)
		for i, ref := range refs {
			require.EqualValues(t, i, ref.TaskId.TaskId)
		}
	})
}
