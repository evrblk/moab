package tasks

import (
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/moab/pkg/corepb"
)

func TestThreadsTable_Get(t *testing.T) {
	t.Run("get existing thread", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newThreadsTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		thread := &corepb.Thread{
			ThreadId:    "thread-1",
			HeadTaskId:  &corepb.TaskId{AccountId: accountId, QueueId: queueId, TaskId: 1},
			ScheduledAt: 1000,
		}

		txn := badgerStore.Update()
		require.NoError(t, table.set(txn, accountId, queueId, thread))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, accountId, queueId, "thread-1")
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.EqualValues(t, 1, retrieved.HeadTaskId.TaskId)
		require.EqualValues(t, 1000, retrieved.ScheduledAt)
	})

	t.Run("get nonexistent thread", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newThreadsTable([]byte{0x77, 0x77, 0x77, 0x77})

		txn := badgerStore.View()
		defer txn.Discard()

		_, err = table.Get(txn, rand.Uint64(), rand.Uint64(), "nonexistent")
		require.Error(t, err)
		require.ErrorIs(t, err, store.ErrNotFound)
	})
}

func TestThreadsTable_Delete(t *testing.T) {
	t.Run("deletes a thread", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newThreadsTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		thread := &corepb.Thread{
			ThreadId:   "thread-1",
			HeadTaskId: &corepb.TaskId{AccountId: accountId, QueueId: queueId, TaskId: 1},
		}

		txn := badgerStore.Update()
		require.NoError(t, table.set(txn, accountId, queueId, thread))
		require.NoError(t, txn.Commit())

		txn = badgerStore.Update()
		require.NoError(t, table.Delete(txn, accountId, queueId, "thread-1"))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		_, err = table.Get(txn, accountId, queueId, "thread-1")
		require.ErrorIs(t, err, store.ErrNotFound)
	})

	t.Run("deleting a nonexistent thread is not an error", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newThreadsTable([]byte{0x77, 0x77, 0x77, 0x77})

		txn := badgerStore.Update()
		require.NoError(t, table.Delete(txn, rand.Uint64(), rand.Uint64(), "nonexistent"))
		require.NoError(t, txn.Commit())
	})
}

func TestThreadsTable_Index(t *testing.T) {
	t.Run("AddToIndex and ListIndex return tasks in ScheduledAt order", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newThreadsTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		require.NoError(t, table.AddToIndex(txn, accountId, queueId, "thread-1", 3000, 3))
		require.NoError(t, table.AddToIndex(txn, accountId, queueId, "thread-1", 1000, 1))
		require.NoError(t, table.AddToIndex(txn, accountId, queueId, "thread-1", 2000, 2))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		var taskIds []uint64
		err = table.ListIndex(txn, accountId, queueId, "thread-1", func(taskId uint64) (bool, error) {
			taskIds = append(taskIds, taskId)
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3}, taskIds)
	})

	t.Run("RemoveFromIndex removes only the given task", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newThreadsTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		require.NoError(t, table.AddToIndex(txn, accountId, queueId, "thread-1", 1000, 1))
		require.NoError(t, table.AddToIndex(txn, accountId, queueId, "thread-1", 2000, 2))
		require.NoError(t, txn.Commit())

		txn = badgerStore.Update()
		require.NoError(t, table.RemoveFromIndex(txn, accountId, queueId, "thread-1", 1000, 1))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		var taskIds []uint64
		err = table.ListIndex(txn, accountId, queueId, "thread-1", func(taskId uint64) (bool, error) {
			taskIds = append(taskIds, taskId)
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, []uint64{2}, taskIds)
	})

	t.Run("ListIndex can stop early", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newThreadsTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		for i := range 5 {
			require.NoError(t, table.AddToIndex(txn, accountId, queueId, "thread-1", int64(1000+i), uint64(i)))
		}
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		var taskIds []uint64
		err = table.ListIndex(txn, accountId, queueId, "thread-1", func(taskId uint64) (bool, error) {
			taskIds = append(taskIds, taskId)
			return len(taskIds) < 2, nil
		})
		require.NoError(t, err)
		require.Equal(t, []uint64{0, 1}, taskIds)
	})

	t.Run("different threads are isolated", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newThreadsTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		require.NoError(t, table.AddToIndex(txn, accountId, queueId, "thread-1", 1000, 1))
		require.NoError(t, table.AddToIndex(txn, accountId, queueId, "thread-2", 1000, 2))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		var thread1TaskIds []uint64
		err = table.ListIndex(txn, accountId, queueId, "thread-1", func(taskId uint64) (bool, error) {
			thread1TaskIds = append(thread1TaskIds, taskId)
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, []uint64{1}, thread1TaskIds)

		var thread2TaskIds []uint64
		err = table.ListIndex(txn, accountId, queueId, "thread-2", func(taskId uint64) (bool, error) {
			thread2TaskIds = append(thread2TaskIds, taskId)
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, []uint64{2}, thread2TaskIds)
	})
}
