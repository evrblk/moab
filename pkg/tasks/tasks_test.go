package tasks

import (
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/moab/pkg/corepb"
)

func TestTasksTable_Get(t *testing.T) {
	t.Run("get existing task", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newTasksTable([]byte{0x77, 0x77, 0x77, 0x77})

		taskId := &corepb.TaskId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), TaskId: 1}
		task := &corepb.Task{
			Id:          taskId,
			Payload:     []byte("payload-1"),
			ScheduledAt: 1000,
			State:       corepb.TaskState_TASK_STATE_ENQUEUED,
		}

		txn := badgerStore.Update()
		require.NoError(t, table.set(txn, task))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, taskId)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, []byte("payload-1"), retrieved.Payload)
		require.EqualValues(t, 1000, retrieved.ScheduledAt)
	})

	t.Run("get nonexistent task", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newTasksTable([]byte{0x77, 0x77, 0x77, 0x77})

		txn := badgerStore.View()
		defer txn.Discard()

		_, err = table.Get(txn, &corepb.TaskId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), TaskId: 999})
		require.Error(t, err)
		require.ErrorIs(t, err, store.ErrNotFound)
	})
}

func TestTasksTable_Set(t *testing.T) {
	t.Run("overwrites an existing task", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newTasksTable([]byte{0x77, 0x77, 0x77, 0x77})

		taskId := &corepb.TaskId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), TaskId: 1}

		txn := badgerStore.Update()
		require.NoError(t, table.set(txn, &corepb.Task{Id: taskId, Payload: []byte("v1")}))
		require.NoError(t, txn.Commit())

		txn = badgerStore.Update()
		require.NoError(t, table.set(txn, &corepb.Task{Id: taskId, Payload: []byte("v2")}))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, taskId)
		require.NoError(t, err)
		require.Equal(t, []byte("v2"), retrieved.Payload)
	})
}

func TestTasksTable_Delete(t *testing.T) {
	t.Run("deletes a task", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newTasksTable([]byte{0x77, 0x77, 0x77, 0x77})

		taskId := &corepb.TaskId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), TaskId: 1}

		txn := badgerStore.Update()
		require.NoError(t, table.set(txn, &corepb.Task{Id: taskId}))
		require.NoError(t, txn.Commit())

		txn = badgerStore.Update()
		require.NoError(t, table.delete(txn, taskId))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		_, err = table.Get(txn, taskId)
		require.ErrorIs(t, err, store.ErrNotFound)
	})

	t.Run("deleting a nonexistent task is not an error", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newTasksTable([]byte{0x77, 0x77, 0x77, 0x77})

		taskId := &corepb.TaskId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), TaskId: 1}

		txn := badgerStore.Update()
		require.NoError(t, table.delete(txn, taskId))
		require.NoError(t, txn.Commit())
	})
}

func TestTasksTable_ListAll(t *testing.T) {
	t.Run("lists every task for a queue", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newTasksTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		for i := range uint64(3) {
			require.NoError(t, table.set(txn, &corepb.Task{
				Id: &corepb.TaskId{AccountId: accountId, QueueId: queueId, TaskId: i + 1},
			}))
		}
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		tasks, err := table.ListAll(txn, accountId, queueId)
		require.NoError(t, err)
		require.Len(t, tasks, 3)
	})

	t.Run("different queues are isolated", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newTasksTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()
		queueId1, queueId2 := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		require.NoError(t, table.set(txn, &corepb.Task{Id: &corepb.TaskId{AccountId: accountId, QueueId: queueId1, TaskId: 1}}))
		require.NoError(t, table.set(txn, &corepb.Task{Id: &corepb.TaskId{AccountId: accountId, QueueId: queueId2, TaskId: 1}}))
		require.NoError(t, table.set(txn, &corepb.Task{Id: &corepb.TaskId{AccountId: accountId, QueueId: queueId2, TaskId: 2}}))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		tasks1, err := table.ListAll(txn, accountId, queueId1)
		require.NoError(t, err)
		require.Len(t, tasks1, 1)

		tasks2, err := table.ListAll(txn, accountId, queueId2)
		require.NoError(t, err)
		require.Len(t, tasks2, 2)
	})

	t.Run("empty queue", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newTasksTable([]byte{0x77, 0x77, 0x77, 0x77})

		txn := badgerStore.View()
		defer txn.Discard()

		tasks, err := table.ListAll(txn, rand.Uint64(), rand.Uint64())
		require.NoError(t, err)
		require.Len(t, tasks, 0)
	})
}

func TestTasksTable_DedupeKeysIndex(t *testing.T) {
	t.Run("resolves a dedupe key to a task id", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newTasksTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		require.NoError(t, table.dedupeKeysIndex.Set(txn, table.dedupeKeysIndexPK(accountId, queueId, "dedupe-1"), 42))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		taskId, err := table.dedupeKeysIndex.Get(txn, table.dedupeKeysIndexPK(accountId, queueId, "dedupe-1"))
		require.NoError(t, err)
		require.EqualValues(t, 42, taskId)
	})

	t.Run("removing a dedupe key entry", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newTasksTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		require.NoError(t, table.dedupeKeysIndex.Set(txn, table.dedupeKeysIndexPK(accountId, queueId, "dedupe-1"), 42))
		require.NoError(t, txn.Commit())

		txn = badgerStore.Update()
		require.NoError(t, table.dedupeKeysIndex.Delete(txn, table.dedupeKeysIndexPK(accountId, queueId, "dedupe-1")))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		_, err = table.dedupeKeysIndex.Get(txn, table.dedupeKeysIndexPK(accountId, queueId, "dedupe-1"))
		require.ErrorIs(t, err, store.ErrNotFound)
	})

	t.Run("dedupe keys are isolated per queue", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newTasksTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()
		queueId1, queueId2 := rand.Uint64(), rand.Uint64()

		txn := badgerStore.Update()
		require.NoError(t, table.dedupeKeysIndex.Set(txn, table.dedupeKeysIndexPK(accountId, queueId1, "dedupe-1"), 1))
		require.NoError(t, table.dedupeKeysIndex.Set(txn, table.dedupeKeysIndexPK(accountId, queueId2, "dedupe-1"), 2))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		taskId1, err := table.dedupeKeysIndex.Get(txn, table.dedupeKeysIndexPK(accountId, queueId1, "dedupe-1"))
		require.NoError(t, err)
		require.EqualValues(t, 1, taskId1)

		taskId2, err := table.dedupeKeysIndex.Get(txn, table.dedupeKeysIndexPK(accountId, queueId2, "dedupe-1"))
		require.NoError(t, err)
		require.EqualValues(t, 2, taskId2)
	})
}

func TestTasksTable_QueueIndex(t *testing.T) {
	t.Run("ListInRange returns tasks in ScheduledAt order", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newTasksTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()
		pk := table.tablePK(accountId, queueId)

		txn := badgerStore.Update()
		require.NoError(t, table.queueIndex.Add(txn, pk, queueIndexItem(3000, 3)))
		require.NoError(t, table.queueIndex.Add(txn, pk, queueIndexItem(1000, 1)))
		require.NoError(t, table.queueIndex.Add(txn, pk, queueIndexItem(2000, 2)))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		var taskIds []uint64
		err = table.queueIndex.ListInRange(txn, pk, itemPrefix(0), itemPrefix(2500), func(item []byte) (bool, error) {
			taskIds = append(taskIds, extractTaskIdFromIndexItem(item))
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2}, taskIds)
	})

	t.Run("Delete removes a single entry", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newTasksTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()
		pk := table.tablePK(accountId, queueId)

		txn := badgerStore.Update()
		require.NoError(t, table.queueIndex.Add(txn, pk, queueIndexItem(1000, 1)))
		require.NoError(t, table.queueIndex.Add(txn, pk, queueIndexItem(2000, 2)))
		require.NoError(t, txn.Commit())

		txn = badgerStore.Update()
		require.NoError(t, table.queueIndex.Delete(txn, pk, queueIndexItem(1000, 1)))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		var taskIds []uint64
		err = table.queueIndex.ListAll(txn, pk, func(item []byte) (bool, error) {
			taskIds = append(taskIds, extractTaskIdFromIndexItem(item))
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, []uint64{2}, taskIds)
	})
}

func TestTasksTable_InProgressIndex(t *testing.T) {
	t.Run("ListInRange returns tasks in VisibleAt order", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newTasksTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()
		pk := table.tablePK(accountId, queueId)

		txn := badgerStore.Update()
		require.NoError(t, table.inProgressIndex.Add(txn, pk, inProgressIndexItem(2000, 2)))
		require.NoError(t, table.inProgressIndex.Add(txn, pk, inProgressIndexItem(1000, 1)))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		var taskIds []uint64
		err = table.inProgressIndex.ListInRange(txn, pk, itemPrefix(0), itemPrefix(3000), func(item []byte) (bool, error) {
			taskIds = append(taskIds, extractTaskIdFromIndexItem(item))
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2}, taskIds)
	})
}

func TestTasksTable_DeadTasksIndex(t *testing.T) {
	t.Run("Add and Delete a dead task entry", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newTasksTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId, queueId := rand.Uint64(), rand.Uint64()
		pk := table.tablePK(accountId, queueId)

		txn := badgerStore.Update()
		require.NoError(t, table.deadTasksIndex.Add(txn, pk, deadTasksIndexItem(1000, 1)))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		notEmpty, err := table.deadTasksIndex.NotEmpty(txn, pk)
		txn.Discard()
		require.NoError(t, err)
		require.True(t, notEmpty)

		txn = badgerStore.Update()
		require.NoError(t, table.deadTasksIndex.Delete(txn, pk, deadTasksIndexItem(1000, 1)))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		notEmpty, err = table.deadTasksIndex.NotEmpty(txn, pk)
		require.NoError(t, err)
		require.False(t, notEmpty)
	})
}

func TestIndexItemEncoding(t *testing.T) {
	t.Run("queueIndexItem round-trips scheduledAt and taskId", func(t *testing.T) {
		item := queueIndexItem(123456, 42)
		require.EqualValues(t, 123456, extractTimeFromIndexItem(item))
		require.EqualValues(t, 42, extractTaskIdFromIndexItem(item))
	})

	t.Run("inProgressIndexItem round-trips visibleAt and taskId", func(t *testing.T) {
		item := inProgressIndexItem(654321, 7)
		require.EqualValues(t, 654321, extractTimeFromIndexItem(item))
		require.EqualValues(t, 7, extractTaskIdFromIndexItem(item))
	})

	t.Run("deadTasksIndexItem round-trips lastFailedAt and taskId", func(t *testing.T) {
		item := deadTasksIndexItem(999, 3)
		require.EqualValues(t, 999, extractTimeFromIndexItem(item))
		require.EqualValues(t, 3, extractTaskIdFromIndexItem(item))
	})
}
