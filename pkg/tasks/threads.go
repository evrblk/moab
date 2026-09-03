package tasks

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/moab/pkg/corepb"
	"github.com/evrblk/moab/pkg/sharding"
)

// threadsTable tracks, for every thread that currently has at least one task
// enqueued, which task is its current head (the only task of that thread
// visible for dequeuing at a time) plus the index of every task belonging to
// the thread, ordered by ScheduledAt.
//
// Table Primary Key:
// 1. account id
// 2. queue id
// 3. thread id
//
// Threaded Tasks Index Primary Key:
// 1. account id
// 2. queue id
// 3. thread id
// Sort order: scheduled at, task id
type threadsTable struct {
	table              *honey.BinaryTable[*corepb.Thread, corepb.Thread]
	threadedTasksIndex *honey.OneToManySortedIndex
}

func newThreadsTable(replicaPrefix []byte) *threadsTable {
	return &threadsTable{
		table: honey.NewBinaryTable[*corepb.Thread, corepb.Thread](
			utils.ConcatBytes(replicaPrefix, tablePrefixThreads),
		),
		threadedTasksIndex: honey.NewOneToManySortedIndex(
			utils.ConcatBytes(replicaPrefix, tablePrefixThreadedTasksIndex),
		),
	}
}

// Clear deletes every row this table owns.
func (t *threadsTable) Clear(badgerStore *store.BadgerStore) error {
	for _, prefix := range [][]byte{t.table.TableId(), t.threadedTasksIndex.TableId()} {
		if err := badgerStore.DeletePrefix(prefix); err != nil {
			return err
		}
	}
	return nil
}

// EachEntity streams every thread as (canonical key, stored value) — the
// primary table only; the threaded tasks index is rebuilt from the tasks
// themselves on restore.
func (t *threadsTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// RestoreEntity decodes one streamed thread and, if owned, inserts it.
func (t *threadsTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds honey.ShardRange) (bool, error) {
	thread := &corepb.Thread{}
	if err := thread.UnmarshalBinary(value); err != nil {
		return false, err
	}
	if !bounds.Owns(sharding.ByAccountAndQueue(thread.HeadTaskId.AccountId, thread.HeadTaskId.QueueId)) {
		return false, nil
	}
	return true, t.set(txn, thread.HeadTaskId.AccountId, thread.HeadTaskId.QueueId, thread)
}

// Get returns a thread by (accountId, queueId, threadId).
func (t *threadsTable) Get(txn *store.Txn, accountId uint64, queueId uint64, threadId string) (*corepb.Thread, error) {
	return t.table.Get(txn, t.tablePK(accountId, queueId, threadId))
}

func (t *threadsTable) set(txn *store.Txn, accountId uint64, queueId uint64, thread *corepb.Thread) error {
	return t.table.Set(txn, t.tablePK(accountId, queueId, thread.ThreadId), thread)
}

// Delete removes a thread row by (accountId, queueId, threadId). Callers are
// responsible for having already removed every task still indexed under it
// (e.g. via RemoveFromIndex); it returns store.ErrNotFound if no such thread
// exists.
func (t *threadsTable) Delete(txn *store.Txn, accountId uint64, queueId uint64, threadId string) error {
	return t.table.Delete(txn, t.tablePK(accountId, queueId, threadId))
}

// AddToIndex adds one task to the threaded tasks index for
// (accountId, queueId, threadId), keeping the index ordered by scheduledAt.
func (t *threadsTable) AddToIndex(txn *store.Txn, accountId uint64, queueId uint64, threadId string, scheduledAt int64, taskId uint64) error {
	return t.threadedTasksIndex.Add(txn, t.threadIndexPK(accountId, queueId, threadId), threadedTasksIndexItem(scheduledAt, taskId))
}

// RemoveFromIndex removes one task from the threaded tasks index for
// (accountId, queueId, threadId).
func (t *threadsTable) RemoveFromIndex(txn *store.Txn, accountId uint64, queueId uint64, threadId string, scheduledAt int64, taskId uint64) error {
	return t.threadedTasksIndex.Delete(txn, t.threadIndexPK(accountId, queueId, threadId), threadedTasksIndexItem(scheduledAt, taskId))
}

// ListIndex lists every task id in the thread, in ScheduledAt order.
func (t *threadsTable) ListIndex(txn *store.Txn, accountId uint64, queueId uint64, threadId string, fn func(taskId uint64) (bool, error)) error {
	return t.threadedTasksIndex.ListAll(txn, t.threadIndexPK(accountId, queueId, threadId), func(item []byte) (bool, error) {
		return fn(extractTaskIdFromIndexItem(item))
	})
}

func (t *threadsTable) tablePK(accountId uint64, queueId uint64, threadId string) []byte {
	return utils.ConcatBytes(accountId, queueId, threadId)
}

func (t *threadsTable) threadIndexPK(accountId uint64, queueId uint64, threadId string) []byte {
	return utils.ConcatBytes(accountId, queueId, threadId)
}

// scheduledAt is encoded as plain big-endian, so byte-lexicographic ordering
// only matches numeric ordering for scheduledAt >= 0 (two's complement makes
// negative values sort after positive ones). promoteNextThreadHead relies on
// this index's iteration order to pick "the earliest remaining task" as the
// first item, so scheduledAt must never be negative for that to be correct;
// in practice it never is, since it's always max(requested ScheduledAt, now).
func threadedTasksIndexItem(scheduledAt int64, taskId uint64) []byte {
	return utils.ConcatBytes(scheduledAt, taskId)
}
