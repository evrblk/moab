package tasks

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/moab/pkg/corepb"
)

// tasksTable is the primary table of tasks, plus every secondary index that
// tracks a task's own lifecycle. It holds no reference to threadsTable —
// the two are independent leaf tables. Every invariant that couples them
// (only a thread's head ever appears in queueIndex) is resolved by Core,
// which holds both tables; see the "Thread head coordination" section of
// core.go.
//
// Table Primary Key:
// 1. account id
// 2. queue id
//
// Table Sort Key:
// 1. task id
//
// Dedupe Keys Index Primary Key:
// 1. account id
// 2. queue id
// 3. dedupe key
//
// Queue Index (ENQUEUED tasks ready to dequeue — only a thread's head, never
// its other members), In Progress Index (dequeued tasks awaiting ack or
// keepalive timeout) and Dead Tasks Index (DLQ) share the same shape:
// Primary Key: account id, queue id
// Sort order: a timestamp (scheduled/visible/last-failed at, respectively), task id
type tasksTable struct {
	table           *honey.BinaryTable[*corepb.Task, corepb.Task]
	dedupeKeysIndex *honey.Uint64Table
	queueIndex      *honey.OneToManySortedIndex
	inProgressIndex *honey.OneToManySortedIndex
	deadTasksIndex  *honey.OneToManySortedIndex
	expiration      *expirationIndex
}

func newTasksTable(replicaPrefix []byte) *tasksTable {
	return &tasksTable{
		table: honey.NewBinaryTable[*corepb.Task, corepb.Task](
			utils.ConcatBytes(replicaPrefix, tablePrefixTasks),
		),
		dedupeKeysIndex: honey.NewUint64Table(
			utils.ConcatBytes(replicaPrefix, tablePrefixDedupeKeysIndex),
		),
		queueIndex: honey.NewOneToManySortedIndex(
			utils.ConcatBytes(replicaPrefix, tablePrefixQueueIndex),
		),
		inProgressIndex: honey.NewOneToManySortedIndex(
			utils.ConcatBytes(replicaPrefix, tablePrefixInProgressIndex),
		),
		deadTasksIndex: honey.NewOneToManySortedIndex(
			utils.ConcatBytes(replicaPrefix, tablePrefixDeadTasksIndex),
		),
		expiration: newExpirationIndex(replicaPrefix),
	}
}

// Clear deletes every row this table owns.
func (t *tasksTable) Clear(badgerStore *store.BadgerStore) error {
	for _, prefix := range [][]byte{
		t.table.TableId(), t.dedupeKeysIndex.TableId(), t.queueIndex.TableId(),
		t.inProgressIndex.TableId(), t.deadTasksIndex.TableId(),
		t.expiration.index.TableId(),
	} {
		if err := badgerStore.DeletePrefix(prefix); err != nil {
			return err
		}
	}
	return nil
}

// EachEntity streams every task as (canonical key, stored value) — the
// primary table only; every secondary index is rebuilt from the tasks
// themselves on restore.
func (t *tasksTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// restoreIndexes persists a decoded task and rebuilds every secondary index
// this table owns. isHead is meaningless when task.ThreadId == "" (a
// non-threaded task is always its own head); for a threaded task, only Core
// can resolve it (see taskRestorer in core.go), since this table holds no
// reference to threadsTable.
func (t *tasksTable) restoreIndexes(txn *store.Txn, task *corepb.Task, isHead bool) error {
	if err := t.set(txn, task); err != nil {
		return err
	}

	// Dead tasks are removed from the dedupe keys index as soon as they die.
	if task.DedupeKey != "" && task.State != corepb.TaskState_TASK_STATE_DEAD {
		if err := t.dedupeKeysIndex.Set(txn, t.dedupeKeysIndexPK(task.Id.AccountId, task.Id.QueueId, task.DedupeKey), task.Id.TaskId); err != nil {
			return err
		}
	}

	switch task.State {
	case corepb.TaskState_TASK_STATE_ENQUEUED:
		// Only a thread's head (or a non-threaded task, always its own head)
		// is ever dequeued, so only heads belong in queueIndex.
		if isHead {
			if err := t.queueIndex.Add(txn, t.tablePK(task.Id.AccountId, task.Id.QueueId), queueIndexItem(task.ScheduledAt, task.Id.TaskId)); err != nil {
				return err
			}
		}
	case corepb.TaskState_TASK_STATE_IN_PROGRESS:
		if err := t.inProgressIndex.Add(txn, t.tablePK(task.Id.AccountId, task.Id.QueueId), inProgressIndexItem(task.VisibleAt, task.Id.TaskId)); err != nil {
			return err
		}
	case corepb.TaskState_TASK_STATE_DEAD:
		if err := t.deadTasksIndex.Add(txn, t.tablePK(task.Id.AccountId, task.Id.QueueId), deadTasksIndexItem(task.LastFailedAt, task.Id.TaskId)); err != nil {
			return err
		}
	}

	return t.expiration.add(txn, task.Id, task.ExpiresAt)
}

// Get returns a task by its ID.
func (t *tasksTable) Get(txn *store.Txn, taskId *corepb.TaskId) (*corepb.Task, error) {
	return t.table.Get(txn, utils.ConcatBytes(t.tablePK(taskId.AccountId, taskId.QueueId), t.tableSK(taskId.TaskId)))
}

// set persists a task's fields without touching any secondary index. Use
// this only when the caller has already reconciled every index this task's
// state change might affect.
func (t *tasksTable) set(txn *store.Txn, task *corepb.Task) error {
	return t.table.Set(txn, utils.ConcatBytes(t.tablePK(task.Id.AccountId, task.Id.QueueId), t.tableSK(task.Id.TaskId)), task)
}

func (t *tasksTable) delete(txn *store.Txn, taskId *corepb.TaskId) error {
	return t.table.Delete(txn, utils.ConcatBytes(t.tablePK(taskId.AccountId, taskId.QueueId), t.tableSK(taskId.TaskId)))
}

// ListAll returns every task belonging to (accountId, queueId), in no
// particular guaranteed order, regardless of state.
func (t *tasksTable) ListAll(txn *store.Txn, accountId uint64, queueId uint64) ([]*corepb.Task, error) {
	return t.table.ListAll(txn, t.tablePK(accountId, queueId))
}

func (t *tasksTable) tablePK(accountId uint64, queueId uint64) []byte {
	return utils.ConcatBytes(accountId, queueId)
}

func (t *tasksTable) tableSK(taskId uint64) []byte {
	return utils.ConcatBytes(taskId)
}

func (t *tasksTable) dedupeKeysIndexPK(accountId uint64, queueId uint64, dedupeKey string) []byte {
	return utils.ConcatBytes(accountId, queueId, dedupeKey)
}

// scheduledAt is encoded as plain big-endian, so byte-lexicographic ordering
// only matches numeric ordering for scheduledAt >= 0 (two's complement makes
// negative values sort after positive ones). queueIndex's and
// threadedTasksIndex's ordering both depend on this: dequeueTasksBeforeTime's
// range scans and promoteNextThreadHead's "earliest first, since sorted"
// assumption both require scheduledAt to never be negative. In practice it
// never is, since it's always max(requested ScheduledAt, now) — never a
// zero/negative sentinel.
func queueIndexItem(scheduledAt int64, taskId uint64) []byte {
	return utils.ConcatBytes(scheduledAt, taskId)
}

// visibleAt is encoded as plain big-endian, so byte-lexicographic ordering
// only matches numeric ordering for visibleAt >= 0 (see queueIndexItem).
// inProgressIndex's range scans in dequeueInProgressTasksBeforeTime depend on
// visibleAt never being negative; in practice it never is, since it's always
// now + a positive keepalive timeout while a task is actually in this index
// (it is removed from the index, not left at visibleAt == 0, whenever a task
// leaves the in-progress state).
func inProgressIndexItem(visibleAt int64, taskId uint64) []byte {
	return utils.ConcatBytes(visibleAt, taskId)
}

// lastFailedAt is encoded as plain big-endian, so byte-lexicographic ordering
// only matches numeric ordering for lastFailedAt >= 0 (see queueIndexItem).
// deadTasksIndex is not range-scanned by anything today (only Add/Delete), so
// this isn't currently a live ordering bug, but would become one the moment
// a range scan (e.g. a future "list dead tasks by age" feature) is added; in
// practice lastFailedAt is always req.Now, never a zero/negative sentinel.
func deadTasksIndexItem(lastFailedAt int64, taskId uint64) []byte {
	return utils.ConcatBytes(lastFailedAt, taskId)
}

func itemPrefix(t int64) []byte {
	return utils.ConcatBytes(t)
}

func extractTaskIdFromIndexItem(item []byte) uint64 {
	return utils.BytesToUint64(item[len(item)-8:])
}

func extractTimeFromIndexItem(item []byte) int64 {
	return int64(utils.BytesToUint64(item[len(item)-16 : len(item)-8]))
}
