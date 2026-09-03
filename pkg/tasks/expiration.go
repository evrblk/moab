package tasks

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/moab/pkg/corepb"
)

// expirationIndex is a shard-local, time-ordered index of every task
// currently in tasksTable, regardless of its state (enqueued, in progress,
// or dead). RunGarbageCollection sweeps it to delete tasks whose ExpiresAt
// has passed no matter how far they got in processing.
//
// Sort order (no primary key, like a lease expiration index):
// 1. expires at
// 2. account id
// 3. queue id
// 4. task id
type expirationIndex struct {
	index *honey.SortedIndex
}

func newExpirationIndex(replicaPrefix []byte) *expirationIndex {
	return &expirationIndex{
		index: honey.NewSortedIndex(utils.ConcatBytes(replicaPrefix, tablePrefixExpirationIndex)),
	}
}

func (e *expirationIndex) add(txn *store.Txn, taskId *corepb.TaskId, expiresAt int64) error {
	return e.index.Add(txn, e.item(expiresAt, taskId))
}

func (e *expirationIndex) remove(txn *store.Txn, taskId *corepb.TaskId, expiresAt int64) error {
	return e.index.Delete(txn, e.item(expiresAt, taskId))
}

// expiredTaskRef is one entry found by ListDue: the task id plus the
// ExpiresAt it was indexed under (needed to delete the exact index entry).
type expiredTaskRef struct {
	TaskId    *corepb.TaskId
	ExpiresAt int64
}

// ListDue returns up to limit tasks whose ExpiresAt is at or before before,
// in ascending ExpiresAt order.
func (e *expirationIndex) ListDue(txn *store.Txn, before int64, limit int) ([]expiredTaskRef, error) {
	refs := make([]expiredTaskRef, 0, limit)

	err := e.index.ListInRange(txn, e.itemPrefix(0), e.itemPrefix(before), func(item []byte) (bool, error) {
		refs = append(refs, expiredTaskRef{
			TaskId: &corepb.TaskId{
				AccountId: utils.BytesToUint64(item[8:16]),
				QueueId:   utils.BytesToUint64(item[16:24]),
				TaskId:    utils.BytesToUint64(item[24:32]),
			},
			ExpiresAt: int64(utils.BytesToUint64(item[0:8])),
		})

		return len(refs) < limit, nil
	})
	if err != nil {
		return nil, err
	}

	return refs, nil
}

// expiresAt is encoded as plain big-endian, so byte-lexicographic ordering
// only matches numeric ordering for expiresAt >= 0 (two's complement makes
// negative values sort after positive ones). ListDue's range scan (which
// starts at itemPrefix(0)) depends on expiresAt never being negative; in
// practice it never is — every producer guarantees ExpiresAt is a real,
// positive deadline, never a zero/negative sentinel (see core.go).
func (e *expirationIndex) item(expiresAt int64, taskId *corepb.TaskId) []byte {
	return utils.ConcatBytes(expiresAt, taskId.AccountId, taskId.QueueId, taskId.TaskId)
}

func (e *expirationIndex) itemPrefix(expiresAt int64) []byte {
	return utils.ConcatBytes(expiresAt)
}
