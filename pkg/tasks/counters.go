package tasks

import (
	"errors"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/moab/pkg/corepb"
	"github.com/evrblk/moab/pkg/sharding"
)

// countersTable is a table of task counters indexed by account id and queue
// id (unlike pkg/queues' countersTable, which is per account: tasks live
// under a queue, and every dequeuing decision needs a fast per-queue read).
//
// Table Primary Key:
// 1. account id
// 2. queue id
type countersTable struct {
	table *honey.BinaryTable[*corepb.TasksCounter, corepb.TasksCounter]
}

func newCountersTable(replicaPrefix []byte) *countersTable {
	return &countersTable{
		table: honey.NewBinaryTable[*corepb.TasksCounter, corepb.TasksCounter](
			utils.ConcatBytes(replicaPrefix, tablePrefixCounters),
		),
	}
}

// Clear deletes every counter row.
func (t *countersTable) Clear(badgerStore *store.BadgerStore) error {
	return badgerStore.DeletePrefix(t.table.TableId())
}

// EachEntity streams every counter as (canonical key, stored value).
func (t *countersTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// RestoreEntity decodes one streamed counter and, if owned, inserts it. A
// counter value carries no identity of its own — the identity lives in the
// canonical key: <8-byte account id><8-byte queue id>.
func (t *countersTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds honey.ShardRange) (bool, error) {
	if len(key) != 16 {
		return false, errors.New("tasks counter key must be 16 bytes")
	}
	accountId := utils.BytesToUint64(key[0:8])
	queueId := utils.BytesToUint64(key[8:16])
	if !bounds.Owns(sharding.ByAccountAndQueue(accountId, queueId)) {
		return false, nil
	}

	counter := &corepb.TasksCounter{}
	if err := counter.UnmarshalBinary(value); err != nil {
		return false, err
	}
	return true, t.Set(txn, accountId, queueId, counter)
}

// Get returns the counters for (accountId, queueId), or a zero-value
// TasksCounter if none has been set yet (queues start uncounted rather than
// erroring).
func (t *countersTable) Get(txn *store.Txn, accountId uint64, queueId uint64) (*corepb.TasksCounter, error) {
	counters, err := t.table.Get(txn, t.tablePK(accountId, queueId))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &corepb.TasksCounter{}, nil
		}
		return nil, err
	}
	return counters, nil
}

// Set stores counters for (accountId, queueId), overwriting any previous
// value.
func (t *countersTable) Set(txn *store.Txn, accountId uint64, queueId uint64, counters *corepb.TasksCounter) error {
	return t.table.Set(txn, t.tablePK(accountId, queueId), counters)
}

// Delete removes the counters row for (accountId, queueId). Deleting a row
// that does not exist is not an error.
func (t *countersTable) Delete(txn *store.Txn, accountId uint64, queueId uint64) error {
	err := t.table.Delete(txn, t.tablePK(accountId, queueId))
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	return nil
}

func (t *countersTable) tablePK(accountId uint64, queueId uint64) []byte {
	return utils.ConcatBytes(accountId, queueId)
}
