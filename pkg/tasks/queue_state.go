package tasks

import (
	"errors"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/moab/pkg/corepb"
	"github.com/evrblk/moab/pkg/sharding"
)

// queueStateTable is a table of per-queue operational state: the task id
// sequence used to assign new task ids, and the dequeuing cursors used to
// avoid rescanning already-visited entries at the head of queueIndex and
// inProgressIndex.
//
// Table Primary Key:
// 1. account id
// 2. queue id
type queueStateTable struct {
	table *honey.BinaryTable[*corepb.QueueState, corepb.QueueState]
}

func newQueueStateTable(replicaPrefix []byte) *queueStateTable {
	return &queueStateTable{
		table: honey.NewBinaryTable[*corepb.QueueState, corepb.QueueState](
			utils.ConcatBytes(replicaPrefix, tablePrefixQueueState),
		),
	}
}

// Clear deletes every queue state row.
func (t *queueStateTable) Clear(badgerStore *store.BadgerStore) error {
	return badgerStore.DeletePrefix(t.table.TableId())
}

// EachEntity streams every queue state as (canonical key, stored value).
func (t *queueStateTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// RestoreEntity decodes one streamed queue state and, if owned, inserts it.
func (t *queueStateTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds honey.ShardRange) (bool, error) {
	if len(key) != 16 {
		return false, errors.New("queue state key must be 16 bytes")
	}
	accountId := utils.BytesToUint64(key[0:8])
	queueId := utils.BytesToUint64(key[8:16])
	if !bounds.Owns(sharding.ByAccountAndQueue(accountId, queueId)) {
		return false, nil
	}

	state := &corepb.QueueState{}
	if err := state.UnmarshalBinary(value); err != nil {
		return false, err
	}
	return true, t.Set(txn, accountId, queueId, state)
}

// Get returns the operational state for (accountId, queueId), or a
// zero-value QueueState if none has been set yet (a queue starts with a task
// id sequence of 0 and unvisited dequeuing cursors).
func (t *queueStateTable) Get(txn *store.Txn, accountId uint64, queueId uint64) (*corepb.QueueState, error) {
	state, err := t.table.Get(txn, t.tablePK(accountId, queueId))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &corepb.QueueState{}, nil
		}
		return nil, err
	}
	return state, nil
}

// Set stores the operational state for (accountId, queueId), overwriting
// any previous value.
func (t *queueStateTable) Set(txn *store.Txn, accountId uint64, queueId uint64, state *corepb.QueueState) error {
	return t.table.Set(txn, t.tablePK(accountId, queueId), state)
}

func (t *queueStateTable) tablePK(accountId uint64, queueId uint64) []byte {
	return utils.ConcatBytes(accountId, queueId)
}
