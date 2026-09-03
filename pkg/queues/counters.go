package queues

import (
	"errors"
	"fmt"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/moab/pkg/corepb"
	"github.com/evrblk/moab/pkg/sharding"
)

// countersTable is a table of queues counters indexed by account ID.
//
// Table Primary Key:
// 1. account id
type countersTable struct {
	table *honey.BinaryTable[*corepb.QueuesCounter, corepb.QueuesCounter]
}

// newCountersTable scopes the table under the shard-unique prefix; see
// newNamespacesTable.
func newCountersTable(replicaPrefix []byte) *countersTable {
	return &countersTable{
		table: honey.NewBinaryTable[*corepb.QueuesCounter, corepb.QueuesCounter](
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
// canonical key, whose fixed-width layout this table defines (see tablePK):
// <8-byte account id>. The namespaces keyspace is sharded by account, so the
// namespace part of the ownership predicate is zero.
func (t *countersTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds honey.ShardRange) (bool, error) {
	if len(key) != 8 {
		return false, fmt.Errorf("namespaces counter key has %d bytes, want 8", len(key))
	}
	accountId := utils.BytesToUint64(key[0:8])
	if !bounds.Owns(sharding.ByAccount(accountId)) {
		return false, nil
	}

	counter := &corepb.QueuesCounter{}
	if err := counter.UnmarshalBinary(value); err != nil {
		return false, err
	}
	return true, t.Set(txn, accountId, counter)
}

// Get returns the counters for accountId, or a zero-value QueuesCounter if
// none has been set yet (accounts start uncounted rather than erroring).
func (t *countersTable) Get(txn *store.Txn, accountId uint64) (*corepb.QueuesCounter, error) {
	counters, err := t.table.Get(txn, t.tablePK(accountId))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &corepb.QueuesCounter{}, nil
		}
		return nil, err
	}
	return counters, nil
}

// Set stores counters for accountId, overwriting any previous value.
func (t *countersTable) Set(txn *store.Txn, accountId uint64, counters *corepb.QueuesCounter) error {
	return t.table.Set(txn, t.tablePK(accountId), counters)
}

func (t *countersTable) tablePK(accountId uint64) []byte {
	return utils.ConcatBytes(
		accountId,
	)
}
