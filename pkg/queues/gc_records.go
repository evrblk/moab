package queues

import (
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/moab/pkg/corepb"
	"github.com/evrblk/moab/pkg/sharding"
)

// gcRecordsTable stores pending garbage-collection markers for deleted
// queues. DeleteQueue creates one of these instead of synchronously deleting
// all of a queue's schedules; RunQueuesGarbageCollection drains them in
// bounded batches.
//
// Table Primary Key:
// 1. account id
// 2. queue id
type gcRecordsTable struct {
	table *honey.BinaryTable[*corepb.QueuesGarbageCollectionRecord, corepb.QueuesGarbageCollectionRecord]
}

func newGCRecordsTable(replicaPrefix []byte) *gcRecordsTable {
	return &gcRecordsTable{
		table: honey.NewBinaryTable[*corepb.QueuesGarbageCollectionRecord, corepb.QueuesGarbageCollectionRecord](
			utils.ConcatBytes(replicaPrefix, tablePrefixGCRecords),
		),
	}
}

// Clear deletes every GC record row this table owns.
func (t *gcRecordsTable) Clear(badgerStore *store.BadgerStore) error {
	return badgerStore.DeletePrefix(t.table.TableId())
}

// EachEntity streams every GC record as (canonical key, stored value).
func (t *gcRecordsTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// RestoreEntity decodes one streamed GC record and, if owned, inserts it
// through Create, re-deriving its key from the record's own QueueId.
func (t *gcRecordsTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds honey.ShardRange) (bool, error) {
	record := &corepb.QueuesGarbageCollectionRecord{}
	if err := record.UnmarshalBinary(value); err != nil {
		return false, err
	}
	if !bounds.Owns(sharding.ByAccount(record.QueueId.AccountId)) {
		return false, nil
	}
	return true, t.Create(txn, record)
}

// Create marks a queue's schedules for asynchronous deletion. Callers are
// responsible for only creating one record per queue (DeleteQueue can only
// delete a given queue once, so this is naturally the case in practice).
func (t *gcRecordsTable) Create(txn *store.Txn, record *corepb.QueuesGarbageCollectionRecord) error {
	return t.table.Set(txn, t.tablePK(record.QueueId.AccountId, record.QueueId.QueueId), record)
}

// Delete removes a GC record once its queue's schedules have all been
// deleted.
func (t *gcRecordsTable) Delete(txn *store.Txn, record *corepb.QueuesGarbageCollectionRecord) error {
	return t.table.Delete(txn, t.tablePK(record.QueueId.AccountId, record.QueueId.QueueId))
}

// List returns up to limit pending GC records, in no particular guaranteed
// order across accounts.
func (t *gcRecordsTable) List(txn *store.Txn, limit int) ([]*corepb.QueuesGarbageCollectionRecord, error) {
	result, err := t.table.ListPaginated(txn, nil, nil, limit)
	if err != nil {
		return nil, err
	}
	return result.Items, nil
}

func (t *gcRecordsTable) tablePK(accountId uint64, queueId uint64) []byte {
	return utils.ConcatBytes(
		accountId,
		queueId,
	)
}
