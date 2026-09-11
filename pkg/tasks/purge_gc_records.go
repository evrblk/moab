package tasks

import (
	"errors"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/moab/pkg/corepb"
	"github.com/evrblk/moab/pkg/sharding"
)

// purgeGCRecordsTable stores pending garbage-collection markers for purged
// queue ids. PurgeQueue creates one of these instead of synchronously
// deleting all of a queue's tasks; RunPurgeQueueGarbageCollection drains
// them in bounded batches.
//
// Table Primary Key:
// 1. account id
// 2. queue id
type purgeGCRecordsTable struct {
	table *honey.BinaryTable[*corepb.PurgeQueueGarbageCollectionRecord, corepb.PurgeQueueGarbageCollectionRecord]
}

func newPurgeGCRecordsTable(replicaPrefix []byte) *purgeGCRecordsTable {
	return &purgeGCRecordsTable{
		table: honey.NewBinaryTable[*corepb.PurgeQueueGarbageCollectionRecord, corepb.PurgeQueueGarbageCollectionRecord](
			utils.ConcatBytes(replicaPrefix, tablePrefixPurgeGCRecords),
		),
	}
}

// Clear deletes every GC record row this table owns.
func (t *purgeGCRecordsTable) Clear(badgerStore *store.BadgerStore) error {
	return badgerStore.DeletePrefix(t.table.TableId())
}

// EachEntity streams every GC record as (canonical key, stored value).
func (t *purgeGCRecordsTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// RestoreEntity decodes one streamed GC record and, if owned, inserts it
// through Create, re-deriving its key from the record's own QueueId.
func (t *purgeGCRecordsTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds honey.ShardRange) (bool, error) {
	record := &corepb.PurgeQueueGarbageCollectionRecord{}
	if err := record.UnmarshalBinary(value); err != nil {
		return false, err
	}
	if !bounds.Owns(sharding.ByAccountAndQueue(record.QueueId.AccountId, record.QueueId.QueueId)) {
		return false, nil
	}
	return true, t.Create(txn, record)
}

// Create marks a queue id's tasks for asynchronous deletion. Callers are
// responsible for only creating one record per queue id (PurgeQueue only
// ever targets an id that was just rotated out by SwapQueueId, which by
// construction never repeats), so this is naturally the case in practice.
func (t *purgeGCRecordsTable) Create(txn *store.Txn, record *corepb.PurgeQueueGarbageCollectionRecord) error {
	return t.table.Set(txn, t.tablePK(record.QueueId.AccountId, record.QueueId.QueueId), record)
}

// Delete removes a GC record once its queue id's tasks have all been
// deleted.
func (t *purgeGCRecordsTable) Delete(txn *store.Txn, record *corepb.PurgeQueueGarbageCollectionRecord) error {
	return t.table.Delete(txn, t.tablePK(record.QueueId.AccountId, record.QueueId.QueueId))
}

// Exists reports whether (accountId, queueId) has an active purge marker —
// i.e. it was rotated out by SwapQueueId and its tasks are being (or have
// already been) asynchronously drained.
func (t *purgeGCRecordsTable) Exists(txn *store.Txn, accountId uint64, queueId uint64) (bool, error) {
	_, err := t.table.Get(txn, t.tablePK(accountId, queueId))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// List returns up to limit pending GC records, in no particular guaranteed
// order across accounts.
func (t *purgeGCRecordsTable) List(txn *store.Txn, limit int) ([]*corepb.PurgeQueueGarbageCollectionRecord, error) {
	result, err := t.table.ListPaginated(txn, nil, nil, limit)
	if err != nil {
		return nil, err
	}
	return result.Items, nil
}

func (t *purgeGCRecordsTable) tablePK(accountId uint64, queueId uint64) []byte {
	return utils.ConcatBytes(
		accountId,
		queueId,
	)
}
