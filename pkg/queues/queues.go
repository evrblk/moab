package queues

import (
	"errors"
	"slices"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/moab/pkg/corepb"
	"github.com/evrblk/moab/pkg/pagination"
	"github.com/evrblk/moab/pkg/sharding"
)

// queuesTable is a table of queues indexed by queue name.
//
// Table Primary Key:
// 1. account id
//
// Table Sort Key:
// 1. queue id
//
// Names Index Primary Key:
// 1. account id
// 2. queue name
type queuesTable struct {
	table      *honey.BinaryTable[*corepb.Queue, corepb.Queue]
	namesIndex *honey.Uint64Table
}

func newQueuesTable(replicaPrefix []byte) *queuesTable {
	return &queuesTable{
		table: honey.NewBinaryTable[*corepb.Queue, corepb.Queue](
			utils.ConcatBytes(replicaPrefix, tablePrefixQueues),
		),
		namesIndex: honey.NewUint64Table(
			utils.ConcatBytes(replicaPrefix, tablePrefixQueuesNamesIndex),
		),
	}
}

// Clear deletes every row this table owns.
func (t *queuesTable) Clear(badgerStore *store.BadgerStore) error {
	for _, prefix := range [][]byte{t.table.TableId(), t.namesIndex.TableId()} {
		if err := badgerStore.DeletePrefix(prefix); err != nil {
			return err
		}
	}
	return nil
}

// EachEntity streams every queue as (canonical key, stored value) — the
// primary table only; the names index is rebuilt from the queues on restore.
func (t *queuesTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// RestoreEntity decodes one streamed queue and, if owned, inserts it
// through Create re-deriving its keys.
func (t *queuesTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds honey.ShardRange) (bool, error) {
	queue := &corepb.Queue{}
	if err := queue.UnmarshalBinary(value); err != nil {
		return false, err
	}
	if !bounds.Owns(sharding.ByAccount(queue.Id.AccountId)) {
		return false, nil
	}
	return true, t.Create(txn, queue)
}

// Get returns a queue by its ID.
func (t *queuesTable) Get(txn *store.Txn, queueId *corepb.QueueId) (*corepb.Queue, error) {
	return t.table.Get(txn,
		utils.ConcatBytes(
			t.tablePK(queueId.AccountId),
			t.tableSK(queueId.QueueId)))
}

// GetByName returns a queue by account ID and queue name, resolving the name
// to a queue ID through the names index and then delegating to Get.
func (t *queuesTable) GetByName(txn *store.Txn, accountId uint64, queueName string) (*corepb.Queue, error) {
	queueId, err := t.namesIndex.Get(txn, t.namesIndexPK(accountId, queueName))
	if err != nil {
		return nil, err
	}

	return t.Get(txn, &corepb.QueueId{
		AccountId: accountId,
		QueueId:   queueId,
	})
}

// listQueuesResult is the result of a paginated List call: a page of queues
// plus tokens for fetching the adjacent pages.
type listQueuesResult struct {
	Queues                  []*corepb.Queue
	NextPaginationToken     *corepb.PaginationToken
	PreviousPaginationToken *corepb.PaginationToken
}

// List returns a page of up to limit queues for accountId, ordered by queue
// ID, continuing from paginationToken if provided.
func (t *queuesTable) List(txn *store.Txn, accountId uint64, paginationToken *corepb.PaginationToken, limit int) (*listQueuesResult, error) {
	monsteraToken := pagination.CoreToMonstera(paginationToken)

	result, err := t.table.ListPaginated(txn, t.tablePK(accountId), monsteraToken, limit)
	if err != nil {
		return nil, err
	}

	// A page fetched by walking backward (via PreviousPaginationToken) comes
	// back from the underlying store in descending key order; reverse it so
	// every page, forward or backward, is returned in the same ascending
	// order a caller of List would expect.
	queues := result.Items
	if monsteraToken != nil && monsteraToken.Reverse {
		slices.Reverse(queues)
	}

	return &listQueuesResult{
		Queues:                  queues,
		NextPaginationToken:     pagination.MonsteraToCore(result.NextPaginationToken),
		PreviousPaginationToken: pagination.MonsteraToCore(result.PreviousPaginationToken),
	}, nil
}

// Create inserts a new queue row and its names-index entry. Callers are
// responsible for checking that the queue's ID and name are not already in
// use, since Create itself will silently overwrite a colliding row.
func (t *queuesTable) Create(txn *store.Txn, queue *corepb.Queue) error {
	err := t.namesIndex.Set(txn, t.namesIndexPK(queue.Id.AccountId, queue.Name), queue.Id.QueueId)
	if err != nil {
		return err
	}

	return t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(queue.Id.AccountId),
			t.tableSK(queue.Id.QueueId)),
		queue)
}

// Update overwrites an existing queue row in place. The queue's name (and
// therefore its names-index entry) is expected not to change between Get and
// Update; renaming a queue via Update will leave a stale names-index entry.
func (t *queuesTable) Update(txn *store.Txn, queue *corepb.Queue) error {
	return t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(queue.Id.AccountId),
			t.tableSK(queue.Id.QueueId)),
		queue)
}

// Delete removes a queue row and its names-index entry. Deleting a queue
// that no longer exists in one of the two places is not an error.
func (t *queuesTable) Delete(txn *store.Txn, queue *corepb.Queue) error {
	err := t.table.Delete(txn,
		utils.ConcatBytes(
			t.tablePK(queue.Id.AccountId),
			t.tableSK(queue.Id.QueueId)))
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}

	err = t.namesIndex.Delete(txn, t.namesIndexPK(queue.Id.AccountId, queue.Name))
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}

	return nil
}

func (t *queuesTable) tablePK(accountId uint64) []byte {
	return utils.ConcatBytes(
		accountId,
	)
}

func (t *queuesTable) tableSK(queueId uint64) []byte {
	return utils.ConcatBytes(
		queueId,
	)
}

func (t *queuesTable) namesIndexPK(accountId uint64, queueName string) []byte {
	return utils.ConcatBytes(
		accountId,
		queueName,
	)
}
