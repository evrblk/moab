package tasks

import (
	"errors"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/moab/pkg/corepb"
	"github.com/evrblk/moab/pkg/sharding"
)

// rateLimitersTable is a table of per-queue token bucket state, used when a
// queue's DequeuingSettings.RateLimiting is set.
//
// Table Primary Key:
// 1. account id
// 2. queue id
type rateLimitersTable struct {
	table *honey.BinaryTable[*corepb.RateLimiterState, corepb.RateLimiterState]
}

func newRateLimitersTable(replicaPrefix []byte) *rateLimitersTable {
	return &rateLimitersTable{
		table: honey.NewBinaryTable[*corepb.RateLimiterState, corepb.RateLimiterState](
			utils.ConcatBytes(replicaPrefix, tablePrefixRateLimiters),
		),
	}
}

// Clear deletes every rate limiter row.
func (t *rateLimitersTable) Clear(badgerStore *store.BadgerStore) error {
	return badgerStore.DeletePrefix(t.table.TableId())
}

// EachEntity streams every rate limiter state as (canonical key, stored value).
func (t *rateLimitersTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// RestoreEntity decodes one streamed rate limiter state and, if owned, inserts it.
func (t *rateLimitersTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds honey.ShardRange) (bool, error) {
	if len(key) != 16 {
		return false, errors.New("rate limiter key must be 16 bytes")
	}
	accountId := utils.BytesToUint64(key[0:8])
	queueId := utils.BytesToUint64(key[8:16])
	if !bounds.Owns(sharding.ByAccountAndQueue(accountId, queueId)) {
		return false, nil
	}

	state := &corepb.RateLimiterState{}
	if err := state.UnmarshalBinary(value); err != nil {
		return false, err
	}
	return true, t.Set(txn, accountId, queueId, state)
}

// GetOrDefault returns the current bucket, or a full bucket refilled as of
// now if this queue has never dequeued before.
func (t *rateLimitersTable) GetOrDefault(txn *store.Txn, accountId uint64, queueId uint64, rateLimiting *corepb.TokenBucketRateLimiting, now int64) (*corepb.RateLimiterState, error) {
	state, err := t.table.Get(txn, t.tablePK(accountId, queueId))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &corepb.RateLimiterState{
				Tokens:         rateLimiting.MaxTokens,
				LastRefilledAt: now,
			}, nil
		}
		return nil, err
	}
	return state, nil
}

// Set stores the token bucket state for (accountId, queueId), overwriting
// any previous value.
func (t *rateLimitersTable) Set(txn *store.Txn, accountId uint64, queueId uint64, state *corepb.RateLimiterState) error {
	return t.table.Set(txn, t.tablePK(accountId, queueId), state)
}

// Delete removes the token bucket row for (accountId, queueId). Deleting a
// row that does not exist is not an error.
func (t *rateLimitersTable) Delete(txn *store.Txn, accountId uint64, queueId uint64) error {
	err := t.table.Delete(txn, t.tablePK(accountId, queueId))
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	return nil
}

func (t *rateLimitersTable) tablePK(accountId uint64, queueId uint64) []byte {
	return utils.ConcatBytes(accountId, queueId)
}
