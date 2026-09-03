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

// schedulesTable is a table of schedules indexed by schedule id and schedule name.
//
// Table Primary Key:
// 1. account id
// 2. queue id
//
// Table Sort Key:
// 1. schedule id
//
// Names Index Primary Key:
// 1. account id
// 2. queue id
// 3. schedule name
//
// Scheduled Index Primary Key:
// 1. timestamp
// 2. account id
// 3. queue Id
// 4. schedule id
type schedulesTable struct {
	table          *honey.BinaryTable[*corepb.Schedule, corepb.Schedule]
	namesIndex     *honey.Uint64Table
	scheduledIndex *honey.SortedIndex
}

func newSchedulesTable(replicaPrefix []byte) *schedulesTable {
	return &schedulesTable{
		table: honey.NewBinaryTable[*corepb.Schedule, corepb.Schedule](
			utils.ConcatBytes(replicaPrefix, tablePrefixSchedules),
		),
		namesIndex: honey.NewUint64Table(
			utils.ConcatBytes(replicaPrefix, tablePrefixSchedulesNamesIndex),
		),
		scheduledIndex: honey.NewSortedIndex(
			utils.ConcatBytes(replicaPrefix, tablePrefixSchedulesScheduledIndex),
		),
	}
}

// Clear deletes every row this table owns.
func (t *schedulesTable) Clear(badgerStore *store.BadgerStore) error {
	for _, prefix := range [][]byte{t.table.TableId(), t.namesIndex.TableId(), t.scheduledIndex.TableId()} {
		if err := badgerStore.DeletePrefix(prefix); err != nil {
			return err
		}
	}
	return nil
}

// EachEntity streams every queue as (canonical key, stored value) — the
// primary table only; the names index is rebuilt from the queues on restore.
func (t *schedulesTable) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.table.EachEntry(txn, fn)
}

// RestoreEntity decodes one streamed schedule and, if owned, inserts it
// through Create re-deriving its keys.
func (t *schedulesTable) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds honey.ShardRange) (bool, error) {
	schedule := &corepb.Schedule{}
	if err := schedule.UnmarshalBinary(value); err != nil {
		return false, err
	}
	if !bounds.Owns(sharding.ByAccount(schedule.Id.AccountId)) {
		return false, nil
	}
	return true, t.Create(txn, schedule)
}

// Get returns a schedule by its ID.
func (t *schedulesTable) Get(txn *store.Txn, scheduleId *corepb.ScheduleId) (*corepb.Schedule, error) {
	return t.table.Get(txn,
		utils.ConcatBytes(
			t.tablePK(scheduleId.AccountId, scheduleId.QueueId),
			t.tableSK(scheduleId.ScheduleId)))
}

// GetByName returns a schedule by queue ID and schedule name, resolving the
// name to a schedule ID through the names index and then delegating to Get.
func (t *schedulesTable) GetByName(txn *store.Txn, queueId *corepb.QueueId, scheduleName string) (*corepb.Schedule, error) {
	scheduleId, err := t.namesIndex.Get(txn, t.namesIndexPK(queueId.AccountId, queueId.QueueId, scheduleName))
	if err != nil {
		return nil, err
	}

	return t.Get(txn, &corepb.ScheduleId{
		AccountId:  queueId.AccountId,
		QueueId:    queueId.QueueId,
		ScheduleId: scheduleId,
	})
}

// ListAll returns every schedule belonging to the given queue, in no
// particular guaranteed order.
func (t *schedulesTable) ListAll(txn *store.Txn, queueId *corepb.QueueId) ([]*corepb.Schedule, error) {
	return t.table.ListAll(txn, t.tablePK(queueId.AccountId, queueId.QueueId))
}

// listSchedulesResult is the result of a paginated List call: a page of
// schedules plus tokens for fetching the adjacent pages.
type listSchedulesResult struct {
	Schedules               []*corepb.Schedule
	NextPaginationToken     *corepb.PaginationToken
	PreviousPaginationToken *corepb.PaginationToken
}

// List returns a page of up to limit schedules belonging to the given queue,
// ordered by schedule ID, continuing from paginationToken if provided.
func (t *schedulesTable) List(txn *store.Txn, queueId *corepb.QueueId, paginationToken *corepb.PaginationToken, limit int) (*listSchedulesResult, error) {
	monsteraToken := pagination.CoreToMonstera(paginationToken)

	result, err := t.table.ListPaginated(txn, t.tablePK(queueId.AccountId, queueId.QueueId), monsteraToken, limit)
	if err != nil {
		return nil, err
	}

	// A page fetched by walking backward (via PreviousPaginationToken) comes
	// back from the underlying store in descending key order; reverse it so
	// every page, forward or backward, is returned in the same ascending
	// order a caller of List would expect.
	schedules := result.Items
	if monsteraToken != nil && monsteraToken.Reverse {
		slices.Reverse(schedules)
	}

	return &listSchedulesResult{
		Schedules:               schedules,
		NextPaginationToken:     pagination.MonsteraToCore(result.NextPaginationToken),
		PreviousPaginationToken: pagination.MonsteraToCore(result.PreviousPaginationToken),
	}, nil
}

// Create inserts a new schedule row along with its names-index and
// scheduled-index entries. Callers are responsible for checking that the
// schedule's ID and name are not already in use, since Create itself will
// silently overwrite a colliding row.
func (t *schedulesTable) Create(txn *store.Txn, schedule *corepb.Schedule) error {
	// Add to index of all schedules by NextScheduledAt time
	err := t.scheduledIndex.Add(txn,
		t.scheduledIndexPK(schedule.NextScheduledAt, schedule.Id.AccountId, schedule.Id.QueueId, schedule.Id.ScheduleId))
	if err != nil {
		return err
	}

	// Add to names index
	err = t.namesIndex.Set(txn,
		t.namesIndexPK(schedule.Id.AccountId, schedule.Id.QueueId, schedule.Name),
		schedule.Id.ScheduleId)
	if err != nil {
		return err
	}

	// Add schedule to main table
	return t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(schedule.Id.AccountId, schedule.Id.QueueId),
			t.tableSK(schedule.Id.ScheduleId)),
		schedule)
}

// Update overwrites an existing schedule row in place, moving its
// scheduled-index entry to schedule.NextScheduledAt if it changed relative
// to the currently stored row. The schedule's name is expected not to change
// between Get and Update; renaming a schedule via Update will leave a stale
// names-index entry.
func (t *schedulesTable) Update(txn *store.Txn, schedule *corepb.Schedule) error {
	// The previous NextScheduledAt is read from the currently stored row
	// rather than trusted from the caller, so a caller that mutated schedule
	// in place before calling Update (the only way this is ever called)
	// can't accidentally pass the wrong "old" value and silently corrupt the
	// scheduled index.
	existing, err := t.Get(txn, schedule.Id)
	if err != nil {
		return err
	}
	oldNextScheduledAt := existing.NextScheduledAt

	// Update scheduledIndex only if NextScheduledAt changed after update
	if schedule.NextScheduledAt != oldNextScheduledAt {
		// Remove from index of all schedules at oldNextScheduledAt position
		err := t.scheduledIndex.Delete(txn,
			t.scheduledIndexPK(oldNextScheduledAt, schedule.Id.AccountId, schedule.Id.QueueId, schedule.Id.ScheduleId))
		if err != nil {
			return err
		}

		// Add to index of all schedules by new NextScheduledAt time
		err = t.scheduledIndex.Add(txn,
			t.scheduledIndexPK(schedule.NextScheduledAt, schedule.Id.AccountId, schedule.Id.QueueId, schedule.Id.ScheduleId))
		if err != nil {
			return err
		}
	}

	return t.table.Set(txn,
		utils.ConcatBytes(
			t.tablePK(schedule.Id.AccountId, schedule.Id.QueueId),
			t.tableSK(schedule.Id.ScheduleId)),
		schedule)
}

// ListDue returns up to limit schedules whose NextScheduledAt is at or before
// the given time, in ascending NextScheduledAt order.
func (t *schedulesTable) ListDue(txn *store.Txn, before int64, limit int) ([]*corepb.Schedule, error) {
	schedules := make([]*corepb.Schedule, 0, limit)

	err := t.scheduledIndex.ListInRange(txn, t.scheduledIndexPKPrefix(0), t.scheduledIndexPKPrefix(before), func(item []byte) (bool, error) {
		// item is [timestamp(8)][accountId(8)][queueId(8)][scheduleId(8)]
		accountId := utils.BytesToUint64(item[8:16])
		queueId := utils.BytesToUint64(item[16:24])
		scheduleId := utils.BytesToUint64(item[24:32])

		schedule, err := t.Get(txn, &corepb.ScheduleId{
			AccountId:  accountId,
			QueueId:    queueId,
			ScheduleId: scheduleId,
		})
		if err != nil {
			// Any error, including NotFound (meaning the index is corrupted), should stop the scan.
			return false, err
		}

		schedules = append(schedules, schedule)

		return len(schedules) < limit, nil
	})
	if err != nil {
		return nil, err
	}

	return schedules, nil
}

// Delete removes a schedule row along with its names-index and
// scheduled-index entries. Deleting a schedule that no longer exists in one
// of those places is not an error.
func (t *schedulesTable) Delete(txn *store.Txn, schedule *corepb.Schedule) error {
	// Remove from index of all schedules at NextScheduledAt position
	err := t.scheduledIndex.Delete(txn,
		t.scheduledIndexPK(schedule.NextScheduledAt, schedule.Id.AccountId, schedule.Id.QueueId, schedule.Id.ScheduleId))
	if err != nil {
		return err
	}

	// Remove from names index
	err = t.namesIndex.Delete(txn,
		t.namesIndexPK(schedule.Id.AccountId, schedule.Id.QueueId, schedule.Name))
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}

	// Remove schedule from main table
	err = t.table.Delete(txn,
		utils.ConcatBytes(
			t.tablePK(schedule.Id.AccountId, schedule.Id.QueueId),
			t.tableSK(schedule.Id.ScheduleId)))
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}

	return nil
}

func (t *schedulesTable) tablePK(accountId uint64, queueId uint64) []byte {
	return utils.ConcatBytes(
		accountId,
		queueId,
	)
}

func (t *schedulesTable) tableSK(scheduleId uint64) []byte {
	return utils.ConcatBytes(
		scheduleId,
	)
}

func (t *schedulesTable) namesIndexPK(accountId uint64, queueId uint64, scheduleName string) []byte {
	return utils.ConcatBytes(
		accountId,
		queueId,
		scheduleName,
	)
}

// time is encoded as plain big-endian, so byte-lexicographic ordering only
// matches numeric ordering for time >= 0 (two's complement makes negative
// values sort after positive ones). time must never be <= 0 for this index's
// range scans (ListDue starts at scheduledIndexPKPrefix(0)) to stay correct;
// in practice it never is, since it's always derived from real wall-clock
// NextScheduledAt values, never from a zero/negative one.
func (t *schedulesTable) scheduledIndexPKPrefix(time int64) []byte {
	return utils.ConcatBytes(
		time,
	)
}

func (t *schedulesTable) scheduledIndexPK(time int64, accountId uint64, queueId uint64, scheduleId uint64) []byte {
	return utils.ConcatBytes(
		time,
		accountId,
		queueId,
		scheduleId,
	)
}
