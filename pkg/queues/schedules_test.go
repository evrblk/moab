package queues

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/moab/pkg/corepb"
)

func TestSchedulesTable_Get(t *testing.T) {
	t.Run("gets an existing schedule", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		schedule := &corepb.Schedule{
			Id: &corepb.ScheduleId{
				AccountId:  rand.Uint64(),
				QueueId:    rand.Uint64(),
				ScheduleId: rand.Uint64(),
			},
			Name:            "test_schedule",
			NextScheduledAt: rand.Int64(),
		}

		txn := badgerStore.Update()
		err = table.Create(txn, schedule)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, schedule.Id)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, schedule.Id.ScheduleId, retrieved.Id.ScheduleId)
		require.Equal(t, schedule.Name, retrieved.Name)
	})

	t.Run("gets a nonexistent schedule", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		scheduleId := &corepb.ScheduleId{
			AccountId:  rand.Uint64(),
			QueueId:    rand.Uint64(),
			ScheduleId: rand.Uint64(),
		}

		txn := badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, scheduleId)
		require.Error(t, err)
		require.Nil(t, retrieved)
		require.ErrorIs(t, err, store.ErrNotFound)
	})
}

func TestSchedulesTable_GetByName(t *testing.T) {
	t.Run("gets a schedule by name", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		queueId := &corepb.QueueId{
			AccountId: rand.Uint64(),
			QueueId:   rand.Uint64(),
		}

		schedule := &corepb.Schedule{
			Id: &corepb.ScheduleId{
				AccountId:  queueId.AccountId,
				QueueId:    queueId.QueueId,
				ScheduleId: rand.Uint64(),
			},
			Name:        "test_schedule",
			Description: "test description",
		}

		txn := badgerStore.Update()
		err = table.Create(txn, schedule)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.GetByName(txn, queueId, schedule.Name)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, schedule.Id.ScheduleId, retrieved.Id.ScheduleId)
		require.Equal(t, schedule.Description, retrieved.Description)
	})

	t.Run("gets a nonexistent schedule by name", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		queueId := &corepb.QueueId{
			AccountId: rand.Uint64(),
			QueueId:   rand.Uint64(),
		}

		txn := badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.GetByName(txn, queueId, "nonexistent_schedule")
		require.Error(t, err)
		require.Nil(t, retrieved)
		require.ErrorIs(t, err, store.ErrNotFound)
	})

	t.Run("a schedule is not visible by name from a different queue", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()
		queueId1 := &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}
		queueId2 := &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}

		schedule := &corepb.Schedule{
			Id: &corepb.ScheduleId{
				AccountId:  queueId1.AccountId,
				QueueId:    queueId1.QueueId,
				ScheduleId: rand.Uint64(),
			},
			Name: "test_schedule",
		}

		txn := badgerStore.Update()
		err = table.Create(txn, schedule)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.GetByName(txn, queueId2, "test_schedule")
		require.Error(t, err)
		require.Nil(t, retrieved)
		require.ErrorIs(t, err, store.ErrNotFound)
	})
}

func TestSchedulesTable_Create(t *testing.T) {
	t.Run("creates a schedule", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		schedule := &corepb.Schedule{
			Id: &corepb.ScheduleId{
				AccountId:  rand.Uint64(),
				QueueId:    rand.Uint64(),
				ScheduleId: rand.Uint64(),
			},
			Name:            "test_schedule",
			Description:     "test description",
			Cron:            "*/5 * * * *",
			NextScheduledAt: 1000,
		}

		txn := badgerStore.Update()
		err = table.Create(txn, schedule)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, schedule.Id)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, schedule.Name, retrieved.Name)
		require.Equal(t, schedule.Description, retrieved.Description)
		require.Equal(t, schedule.Cron, retrieved.Cron)
	})

	t.Run("create adds a names index entry", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		queueId := &corepb.QueueId{AccountId: rand.Uint64(), QueueId: rand.Uint64()}
		schedule := &corepb.Schedule{
			Id: &corepb.ScheduleId{
				AccountId:  queueId.AccountId,
				QueueId:    queueId.QueueId,
				ScheduleId: rand.Uint64(),
			},
			Name: "test_schedule",
		}

		txn := badgerStore.Update()
		err = table.Create(txn, schedule)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.GetByName(txn, queueId, schedule.Name)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, schedule.Id.ScheduleId, retrieved.Id.ScheduleId)
	})

	t.Run("create adds a scheduled index entry", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		schedule := &corepb.Schedule{
			Id: &corepb.ScheduleId{
				AccountId:  rand.Uint64(),
				QueueId:    rand.Uint64(),
				ScheduleId: rand.Uint64(),
			},
			Name:            "test_schedule",
			NextScheduledAt: 1000,
		}

		txn := badgerStore.Update()
		err = table.Create(txn, schedule)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		due, err := table.ListDue(txn, 1000, 10)
		require.NoError(t, err)
		require.Len(t, due, 1)
		require.Equal(t, schedule.Id.ScheduleId, due[0].Id.ScheduleId)
	})
}

func TestSchedulesTable_Update(t *testing.T) {
	t.Run("updates a schedule", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		schedule := &corepb.Schedule{
			Id: &corepb.ScheduleId{
				AccountId:  rand.Uint64(),
				QueueId:    rand.Uint64(),
				ScheduleId: rand.Uint64(),
			},
			Name:            "test_schedule",
			Description:     "original description",
			Version:         1,
			NextScheduledAt: 1000,
		}

		txn := badgerStore.Update()
		err = table.Create(txn, schedule)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		schedule.Description = "updated description"
		schedule.Version = 2

		txn = badgerStore.Update()
		err = table.Update(txn, schedule)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, schedule.Id)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, "updated description", retrieved.Description)
		require.EqualValues(t, 2, retrieved.Version)
	})

	t.Run("update moves the scheduled index entry when NextScheduledAt changes", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		schedule := &corepb.Schedule{
			Id: &corepb.ScheduleId{
				AccountId:  rand.Uint64(),
				QueueId:    rand.Uint64(),
				ScheduleId: rand.Uint64(),
			},
			Name:            "test_schedule",
			NextScheduledAt: 1000,
		}

		txn := badgerStore.Update()
		err = table.Create(txn, schedule)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		oldNextScheduledAt := schedule.NextScheduledAt
		schedule.NextScheduledAt = 2000

		txn = badgerStore.Update()
		err = table.Update(txn, schedule)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		// The old position must no longer report this schedule as due
		dueAtOldTime, err := table.ListDue(txn, oldNextScheduledAt, 10)
		require.NoError(t, err)
		require.Len(t, dueAtOldTime, 0)

		// The new position must
		dueAtNewTime, err := table.ListDue(txn, schedule.NextScheduledAt, 10)
		require.NoError(t, err)
		require.Len(t, dueAtNewTime, 1)
		require.Equal(t, schedule.Id.ScheduleId, dueAtNewTime[0].Id.ScheduleId)
	})

	t.Run("update leaves the scheduled index alone when NextScheduledAt is unchanged", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		schedule := &corepb.Schedule{
			Id: &corepb.ScheduleId{
				AccountId:  rand.Uint64(),
				QueueId:    rand.Uint64(),
				ScheduleId: rand.Uint64(),
			},
			Name:            "test_schedule",
			Description:     "original description",
			NextScheduledAt: 1000,
		}

		txn := badgerStore.Update()
		err = table.Create(txn, schedule)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Update a different field, leaving NextScheduledAt unchanged
		schedule.Description = "updated description"

		txn = badgerStore.Update()
		err = table.Update(txn, schedule)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		// Exactly one (not zero, not duplicated) entry should still be due at that time
		due, err := table.ListDue(txn, schedule.NextScheduledAt, 10)
		require.NoError(t, err)
		require.Len(t, due, 1)
		require.Equal(t, "updated description", due[0].Description)
	})
}

func TestSchedulesTable_Delete(t *testing.T) {
	t.Run("deletes a schedule", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		queueId := &corepb.QueueId{AccountId: rand.Uint64(), QueueId: rand.Uint64()}
		schedule := &corepb.Schedule{
			Id: &corepb.ScheduleId{
				AccountId:  queueId.AccountId,
				QueueId:    queueId.QueueId,
				ScheduleId: rand.Uint64(),
			},
			Name:            "test_schedule",
			NextScheduledAt: 1000,
		}

		txn := badgerStore.Update()
		err = table.Create(txn, schedule)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.Update()
		err = table.Delete(txn, schedule)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		_, err = table.Get(txn, schedule.Id)
		require.Error(t, err)
		require.ErrorIs(t, err, store.ErrNotFound)

		// The names index entry must be gone too
		_, err = table.namesIndex.Get(txn, table.namesIndexPK(queueId.AccountId, queueId.QueueId, schedule.Name))
		require.Error(t, err)
		require.ErrorIs(t, err, store.ErrNotFound)

		retrievedByName, err := table.GetByName(txn, queueId, schedule.Name)
		require.Error(t, err)
		require.Nil(t, retrievedByName)
		require.ErrorIs(t, err, store.ErrNotFound)

		// The scheduled index entry must be gone too
		due, err := table.ListDue(txn, schedule.NextScheduledAt, 10)
		require.NoError(t, err)
		require.Len(t, due, 0)
	})

	t.Run("deletes a nonexistent schedule does not error", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		schedule := &corepb.Schedule{
			Id: &corepb.ScheduleId{
				AccountId:  rand.Uint64(),
				QueueId:    rand.Uint64(),
				ScheduleId: rand.Uint64(),
			},
			Name:            "test_schedule",
			NextScheduledAt: 1000,
		}

		txn := badgerStore.Update()
		err = table.Delete(txn, schedule)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
	})
}

func TestSchedulesTable_ListAll(t *testing.T) {
	t.Run("lists all schedules for a queue", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		queueId := &corepb.QueueId{AccountId: rand.Uint64(), QueueId: rand.Uint64()}

		numSchedules := 5
		for i := range numSchedules {
			schedule := &corepb.Schedule{
				Id: &corepb.ScheduleId{
					AccountId:  queueId.AccountId,
					QueueId:    queueId.QueueId,
					ScheduleId: uint64(i + 1),
				},
				Name: fmt.Sprintf("schedule_%d", i),
			}
			txn := badgerStore.Update()
			err := table.Create(txn, schedule)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		txn := badgerStore.View()
		defer txn.Discard()

		schedules, err := table.ListAll(txn, queueId)
		require.NoError(t, err)
		require.Len(t, schedules, numSchedules)
	})

	t.Run("schedules from different queues are isolated", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()
		queueId1 := &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}
		queueId2 := &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}

		for i := range 3 {
			schedule := &corepb.Schedule{
				Id:   &corepb.ScheduleId{AccountId: queueId1.AccountId, QueueId: queueId1.QueueId, ScheduleId: uint64(i + 1)},
				Name: fmt.Sprintf("queue1_schedule_%d", i),
			}
			txn := badgerStore.Update()
			err := table.Create(txn, schedule)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		for i := range 2 {
			schedule := &corepb.Schedule{
				Id:   &corepb.ScheduleId{AccountId: queueId2.AccountId, QueueId: queueId2.QueueId, ScheduleId: uint64(i + 1)},
				Name: fmt.Sprintf("queue2_schedule_%d", i),
			}
			txn := badgerStore.Update()
			err := table.Create(txn, schedule)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		txn := badgerStore.View()
		defer txn.Discard()

		schedules1, err := table.ListAll(txn, queueId1)
		require.NoError(t, err)
		require.Len(t, schedules1, 3)

		schedules2, err := table.ListAll(txn, queueId2)
		require.NoError(t, err)
		require.Len(t, schedules2, 2)
	})
}

func TestSchedulesTable_List(t *testing.T) {
	t.Run("lists schedules for a queue", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		queueId := &corepb.QueueId{AccountId: rand.Uint64(), QueueId: rand.Uint64()}

		numSchedules := 5
		for i := range numSchedules {
			schedule := &corepb.Schedule{
				Id:   &corepb.ScheduleId{AccountId: queueId.AccountId, QueueId: queueId.QueueId, ScheduleId: uint64(i + 1)},
				Name: fmt.Sprintf("schedule_%d", i),
			}
			txn := badgerStore.Update()
			err := table.Create(txn, schedule)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		txn := badgerStore.View()
		defer txn.Discard()

		result, err := table.List(txn, queueId, nil, 100)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Schedules, numSchedules)
		require.Nil(t, result.NextPaginationToken)
		require.Nil(t, result.PreviousPaginationToken)
	})

	t.Run("lists schedules with pagination", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		queueId := &corepb.QueueId{AccountId: rand.Uint64(), QueueId: rand.Uint64()}

		numSchedules := 10
		for i := range numSchedules {
			schedule := &corepb.Schedule{
				Id:   &corepb.ScheduleId{AccountId: queueId.AccountId, QueueId: queueId.QueueId, ScheduleId: uint64(i + 1)},
				Name: fmt.Sprintf("schedule_%02d", i),
			}
			txn := badgerStore.Update()
			err := table.Create(txn, schedule)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		txn := badgerStore.View()
		defer txn.Discard()

		page1, err := table.List(txn, queueId, nil, 3)
		require.NoError(t, err)
		require.Len(t, page1.Schedules, 3)
		require.NotNil(t, page1.NextPaginationToken)
		require.Nil(t, page1.PreviousPaginationToken)

		page2, err := table.List(txn, queueId, page1.NextPaginationToken, 3)
		require.NoError(t, err)
		require.Len(t, page2.Schedules, 3)
		require.NotNil(t, page2.NextPaginationToken)
		require.NotNil(t, page2.PreviousPaginationToken)

		for _, s1 := range page1.Schedules {
			for _, s2 := range page2.Schedules {
				require.NotEqual(t, s1.Id.ScheduleId, s2.Id.ScheduleId)
			}
		}
	})

	t.Run("the previous token returns to the exact same page", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		queueId := &corepb.QueueId{AccountId: rand.Uint64(), QueueId: rand.Uint64()}

		numSchedules := 10
		for i := range numSchedules {
			schedule := &corepb.Schedule{
				Id:   &corepb.ScheduleId{AccountId: queueId.AccountId, QueueId: queueId.QueueId, ScheduleId: uint64(i + 1)},
				Name: fmt.Sprintf("schedule_%02d", i),
			}
			txn := badgerStore.Update()
			err := table.Create(txn, schedule)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		txn := badgerStore.View()
		defer txn.Discard()

		page1, err := table.List(txn, queueId, nil, 3)
		require.NoError(t, err)
		require.Len(t, page1.Schedules, 3)

		page2, err := table.List(txn, queueId, page1.NextPaginationToken, 3)
		require.NoError(t, err)
		require.NotNil(t, page2.PreviousPaginationToken)

		back, err := table.List(txn, queueId, page2.PreviousPaginationToken, 3)
		require.NoError(t, err)
		require.Len(t, back.Schedules, len(page1.Schedules))
		for i := range page1.Schedules {
			require.Equal(t, page1.Schedules[i].Id.ScheduleId, back.Schedules[i].Id.ScheduleId)
		}
	})

	t.Run("lists an empty queue", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		queueId := &corepb.QueueId{AccountId: rand.Uint64(), QueueId: rand.Uint64()}

		txn := badgerStore.View()
		defer txn.Discard()

		result, err := table.List(txn, queueId, nil, 100)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Schedules, 0)
		require.Nil(t, result.NextPaginationToken)
		require.Nil(t, result.PreviousPaginationToken)
	})

	t.Run("schedules from different queues are isolated", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()
		queueId1 := &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}
		queueId2 := &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}

		for i := range 3 {
			schedule := &corepb.Schedule{
				Id:   &corepb.ScheduleId{AccountId: queueId1.AccountId, QueueId: queueId1.QueueId, ScheduleId: uint64(i + 1)},
				Name: fmt.Sprintf("queue1_schedule_%d", i),
			}
			txn := badgerStore.Update()
			err := table.Create(txn, schedule)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		for i := range 5 {
			schedule := &corepb.Schedule{
				Id:   &corepb.ScheduleId{AccountId: queueId2.AccountId, QueueId: queueId2.QueueId, ScheduleId: uint64(i + 1)},
				Name: fmt.Sprintf("queue2_schedule_%d", i),
			}
			txn := badgerStore.Update()
			err := table.Create(txn, schedule)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		txn := badgerStore.View()
		defer txn.Discard()

		result1, err := table.List(txn, queueId1, nil, 100)
		require.NoError(t, err)
		require.Len(t, result1.Schedules, 3)

		result2, err := table.List(txn, queueId2, nil, 100)
		require.NoError(t, err)
		require.Len(t, result2.Schedules, 5)
	})
}

func TestSchedulesTable_ListDue(t *testing.T) {
	t.Run("returns schedules due before a given time", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		due := &corepb.Schedule{
			Id:              &corepb.ScheduleId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), ScheduleId: rand.Uint64()},
			Name:            "due_schedule",
			NextScheduledAt: 1000,
		}
		notYetDue := &corepb.Schedule{
			Id:              &corepb.ScheduleId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), ScheduleId: rand.Uint64()},
			Name:            "not_yet_due_schedule",
			NextScheduledAt: 5000,
		}

		txn := badgerStore.Update()
		require.NoError(t, table.Create(txn, due))
		require.NoError(t, table.Create(txn, notYetDue))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		result, err := table.ListDue(txn, 2000, 10)
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Equal(t, due.Id.ScheduleId, result[0].Id.ScheduleId)
	})

	t.Run("a schedule due exactly at the cutoff is included", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		schedule := &corepb.Schedule{
			Id:              &corepb.ScheduleId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), ScheduleId: rand.Uint64()},
			Name:            "boundary_schedule",
			NextScheduledAt: 1000,
		}

		txn := badgerStore.Update()
		require.NoError(t, table.Create(txn, schedule))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		// "at or before": before == NextScheduledAt must include it
		result, err := table.ListDue(txn, 1000, 10)
		require.NoError(t, err)
		require.Len(t, result, 1)

		// One nanosecond earlier must not
		result, err = table.ListDue(txn, 999, 10)
		require.NoError(t, err)
		require.Len(t, result, 0)
	})

	t.Run("respects the limit", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		txn := badgerStore.Update()
		for i := range 5 {
			schedule := &corepb.Schedule{
				Id:              &corepb.ScheduleId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), ScheduleId: rand.Uint64()},
				Name:            fmt.Sprintf("schedule_%d", i),
				NextScheduledAt: int64(1000 + i),
			}
			require.NoError(t, table.Create(txn, schedule))
		}
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		result, err := table.ListDue(txn, 10000, 3)
		require.NoError(t, err)
		require.Len(t, result, 3)
	})

	t.Run("aggregates due schedules across queues", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		queueId1 := &corepb.QueueId{AccountId: rand.Uint64(), QueueId: rand.Uint64()}
		queueId2 := &corepb.QueueId{AccountId: rand.Uint64(), QueueId: rand.Uint64()}

		schedule1 := &corepb.Schedule{
			Id:              &corepb.ScheduleId{AccountId: queueId1.AccountId, QueueId: queueId1.QueueId, ScheduleId: rand.Uint64()},
			Name:            "schedule_1",
			NextScheduledAt: 1000,
		}
		schedule2 := &corepb.Schedule{
			Id:              &corepb.ScheduleId{AccountId: queueId2.AccountId, QueueId: queueId2.QueueId, ScheduleId: rand.Uint64()},
			Name:            "schedule_2",
			NextScheduledAt: 2000,
		}

		txn := badgerStore.Update()
		require.NoError(t, table.Create(txn, schedule1))
		require.NoError(t, table.Create(txn, schedule2))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		result, err := table.ListDue(txn, 3000, 10)
		require.NoError(t, err)
		require.Len(t, result, 2)
	})

	t.Run("returns nothing when no schedules are due", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newSchedulesTable([]byte{0x77, 0x77, 0x77, 0x77})

		schedule := &corepb.Schedule{
			Id:              &corepb.ScheduleId{AccountId: rand.Uint64(), QueueId: rand.Uint64(), ScheduleId: rand.Uint64()},
			Name:            "future_schedule",
			NextScheduledAt: 5000,
		}

		txn := badgerStore.Update()
		require.NoError(t, table.Create(txn, schedule))
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		result, err := table.ListDue(txn, 1000, 10)
		require.NoError(t, err)
		require.Len(t, result, 0)
	})
}
