package queues

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/moab/pkg/corepb"
)

func TestQueuesTable_Create(t *testing.T) {
	t.Run("creates a queue", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueuesTable([]byte{0x77, 0x77, 0x77, 0x77})

		queue := &corepb.Queue{
			Id: &corepb.QueueId{
				AccountId: rand.Uint64(),
				QueueId:   rand.Uint64(),
			},
			Name:                      "test_queue",
			Description:               "test description",
			CreatedAt:                 rand.Int64(),
			UpdatedAt:                 rand.Int64(),
			Version:                   1,
			KeepaliveTimeoutInSeconds: 15,
			ExpiresInSeconds:          86400,
		}

		txn := badgerStore.Update()
		err = table.Create(txn, queue)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify queue was created
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, queue.Id)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, queue.Name, retrieved.Name)
		require.Equal(t, queue.Description, retrieved.Description)
		require.Equal(t, queue.CreatedAt, retrieved.CreatedAt)
		require.Equal(t, queue.UpdatedAt, retrieved.UpdatedAt)
		require.EqualValues(t, 15, retrieved.KeepaliveTimeoutInSeconds)
		require.EqualValues(t, 86400, retrieved.ExpiresInSeconds)
	})

	t.Run("create adds a names index entry", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueuesTable([]byte{0x77, 0x77, 0x77, 0x77})

		queue := &corepb.Queue{
			Id: &corepb.QueueId{
				AccountId: rand.Uint64(),
				QueueId:   rand.Uint64(),
			},
			Name: "test_queue",
		}

		txn := badgerStore.Update()
		err = table.Create(txn, queue)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Verify queue can be retrieved by name
		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.GetByName(txn, queue.Id.AccountId, queue.Name)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, queue.Id.QueueId, retrieved.Id.QueueId)
		require.Equal(t, queue.Name, retrieved.Name)
	})
}

func TestQueuesTable_Get(t *testing.T) {
	t.Run("gets an existing queue", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueuesTable([]byte{0x77, 0x77, 0x77, 0x77})

		queue := &corepb.Queue{
			Id: &corepb.QueueId{
				AccountId: rand.Uint64(),
				QueueId:   rand.Uint64(),
			},
			Name: "test_queue",
		}

		txn := badgerStore.Update()
		err = table.Create(txn, queue)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, queue.Id)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, queue.Id.AccountId, retrieved.Id.AccountId)
		require.Equal(t, queue.Id.QueueId, retrieved.Id.QueueId)
	})

	t.Run("gets a nonexistent queue", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueuesTable([]byte{0x77, 0x77, 0x77, 0x77})

		queueId := &corepb.QueueId{
			AccountId: rand.Uint64(),
			QueueId:   rand.Uint64(),
		}

		txn := badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, queueId)
		require.Error(t, err)
		require.Nil(t, retrieved)
		require.ErrorIs(t, err, store.ErrNotFound)
	})
}

func TestQueuesTable_GetByName(t *testing.T) {
	t.Run("gets a queue by name", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueuesTable([]byte{0x77, 0x77, 0x77, 0x77})

		queue := &corepb.Queue{
			Id: &corepb.QueueId{
				AccountId: rand.Uint64(),
				QueueId:   rand.Uint64(),
			},
			Name:        "test_queue",
			Description: "test description",
		}

		txn := badgerStore.Update()
		err = table.Create(txn, queue)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.GetByName(txn, queue.Id.AccountId, queue.Name)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, queue.Id.QueueId, retrieved.Id.QueueId)
		require.Equal(t, queue.Description, retrieved.Description)
	})

	t.Run("gets a nonexistent queue by name", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueuesTable([]byte{0x77, 0x77, 0x77, 0x77})

		txn := badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.GetByName(txn, rand.Uint64(), "nonexistent_queue")
		require.Error(t, err)
		require.Nil(t, retrieved)
		require.ErrorIs(t, err, store.ErrNotFound)
	})

	t.Run("a queue is not visible by name from a different account", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueuesTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()

		queue := &corepb.Queue{
			Id: &corepb.QueueId{
				AccountId: accountId1,
				QueueId:   rand.Uint64(),
			},
			Name: "test_queue",
		}

		txn := badgerStore.Update()
		err = table.Create(txn, queue)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.GetByName(txn, accountId2, "test_queue")
		require.Error(t, err)
		require.Nil(t, retrieved)
		require.ErrorIs(t, err, store.ErrNotFound)
	})
}

func TestQueuesTable_Update(t *testing.T) {
	t.Run("updates a queue", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueuesTable([]byte{0x77, 0x77, 0x77, 0x77})

		queue := &corepb.Queue{
			Id: &corepb.QueueId{
				AccountId: rand.Uint64(),
				QueueId:   rand.Uint64(),
			},
			Name:        "test_queue",
			Description: "original description",
			Version:     1,
		}

		txn := badgerStore.Update()
		err = table.Create(txn, queue)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		queue.Description = "updated description"
		queue.Version = 2

		txn = badgerStore.Update()
		err = table.Update(txn, queue)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		retrieved, err := table.Get(txn, queue.Id)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, "updated description", retrieved.Description)
		require.EqualValues(t, 2, retrieved.Version)
		require.Equal(t, queue.Name, retrieved.Name)
	})
}

func TestQueuesTable_Delete(t *testing.T) {
	t.Run("deletes an existing queue", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueuesTable([]byte{0x77, 0x77, 0x77, 0x77})

		queue := &corepb.Queue{
			Id: &corepb.QueueId{
				AccountId: rand.Uint64(),
				QueueId:   rand.Uint64(),
			},
			Name: "test_queue",
		}

		txn := badgerStore.Update()
		err = table.Create(txn, queue)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		// Delete (should remove from both the main table and the names index)
		txn = badgerStore.Update()
		err = table.Delete(txn, queue)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())

		txn = badgerStore.View()
		defer txn.Discard()

		_, err = table.Get(txn, queue.Id)
		require.Error(t, err)
		require.ErrorIs(t, err, store.ErrNotFound)

		// The names index entry must be gone too
		_, err = table.namesIndex.Get(txn, table.namesIndexPK(queue.Id.AccountId, queue.Name))
		require.Error(t, err)
		require.ErrorIs(t, err, store.ErrNotFound)

		retrieved, err := table.GetByName(txn, queue.Id.AccountId, queue.Name)
		require.Error(t, err)
		require.Nil(t, retrieved)
		require.ErrorIs(t, err, store.ErrNotFound)
	})

	t.Run("deletes a nonexistent queue does not error", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueuesTable([]byte{0x77, 0x77, 0x77, 0x77})

		queue := &corepb.Queue{
			Id: &corepb.QueueId{
				AccountId: rand.Uint64(),
				QueueId:   rand.Uint64(),
			},
			Name: "test_queue",
		}

		txn := badgerStore.Update()
		err = table.Delete(txn, queue)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
	})
}

func TestQueuesTable_List(t *testing.T) {
	t.Run("lists queues in an account", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueuesTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()

		numQueues := 5
		for i := range numQueues {
			queue := &corepb.Queue{
				Id: &corepb.QueueId{
					AccountId: accountId,
					QueueId:   uint64(i + 1),
				},
				Name: fmt.Sprintf("test_queue_%d", i),
			}

			txn := badgerStore.Update()
			err := table.Create(txn, queue)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		txn := badgerStore.View()
		defer txn.Discard()

		result, err := table.List(txn, accountId, nil, 100)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Queues, numQueues)
		require.Nil(t, result.NextPaginationToken)
		require.Nil(t, result.PreviousPaginationToken)
	})

	t.Run("lists queues with pagination", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueuesTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()

		numQueues := 10
		for i := range numQueues {
			queue := &corepb.Queue{
				Id: &corepb.QueueId{
					AccountId: accountId,
					QueueId:   uint64(i + 1),
				},
				Name: fmt.Sprintf("test_queue_%02d", i),
			}

			txn := badgerStore.Update()
			err := table.Create(txn, queue)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		txn := badgerStore.View()
		defer txn.Discard()

		page1, err := table.List(txn, accountId, nil, 3)
		require.NoError(t, err)
		require.Len(t, page1.Queues, 3)
		require.NotNil(t, page1.NextPaginationToken)
		require.Nil(t, page1.PreviousPaginationToken)

		page2, err := table.List(txn, accountId, page1.NextPaginationToken, 3)
		require.NoError(t, err)
		require.Len(t, page2.Queues, 3)
		require.NotNil(t, page2.NextPaginationToken)
		require.NotNil(t, page2.PreviousPaginationToken)

		// No overlap between pages
		for _, q1 := range page1.Queues {
			for _, q2 := range page2.Queues {
				require.NotEqual(t, q1.Id.QueueId, q2.Id.QueueId)
			}
		}
	})

	t.Run("requesting exactly the remaining count returns no next token", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueuesTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()

		numQueues := 5
		for i := range numQueues {
			queue := &corepb.Queue{
				Id: &corepb.QueueId{
					AccountId: accountId,
					QueueId:   uint64(i + 1),
				},
				Name: fmt.Sprintf("test_queue_%d", i),
			}

			txn := badgerStore.Update()
			err := table.Create(txn, queue)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		txn := badgerStore.View()
		defer txn.Discard()

		// The limit exactly matches the number of remaining rows: there is
		// nothing left after this page, so NextPaginationToken must be nil,
		// not just eventually nil after fetching an extra, empty page.
		result, err := table.List(txn, accountId, nil, numQueues)
		require.NoError(t, err)
		require.Len(t, result.Queues, numQueues)
		require.Nil(t, result.NextPaginationToken)
	})

	t.Run("the previous token returns to the exact same page", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueuesTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId := rand.Uint64()

		numQueues := 10
		for i := range numQueues {
			queue := &corepb.Queue{
				Id: &corepb.QueueId{
					AccountId: accountId,
					QueueId:   uint64(i + 1),
				},
				Name: fmt.Sprintf("test_queue_%02d", i),
			}

			txn := badgerStore.Update()
			err := table.Create(txn, queue)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		txn := badgerStore.View()
		defer txn.Discard()

		page1, err := table.List(txn, accountId, nil, 3)
		require.NoError(t, err)
		require.Len(t, page1.Queues, 3)

		page2, err := table.List(txn, accountId, page1.NextPaginationToken, 3)
		require.NoError(t, err)
		require.NotNil(t, page2.PreviousPaginationToken)

		back, err := table.List(txn, accountId, page2.PreviousPaginationToken, 3)
		require.NoError(t, err)
		require.Len(t, back.Queues, len(page1.Queues))
		for i := range page1.Queues {
			require.Equal(t, page1.Queues[i].Id.QueueId, back.Queues[i].Id.QueueId)
		}
	})

	t.Run("lists an empty account", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueuesTable([]byte{0x77, 0x77, 0x77, 0x77})

		txn := badgerStore.View()
		defer txn.Discard()

		result, err := table.List(txn, rand.Uint64(), nil, 100)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Queues, 0)
		require.Nil(t, result.NextPaginationToken)
		require.Nil(t, result.PreviousPaginationToken)
	})

	t.Run("queues from different accounts are isolated", func(t *testing.T) {
		badgerStore, err := store.NewBadgerInMemoryStore()
		require.NoError(t, err)

		table := newQueuesTable([]byte{0x77, 0x77, 0x77, 0x77})

		accountId1 := rand.Uint64()
		accountId2 := rand.Uint64()

		for i := range 3 {
			queue := &corepb.Queue{
				Id:   &corepb.QueueId{AccountId: accountId1, QueueId: uint64(i + 1)},
				Name: fmt.Sprintf("acc1_queue_%d", i),
			}
			txn := badgerStore.Update()
			err := table.Create(txn, queue)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		for i := range 5 {
			queue := &corepb.Queue{
				Id:   &corepb.QueueId{AccountId: accountId2, QueueId: uint64(i + 1)},
				Name: fmt.Sprintf("acc2_queue_%d", i),
			}
			txn := badgerStore.Update()
			err := table.Create(txn, queue)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
		}

		txn := badgerStore.View()
		defer txn.Discard()

		result1, err := table.List(txn, accountId1, nil, 100)
		require.NoError(t, err)
		require.Len(t, result1.Queues, 3)

		result2, err := table.List(txn, accountId2, nil, 100)
		require.NoError(t, err)
		require.Len(t, result2.Queues, 5)
	})
}
