package ids

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/moab/pkg/corepb"
)

func TestTaskIdEncodeDecode(t *testing.T) {
	for range 10000 {
		id := rand.Uint64()

		//fmt.Println(EncodeTaskId(id))

		actual, err := DecodeTaskId(EncodeTaskId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestTaskIdDecode(t *testing.T) {
	_, err := DecodeTaskId("err_ISfFsVup2QS")
	require.Error(t, err)

	_, err = DecodeTaskId("tsk_ISfFsV2QS")
	require.Error(t, err)

	_, err = DecodeTaskId("tsk_ISfFsVup2QS3dg")
	require.Error(t, err)

	_, err = DecodeTaskId("tsk_ISfFs+up2QS")
	require.Error(t, err)

	_, err = DecodeTaskId("tsk_ISfFsVup2QS")
	require.NoError(t, err)
}

func TestQueueIdEncodeDecode(t *testing.T) {
	for range 10000 {
		id := &corepb.QueueId{
			AccountId: rand.Uint64(),
			QueueId:   rand.Uint64(),
		}

		actual, err := DecodeQueueId(EncodeQueueId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestQueueIdDecode(t *testing.T) {
	_, err := DecodeQueueId("err_rXY2PuPCAAwyEsf4eRAAAA")
	require.Error(t, err)

	_, err = DecodeQueueId("que_rXY2PuPCAAwyEsf4eR")
	require.Error(t, err)

	_, err = DecodeQueueId("que_rXY2PuPCAAwyEsf4eRAAAA3dg")
	require.Error(t, err)

	_, err = DecodeQueueId("que_+XY2PuPCAAwyEsf4eRAAAA")
	require.Error(t, err)

	_, err = DecodeQueueId("que_rXY2PuPCAAwyEsf4eRAAAA")
	require.NoError(t, err)
}

func TestScheduleIdEncodeDecode(t *testing.T) {
	for range 10000 {
		id := &corepb.ScheduleId{
			AccountId:  rand.Uint64(),
			QueueId:    rand.Uint64(),
			ScheduleId: rand.Uint64(),
		}

		actual, err := DecodeScheduleId(EncodeScheduleId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestScheduleIdDecode(t *testing.T) {
	_, err := DecodeScheduleId("err_jjQsZFIAAAw6Fm9j7jAAAsMB7H3HBAAA")
	require.Error(t, err)

	_, err = DecodeScheduleId("schdl_jjQsZFIAAAw6Fm9j7jAAAsMB7H3H")
	require.Error(t, err)

	_, err = DecodeScheduleId("schdl_jjQsZFIAAAw6Fm9j7jAAAsMB7H3HBAAA3dg")
	require.Error(t, err)

	_, err = DecodeScheduleId("schdl_+jQsZFIAAAw6Fm9j7jAAAsMB7H3HBAAA")
	require.Error(t, err)

	_, err = DecodeScheduleId("schdl_jjQsZFIAAAw6Fm9j7jAAAsMB7H3HBAAA")
	require.NoError(t, err)
}
