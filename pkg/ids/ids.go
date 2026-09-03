package ids

import (
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/evrblk/yellowstone-common/encoding/base62"

	"github.com/evrblk/moab/pkg/corepb"
)

var (
	ErrInvalidId = errors.New("invalid id")

	taskIdRegex     = regexp.MustCompile("^tsk_[0-9a-zA-Z]+$")
	queueIdRegex    = regexp.MustCompile("^que_[0-9a-zA-Z]+$")
	scheduleIdRegex = regexp.MustCompile("^schdl_[0-9a-zA-Z]+$")
)

func DecodeTaskId(s string) (uint64, error) {
	if !taskIdRegex.MatchString(s) {
		return 0, ErrInvalidId
	}

	b, err := base62.DecodeString(strings.TrimPrefix(s, "tsk_"))
	if err != nil {
		return 0, ErrInvalidId
	}

	if len(b) != 8 {
		return 0, ErrInvalidId
	}

	return binary.BigEndian.Uint64(b[0:8]), nil
}

func EncodeTaskId(id uint64) string {
	src := make([]byte, 8)
	binary.BigEndian.PutUint64(src[0:8], id)
	return fmt.Sprintf("tsk_%s", base62.Encode(src))
}

func DecodeQueueId(s string) (*corepb.QueueId, error) {
	if !queueIdRegex.MatchString(s) {
		return nil, ErrInvalidId
	}

	b, err := base62.DecodeString(strings.TrimPrefix(s, "que_"))
	if err != nil {
		return nil, ErrInvalidId
	}

	if len(b) != 8+8 {
		return nil, ErrInvalidId
	}

	return &corepb.QueueId{
		AccountId: binary.BigEndian.Uint64(b[0:8]),
		QueueId:   binary.BigEndian.Uint64(b[8 : 8+8]),
	}, nil
}

func EncodeQueueId(id *corepb.QueueId) string {
	src := make([]byte, 8+8)
	binary.BigEndian.PutUint64(src[0:8], id.AccountId)
	binary.BigEndian.PutUint64(src[8:8+8], id.QueueId)
	return fmt.Sprintf("que_%s", base62.Encode(src))
}

func DecodeScheduleId(s string) (*corepb.ScheduleId, error) {
	if !scheduleIdRegex.MatchString(s) {
		return nil, ErrInvalidId
	}

	b, err := base62.DecodeString(strings.TrimPrefix(s, "schdl_"))
	if err != nil {
		return nil, ErrInvalidId
	}

	if len(b) != 8+8+8 {
		return nil, ErrInvalidId
	}

	return &corepb.ScheduleId{
		AccountId:  binary.BigEndian.Uint64(b[0:8]),
		QueueId:    binary.BigEndian.Uint64(b[8 : 8+8]),
		ScheduleId: binary.BigEndian.Uint64(b[8+8 : 8+8+8]),
	}, nil
}

func EncodeScheduleId(id *corepb.ScheduleId) string {
	src := make([]byte, 8+8+8)
	binary.BigEndian.PutUint64(src[0:8], id.AccountId)
	binary.BigEndian.PutUint64(src[8:8+8], id.QueueId)
	binary.BigEndian.PutUint64(src[8+8:8+8+8], id.ScheduleId)
	return fmt.Sprintf("schdl_%s", base62.Encode(src))
}
