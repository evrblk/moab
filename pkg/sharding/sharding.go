package sharding

import (
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/utils"
)

func ByAccount(accountId uint64) cluster.ShardKey {
	return utils.GetShardKey(utils.ConcatBytes(accountId))
}

func ByAccountAndQueue(accountId uint64, queueId uint64) cluster.ShardKey {
	return utils.GetShardKey(utils.ConcatBytes(accountId, queueId))
}
