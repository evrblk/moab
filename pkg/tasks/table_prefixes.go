package tasks

// Table prefixes for the TasksCore. Each is a one-byte id for one table,
// following this core's node-local replica prefix (assigned by
// honey.ReplicaPrefixRegistry) in every key: [replica prefix][table prefix]
// [record]. They only need to be unique WITHIN this core — every TasksCore
// instance owns a distinct replica prefix, so rows from different cores never
// collide even when they reuse the same table prefix byte.
//
// Treat these as constants; never mutate the returned slices.
var (
	tablePrefixTasks              = []byte{0x00}
	tablePrefixDedupeKeysIndex    = []byte{0x01}
	tablePrefixQueueIndex         = []byte{0x02}
	tablePrefixInProgressIndex    = []byte{0x03}
	tablePrefixDeadTasksIndex     = []byte{0x04}
	tablePrefixExpirationIndex    = []byte{0x05}
	tablePrefixThreads            = []byte{0x06}
	tablePrefixThreadedTasksIndex = []byte{0x07}
	tablePrefixCounters           = []byte{0x08}
	tablePrefixQueueState         = []byte{0x09}
	tablePrefixRateLimiters       = []byte{0x0a}
)
