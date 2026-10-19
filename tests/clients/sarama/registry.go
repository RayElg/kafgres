package main

// Each scenario prints one "OK ..." line; kafgres and Kafka must print the same line.
type scenarioFunc func(broker, topic string) error

func scenarios() map[string]scenarioFunc {
	return map[string]scenarioFunc{
		// The transactions tests also drive this first group.
		"produce-consume":    produceConsume,
		"group-consume":      groupConsume,
		"metadata":           metadata,
		"unknown-topic":      unknownTopic,
		"txn-commit":         func(b, t string) error { return transaction(b, t, true) },
		"txn-abort":          func(b, t string) error { return transaction(b, t, false) },
		"txn-abort-big":      transactionBig,
		"txn-open":           transactionHeldOpen,
		"txn-second":         transactionThenOpenAnother,
		"txn-offsets-commit": func(b, t string) error { return transactionWithOffsets(b, t, true) },
		"txn-offsets-abort":  func(b, t string) error { return transactionWithOffsets(b, t, false) },

		"records-shape":                 recordsShape,
		"compression":                   compression,
		"large-records":                 largeRecords,
		"record-too-large":              recordTooLarge,
		"multi-partition":               multiPartition,
		"offsets-by-time":               offsetsByTime,
		"seek":                          seek,
		"long-poll":                     longPoll,
		"offset-out-of-range":           offsetOutOfRange,
		"idempotent-produce":            idempotentProduce,
		"acks":                          acks,
		"txn-read-committed":            txnReadCommitted,
		"txn-consume-transform-produce": txnConsumeTransformProduce,
		"txn-fenced":                    txnFenced,
		"group-two-members":             groupTwoMembers,
		"group-resume":                  groupResume,
		"group-rebalance-leave":         groupRebalanceLeave,
		"group-static":                  groupStatic,
		"group-admin":                   groupAdmin,
		"admin-topics":                  adminTopics,
		"admin-configs":                 adminConfigs,
		"admin-cluster":                 adminCluster,
		"admin-delete-records":          adminDeleteRecords,
		"admin-offsets":                 adminOffsets,
		"topic-recreate":                topicRecreate,
		"topic-limits":                  topicLimits,
		"many-partitions":               manyPartitions,
		"many-records":                  manyRecords,
		"seek-into-batch":               seekIntoBatch,
		"group-partition-growth":        groupPartitionGrowth,
		"compacted-null-key":            compactedNullKey,
	}
}
