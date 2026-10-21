package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

// state is what a consumer at `c`'s isolation reads from the start of partition 0. Sarama
// decodes a marker's value only on read_committed fetches, so a missing value shows here.
func state(broker, topic string, c *sarama.Config) string {
	got, err := readPartition(broker, topic, 0, sarama.OffsetOldest, c, 0, 4*time.Second)
	return fmt.Sprintf("values=%s err=%s", values(got), errName(err))
}

func txnReadCommitted(broker, topic string) error {
	t, err := txnProducer(broker, "sarama-rc-"+topic)
	if err != nil {
		return err
	}
	defer t.Close()
	plain, err := syncProducer(broker, producerConfig())
	if err != nil {
		return err
	}
	defer plain.Close()

	send := func(vs ...string) error {
		for _, v := range vs {
			if _, err := sendTo(t, topic, 0, v); err != nil {
				return fmt.Errorf("send %s: %w", v, err)
			}
		}
		return nil
	}
	if err := t.BeginTxn(); err != nil {
		return err
	}
	if err := send("a0", "a1"); err != nil {
		return err
	}
	if err := t.CommitTxn(); err != nil {
		return fmt.Errorf("commit a: %w", err)
	}
	if err := t.BeginTxn(); err != nil {
		return err
	}
	if err := send("b0", "b1"); err != nil {
		return err
	}
	if err := t.AbortTxn(); err != nil {
		return fmt.Errorf("abort b: %w", err)
	}
	if _, err := sendTo(plain, topic, 0, "n0"); err != nil {
		return err
	}
	if err := t.BeginTxn(); err != nil {
		return err
	}
	if err := send("c0"); err != nil {
		return err
	}
	if err := t.CommitTxn(); err != nil {
		return fmt.Errorf("commit c: %w", err)
	}
	if err := t.BeginTxn(); err != nil {
		return err
	}
	if err := send("o0"); err != nil {
		return err
	}
	settled := state(broker, topic, committedConfig())
	open := state(broker, topic, config())
	if err := t.CommitTxn(); err != nil {
		return fmt.Errorf("commit o: %w", err)
	}
	after := state(broker, topic, committedConfig())
	fmt.Printf("OK settled=[%s] open_uncommitted=[%s] after_commit=[%s]\n", settled, open, after)
	return nil
}

func txnConsumeTransformProduce(broker, topic string) error {
	out, group := topic+"-out", topic+"-g"
	admin, err := sarama.NewClusterAdmin([]string{broker}, config())
	if err != nil {
		return err
	}
	defer admin.Close()
	if err := admin.CreateTopic(out, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		return fmt.Errorf("create %s: %w", out, err)
	}
	plain, err := syncProducer(broker, producerConfig())
	if err != nil {
		return err
	}
	defer plain.Close()
	for i := 0; i < 5; i++ {
		if _, err := sendTo(plain, topic, 0, fmt.Sprintf("in%d", i)); err != nil {
			return err
		}
	}
	batch, err := readPartition(broker, topic, 0, sarama.OffsetOldest, committedConfig(), 5, 30*time.Second)
	if err != nil {
		return fmt.Errorf("read input: %w", err)
	}
	t, err := txnProducer(broker, "sarama-cpt-"+topic)
	if err != nil {
		return err
	}
	defer t.Close()

	// Kafka writes commit markers asynchronously, so offsets land shortly after CommitTxn.
	committed := func() string {
		out := "-1"
		for i := 0; i < 40 && out == "-1"; i++ {
			r, err := admin.ListConsumerGroupOffsets(group, map[string][]int32{topic: {0}})
			if err != nil {
				return errName(err)
			}
			out = fmt.Sprint(r.GetBlock(topic, 0).Offset)
			if out == "-1" {
				time.Sleep(250 * time.Millisecond)
			}
		}
		return out
	}
	if err := t.BeginTxn(); err != nil {
		return err
	}
	for _, m := range batch {
		if _, _, err := t.SendMessage(&sarama.ProducerMessage{
			Topic: out, Value: sarama.StringEncoder(strings.ToUpper(string(m.Value)))}); err != nil {
			return fmt.Errorf("send: %w", err)
		}
	}
	if err := t.AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata{topic: {{Partition: 0, Offset: 5}}}, group); err != nil {
		return fmt.Errorf("add offsets: %w", err)
	}
	if err := t.CommitTxn(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	afterCommit := committed()

	if _, err := sendTo(plain, topic, 0, "in5"); err != nil {
		return err
	}
	more, err := readPartition(broker, topic, 0, 5, committedConfig(), 1, 30*time.Second)
	if err != nil {
		return fmt.Errorf("read second: %w", err)
	}
	if err := t.BeginTxn(); err != nil {
		return err
	}
	if _, _, err := t.SendMessage(&sarama.ProducerMessage{Topic: out, Value: sarama.StringEncoder("SHOULD-NOT-APPEAR")}); err != nil {
		return err
	}
	if err := t.AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata{topic: {{Partition: 0, Offset: 6}}}, group); err != nil {
		return fmt.Errorf("add offsets 2: %w", err)
	}
	if err := t.AbortTxn(); err != nil {
		return fmt.Errorf("abort: %w", err)
	}
	afterAbort := committed()
	got, err := readPartition(broker, out, 0, sarama.OffsetOldest, committedConfig(), 0, 4*time.Second)
	if err != nil {
		return fmt.Errorf("read output: %w", err)
	}
	fmt.Printf("OK processed=%d committed_after_commit=%s second_batch=%d committed_after_abort=%s output=%s\n",
		len(batch), afterCommit, len(more), afterAbort, values(got))
	return nil
}

func txnFenced(broker, topic string) error {
	id := "sarama-fence-" + topic
	first, err := txnProducer(broker, id)
	if err != nil {
		return err
	}
	defer first.Close()
	if err := first.BeginTxn(); err != nil {
		return err
	}
	if _, err := sendTo(first, topic, 0, "stale"); err != nil {
		return err
	}
	// InitProducerId over an open transaction aborts it and answers CONCURRENT_TRANSACTIONS
	// until the abort completes; Sarama does not retry that.
	var second sarama.SyncProducer
	for i := 0; i < 40; i++ {
		second, err = txnProducer(broker, id)
		if err == nil || !strings.Contains(err.Error(), "concurrent operation") {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if err != nil {
		return err
	}
	defer second.Close()
	if err := second.BeginTxn(); err != nil {
		return err
	}
	if _, err := sendTo(second, topic, 0, "fresh"); err != nil {
		return err
	}
	failed := "none"
	if _, err := sendTo(first, topic, 0, "stale-2"); err != nil {
		failed = errName(err)
	} else if err := first.CommitTxn(); err != nil {
		failed = errName(err)
	}
	if err := second.CommitTxn(); err != nil {
		return fmt.Errorf("second commit: %w", err)
	}
	fmt.Printf("OK first_failed_with=%s read_committed=[%s]\n", failed, state(broker, topic, committedConfig()))
	return nil
}
