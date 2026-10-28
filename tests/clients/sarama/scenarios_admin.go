package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

func ptr(s string) *string { return &s }

func newAdmin(broker string) (sarama.ClusterAdmin, error) {
	return sarama.NewClusterAdmin([]string{broker}, config())
}

func adminTopics(broker, topic string) error {
	a, err := newAdmin(broker)
	if err != nil {
		return err
	}
	defer a.Close()
	t := topic + "-a"
	var out []string
	if err := a.CreateTopic(t, &sarama.TopicDetail{NumPartitions: 3, ReplicationFactor: 1,
		ConfigEntries: map[string]*string{"retention.ms": ptr("3600000")}}, false); err != nil {
		return fmt.Errorf("create: %w", err)
	}
	try := func(name, sub string, d *sarama.TopicDetail) {
		out = append(out, name+"="+errName(a.CreateTopic(sub, d, false)))
	}
	try("again", t, &sarama.TopicDetail{NumPartitions: 3, ReplicationFactor: 1})
	try("zero_partitions", t+"-z", &sarama.TopicDetail{NumPartitions: 0, ReplicationFactor: 1})
	try("rf2", t+"-r", &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 2})
	try("bad_config", t+"-c", &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1,
		ConfigEntries: map[string]*string{"no.such.config": ptr("1")}})
	try("bad_name", "bad name!", &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1})

	ds, err := a.DescribeTopics([]string{t})
	if err != nil || len(ds) != 1 {
		return fmt.Errorf("describe: %v", err)
	}
	d := ds[0]
	out = append(out, fmt.Sprintf("partitions=%d rf=%d isr=%d internal=%v has_id=%v", len(d.Partitions),
		len(d.Partitions[0].Replicas), len(d.Partitions[0].Isr), d.IsInternal, d.Uuid != sarama.Uuid{}))
	all, err := a.ListTopics()
	if err != nil {
		return err
	}
	_, listed := all[t]
	out = append(out, fmt.Sprintf("listed=%v", listed))
	cfg, err := a.DescribeConfig(sarama.ConfigResource{Type: sarama.TopicResource, Name: t})
	if err != nil {
		return fmt.Errorf("describe config: %w", err)
	}
	for _, e := range cfg {
		if e.Name == "retention.ms" {
			out = append(out, fmt.Sprintf("retention.ms=%s/%d", e.Value, e.Source))
		}
	}
	out = append(out, "grow="+errName(a.CreatePartitions(t, 5, nil, false)))
	// Metadata may lag the growth by a moment; read until it reflects it.
	for i := 0; i < 40; i++ {
		ds, _ = a.DescribeTopics([]string{t})
		if len(ds) == 1 && len(ds[0].Partitions) == 5 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	out = append(out, fmt.Sprintf("grown=%d", len(ds[0].Partitions)))
	out = append(out, "shrink="+errName(a.CreatePartitions(t, 2, nil, false)))
	out = append(out, "grow_missing="+errName(a.CreatePartitions(t+"-nope", 2, nil, false)))
	out = append(out, "delete="+errName(a.DeleteTopic(t)))
	out = append(out, "delete_again="+errName(a.DeleteTopic(t)))
	fmt.Printf("OK %s\n", strings.Join(out, " "))
	return nil
}

func adminConfigs(broker, topic string) error {
	a, err := newAdmin(broker)
	if err != nil {
		return err
	}
	defer a.Close()
	res := sarama.ConfigResource{Type: sarama.TopicResource, Name: topic}
	entry := func(name string) string {
		cfg, err := a.DescribeConfig(res)
		if err != nil {
			return errName(err)
		}
		for _, e := range cfg {
			if e.Name == name {
				return fmt.Sprintf("%s/%d", e.Value, e.Source)
			}
		}
		return "absent"
	}
	var out []string
	for _, k := range []string{"retention.ms", "cleanup.policy", "max.message.bytes", "segment.bytes",
		"min.compaction.lag.ms", "compression.type", "message.timestamp.type"} {
		out = append(out, k+"="+entry(k))
	}
	set := func(k string, v *string, op sarama.IncrementalAlterConfigsOperation) error {
		return a.IncrementalAlterConfig(sarama.TopicResource, topic,
			map[string]sarama.IncrementalAlterConfigsEntry{k: {Operation: op, Value: v}}, false)
	}
	if err := set("retention.ms", ptr("12345"), sarama.IncrementalAlterConfigsOperationSet); err != nil {
		return fmt.Errorf("set: %w", err)
	}
	out = append(out, "after_set="+entry("retention.ms"))
	if err := set("retention.ms", nil, sarama.IncrementalAlterConfigsOperationDelete); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	out = append(out, "after_delete="+entry("retention.ms"))
	// Sarama reports this failure as broker text, which differs between brokers: compare
	// only whether it was refused.
	refused := func(err error) string {
		if err == nil {
			return "accepted"
		}
		return "refused"
	}
	out = append(out, "unknown_key="+refused(set("no.such.config", ptr("1"), sarama.IncrementalAlterConfigsOperationSet)))
	out = append(out, "bad_value="+refused(set("retention.ms", ptr("not-a-number"), sarama.IncrementalAlterConfigsOperationSet)))
	_, derr := a.DescribeConfig(sarama.ConfigResource{Type: sarama.TopicResource, Name: topic + "-missing"})
	out = append(out, "describe_missing="+errName(derr))
	brokers, _, err := a.DescribeCluster()
	if err != nil || len(brokers) == 0 {
		return fmt.Errorf("cluster: %v", err)
	}
	bc, err := a.DescribeConfig(sarama.ConfigResource{Type: sarama.BrokerResource, Name: fmt.Sprint(brokers[0].ID())})
	out = append(out, fmt.Sprintf("broker_config=%v", err == nil && len(bc) > 0))
	fmt.Printf("OK %s\n", strings.Join(out, " "))
	return nil
}

func adminCluster(broker, topic string) error {
	a, err := newAdmin(broker)
	if err != nil {
		return err
	}
	defer a.Close()
	brokers, controller, err := a.DescribeCluster()
	if err != nil {
		return fmt.Errorf("describe cluster: %w", err)
	}
	listed := false
	var ids []int32
	for _, b := range brokers {
		ids = append(ids, b.ID())
		listed = listed || b.ID() == controller
	}
	re, err := a.ListPartitionReassignments(topic, []int32{0})
	if err != nil {
		return fmt.Errorf("reassignments: %w", err)
	}
	dirs, err := a.DescribeLogDirs(ids)
	if err != nil {
		return fmt.Errorf("log dirs: %w", err)
	}
	covers := false
	for _, ds := range dirs {
		for _, d := range ds {
			for _, t := range d.Topics {
				for _, p := range t.Partitions {
					covers = covers || (t.Topic == topic && p.PartitionID == 0)
				}
			}
		}
	}
	n := 0
	for _, ps := range re {
		n += len(ps)
	}
	fmt.Printf("OK nodes=%d controller_listed=%v reassignments=%d log_dirs_cover_partition=%v\n",
		len(brokers), listed, n, covers)
	return nil
}

func adminDeleteRecords(broker, topic string) error {
	p, err := syncProducer(broker, producerConfig())
	if err != nil {
		return err
	}
	for i := 0; i < 10; i++ {
		if _, err := sendTo(p, topic, 0, fmt.Sprintf("d%d", i)); err != nil {
			p.Close()
			return err
		}
	}
	p.Close()
	a, err := newAdmin(broker)
	if err != nil {
		return err
	}
	defer a.Close()
	client, err := sarama.NewClient([]string{broker}, config())
	if err != nil {
		return err
	}
	defer client.Close()
	var out []string
	out = append(out, "delete="+errName(a.DeleteRecords(topic, map[int32]int64{0: 4})))
	b, _ := client.GetOffset(topic, 0, sarama.OffsetOldest)
	out = append(out, fmt.Sprintf("begin=%d", b))
	got, err := readPartition(broker, topic, 0, sarama.OffsetOldest, config(), 6, 30*time.Second)
	if err != nil {
		return err
	}
	out = append(out, "read="+values(got))
	out = append(out, "beyond_end="+errName(a.DeleteRecords(topic, map[int32]int64{0: 99})))
	out = append(out, "to_end="+errName(a.DeleteRecords(topic, map[int32]int64{0: -1})))
	b, _ = client.GetOffset(topic, 0, sarama.OffsetOldest)
	out = append(out, fmt.Sprintf("begin_after=%d", b))
	fmt.Printf("OK %s\n", strings.Join(out, " "))
	return nil
}

func adminOffsets(broker, topic string) error {
	t0 := time.UnixMilli(1_700_000_000_000)
	p, err := syncProducer(broker, producerConfig())
	if err != nil {
		return err
	}
	for i, off := range []int{2000, 0, 5000, 1000} {
		if _, _, err := p.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(fmt.Sprintf("x%d", i)),
			Timestamp: t0.Add(time.Duration(off) * time.Millisecond)}); err != nil {
			p.Close()
			return err
		}
	}
	p.Close()
	client, err := sarama.NewClient([]string{broker}, config())
	if err != nil {
		return err
	}
	defer client.Close()
	var out []string
	b, err := client.GetOffset(topic, 0, sarama.OffsetOldest)
	out = append(out, fmt.Sprintf("earliest=%d(%s)", b, errName(err)))
	e, err := client.GetOffset(topic, 0, sarama.OffsetNewest)
	out = append(out, fmt.Sprintf("latest=%d(%s)", e, errName(err)))
	for _, off := range []int64{1500, 99999} {
		o, err := client.GetOffset(topic, 0, t0.UnixMilli()+off)
		out = append(out, fmt.Sprintf("at_t0+%d=%d(%s)", off, o, errName(err)))
	}
	fmt.Printf("OK %s\n", strings.Join(out, " "))
	return nil
}
