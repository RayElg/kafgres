package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

func makeTopic(a sarama.ClusterAdmin, name string, partitions int32, cfg map[string]*string) error {
	return a.CreateTopic(name, &sarama.TopicDetail{NumPartitions: partitions, ReplicationFactor: 1, ConfigEntries: cfg}, false)
}

func topicRecreate(broker, topic string) error {
	a, err := newAdmin(broker)
	if err != nil {
		return err
	}
	defer a.Close()
	p, err := syncProducer(broker, producerConfig())
	if err != nil {
		return err
	}
	for i := 0; i < 3; i++ {
		if _, err := sendTo(p, topic, 0, fmt.Sprintf("old%d", i)); err != nil {
			p.Close()
			return err
		}
	}
	p.Close()
	before, err := a.DescribeTopics([]string{topic})
	if err != nil || len(before) != 1 {
		return fmt.Errorf("describe: %v", err)
	}
	if err := a.DeleteTopic(topic); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	created := "kerr36"
	for i := 0; i < 60 && created == "kerr36"; i++ {
		created = errName(makeTopic(a, topic, 1, nil))
		if created != "none" {
			time.Sleep(250 * time.Millisecond)
		}
	}
	if created == "none" {
		created = "ok"
	}
	var after []*sarama.TopicMetadata
	for i := 0; i < 40; i++ {
		after, err = a.DescribeTopics([]string{topic})
		if err == nil && len(after) == 1 && after[0].Err == sarama.ErrNoError {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	p2, err := syncProducer(broker, producerConfig())
	if err != nil {
		return err
	}
	defer p2.Close()
	o0, err := sendTo(p2, topic, 0, "new0")
	if err != nil {
		return err
	}
	o1, err := sendTo(p2, topic, 0, "new1")
	if err != nil {
		return err
	}
	got, err := readPartition(broker, topic, 0, sarama.OffsetOldest, config(), 2, 30*time.Second)
	if err != nil {
		return err
	}
	client, err := sarama.NewClient([]string{broker}, config())
	if err != nil {
		return err
	}
	defer client.Close()
	b, _ := client.GetOffset(topic, 0, sarama.OffsetOldest)
	e, _ := client.GetOffset(topic, 0, sarama.OffsetNewest)
	fmt.Printf("OK recreated=%s new_id=%v offsets=%d,%d read=%s begin=%d end=%d\n", created,
		before[0].Uuid != after[0].Uuid, o0, o1, values(got), b, e)
	return nil
}

func topicLimits(broker, topic string) error {
	a, err := newAdmin(broker)
	if err != nil {
		return err
	}
	defer a.Close()
	t := topic + "-lim"
	if err := makeTopic(a, t, 1, map[string]*string{"max.message.bytes": ptr("2048")}); err != nil {
		return fmt.Errorf("create: %w", err)
	}
	cfg, err := a.DescribeConfig(sarama.ConfigResource{Type: sarama.TopicResource, Name: t})
	if err != nil {
		return err
	}
	conf := "absent"
	for _, e := range cfg {
		if e.Name == "max.message.bytes" {
			conf = fmt.Sprintf("%s/%d", e.Value, e.Source)
		}
	}
	p, err := syncProducer(broker, producerConfig())
	if err != nil {
		return err
	}
	defer p.Close()
	_, _, over := p.SendMessage(&sarama.ProducerMessage{Topic: t, Value: sarama.ByteEncoder(make([]byte, 4000))})
	_, small, err := p.SendMessage(&sarama.ProducerMessage{Topic: t, Value: sarama.ByteEncoder(make([]byte, 1000))})
	if err != nil {
		return fmt.Errorf("under limit: %w", err)
	}
	fmt.Printf("OK config=%s over_limit=%s under_limit_offset=%d\n", conf, errName(over), small)
	return nil
}

func manyPartitions(broker, topic string) error {
	const n = 50
	p, err := syncProducer(broker, producerConfig())
	if err != nil {
		return err
	}
	for i := int32(0); i < n; i++ {
		if _, err := sendTo(p, topic, i, fmt.Sprintf("mp%d", i)); err != nil {
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
	parts, err := client.Partitions(topic)
	if err != nil {
		return err
	}
	var total int64
	cons, err := sarama.NewConsumer([]string{broker}, config())
	if err != nil {
		return err
	}
	defer cons.Close()
	seen := 0
	for _, part := range parts {
		e, err := client.GetOffset(topic, part, sarama.OffsetNewest)
		if err != nil {
			return err
		}
		total += e
		pc, err := cons.ConsumePartition(topic, part, sarama.OffsetOldest)
		if err != nil {
			return err
		}
		got, err := collect(pc, 1, 30*time.Second)
		pc.Close()
		if err != nil {
			return err
		}
		seen += len(got)
	}
	fmt.Printf("OK described=%d end_sum=%d read_partitions=%d\n", len(parts), total, seen)
	return nil
}

func manyRecords(broker, topic string) error {
	const n = 20000
	c := producerConfig()
	c.Producer.Flush.Messages = 500
	c.Producer.Flush.Frequency = 20 * time.Millisecond
	p, err := syncProducer(broker, c)
	if err != nil {
		return err
	}
	var sent [][]byte
	var msgs []*sarama.ProducerMessage
	for i := 0; i < n; i++ {
		v := []byte(strings.Repeat(fmt.Sprintf("%08d-", i), 10))
		sent = append(sent, v)
		msgs = append(msgs, &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(v)})
	}
	if err := p.SendMessages(msgs); err != nil {
		p.Close()
		return fmt.Errorf("send: %w", err)
	}
	p.Close()
	got, err := readPartition(broker, topic, 0, sarama.OffsetOldest, config(), n, 120*time.Second)
	if err != nil {
		return err
	}
	var read [][]byte
	for _, m := range got {
		read = append(read, m.Value)
	}
	fmt.Printf("OK count=%d crc=%s same=%v\n", len(read), crc(read), crc(read) == crc(sent))
	return nil
}

func seekIntoBatch(broker, topic string) error {
	t0 := time.UnixMilli(1_700_000_000_000)
	c := producerConfig()
	c.Producer.Flush.Messages = 100
	c.Producer.Flush.Frequency = time.Second
	p, err := syncProducer(broker, c)
	if err != nil {
		return err
	}
	var msgs []*sarama.ProducerMessage
	for i := 0; i < 100; i++ {
		msgs = append(msgs, &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(fmt.Sprintf("b%d", i)),
			Timestamp: t0.Add(time.Duration(i) * time.Millisecond)})
	}
	if err := p.SendMessages(msgs); err != nil {
		p.Close()
		return err
	}
	p.Close()
	contiguous := true
	for i, m := range msgs {
		contiguous = contiguous && m.Offset == int64(i)
	}
	first, err := readPartition(broker, topic, 0, 37, config(), 1, 30*time.Second)
	if err != nil {
		return fmt.Errorf("first: %w", err)
	}
	five, err := readPartition(broker, topic, 0, 37, config(), 5, 30*time.Second)
	if err != nil {
		return fmt.Errorf("five: %w", err)
	}
	last, err := readPartition(broker, topic, 0, 99, config(), 1, 30*time.Second)
	if err != nil {
		return fmt.Errorf("last: %w", err)
	}
	past, err := readPartition(broker, topic, 0, 100, config(), 0, 3*time.Second)
	if err != nil {
		return fmt.Errorf("past: %w", err)
	}
	fmt.Printf("OK contiguous=%v first=%d five=%s last=%s past_end=%d\n", contiguous, first[0].Offset,
		values(five[:5]), values(last), len(past))
	return nil
}

func groupPartitionGrowth(broker, topic string) error {
	g := topic + "-g"
	c := config()
	c.Metadata.RefreshFrequency = time.Second
	m, err := startMember(broker, topic, g, c)
	if err != nil {
		return err
	}
	defer m.close()
	a, err := newAdmin(broker)
	if err != nil {
		return err
	}
	defer a.Close()
	deadline := time.Now().Add(60 * time.Second)
	for len(m.ownedSet()) == 0 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	before := len(m.ownedSet())
	if err := a.CreatePartitions(topic, 3, nil, false); err != nil {
		return fmt.Errorf("grow: %w", err)
	}
	for len(m.ownedSet()) < 3 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	after := len(m.ownedSet())
	if err := produceSpread(broker, topic, 9); err != nil {
		return err
	}
	fmt.Printf("OK before=%d after=%d %s\n", before, after, consumeAll(9, m))
	return nil
}

func compactedNullKey(broker, topic string) error {
	a, err := newAdmin(broker)
	if err != nil {
		return err
	}
	defer a.Close()
	t := topic + "-c"
	if err := makeTopic(a, t, 1, map[string]*string{"cleanup.policy": ptr("compact")}); err != nil {
		return fmt.Errorf("create: %w", err)
	}
	p, err := syncProducer(broker, producerConfig())
	if err != nil {
		return err
	}
	defer p.Close()
	_, _, nullKey := p.SendMessage(&sarama.ProducerMessage{Topic: t, Value: sarama.StringEncoder("v")})
	_, keyed, err := p.SendMessage(&sarama.ProducerMessage{Topic: t, Key: sarama.StringEncoder("k"), Value: sarama.StringEncoder("v")})
	if err != nil {
		return err
	}
	_, tomb, err := p.SendMessage(&sarama.ProducerMessage{Topic: t, Key: sarama.StringEncoder("k")})
	if err != nil {
		return err
	}
	fmt.Printf("OK null_key=%s keyed_offset=%d tombstone_offset=%d\n", errName(nullKey), keyed, tomb)
	return nil
}
