package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Limits, scale and lifecycle scenarios.

func init() {
	for name, fn := range map[string]scenarioFunc{
		"topic-recreate":         topicRecreate,
		"topic-limits":           topicLimits,
		"many-partitions":        manyPartitions,
		"many-records":           manyRecords,
		"seek-into-batch":        seekIntoBatch,
		"group-partition-growth": groupPartitionGrowth,
		"compacted-null-key":     compactedNullKey,
	} {
		extra[name] = fn
	}
}

var extra = map[string]scenarioFunc{}

func topicRecreate(broker, topic string) (string, error) {
	adm, closeAdm, err := admin(broker)
	if err != nil {
		return "", err
	}
	defer closeAdm()
	p, err := newClient(broker)
	if err != nil {
		return "", err
	}
	for i := 0; i < 3; i++ {
		if _, err := produceOne(p, topic, 0, fmt.Sprintf("old%d", i)); err != nil {
			return "", err
		}
	}
	p.Close()
	ctx, cancel := ctxFor(120 * time.Second)
	defer cancel()
	md, err := adm.Metadata(ctx, topic)
	if err != nil {
		return "", err
	}
	before := md.Topics[topic].ID
	if dr, err := adm.DeleteTopics(ctx, topic); err != nil || dr[topic].Err != nil {
		return "", fmt.Errorf("delete: %v %v", err, dr[topic].Err)
	}
	created := "kerr36"
	for i := 0; i < 60 && created == "kerr36"; i++ {
		r, err := adm.CreateTopic(ctx, 1, 1, nil, topic)
		if err != nil {
			created = errName(err)
		} else {
			created = errName(r.Err)
		}
		if created != "none" {
			time.Sleep(250 * time.Millisecond)
		}
	}
	if created == "none" {
		created = "ok"
	}
	var after kadm.TopicID
	for i := 0; i < 40; i++ {
		md, err = adm.Metadata(ctx, topic)
		if err == nil && md.Topics[topic].Err == nil {
			after = md.Topics[topic].ID
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	p2, err := newClient(broker)
	if err != nil {
		return "", err
	}
	defer p2.Close()
	o0, err := produceOne(p2, topic, 0, "new0")
	if err != nil {
		return "", err
	}
	o1, err := produceOne(p2, topic, 0, "new1")
	if err != nil {
		return "", err
	}
	got, err := read(broker, topic, 0, start(), 2, 0)
	if err != nil {
		return "", err
	}
	s, _ := kadm.NewClient(p2).ListStartOffsets(ctx, topic)
	e, _ := kadm.NewClient(p2).ListEndOffsets(ctx, topic)
	so, _ := s.Lookup(topic, 0)
	eo, _ := e.Lookup(topic, 0)
	return fmt.Sprintf("recreated=%s new_id=%v offsets=%d,%d read=%s begin=%d end=%d", created, before != after, o0, o1,
		values(got), so.Offset, eo.Offset), nil
}

func topicLimits(broker, topic string) (string, error) {
	adm, closeAdm, err := admin(broker)
	if err != nil {
		return "", err
	}
	defer closeAdm()
	ctx, cancel := ctxFor(60 * time.Second)
	defer cancel()
	t := topic + "-lim"
	cr, err := adm.CreateTopic(ctx, 1, 1, map[string]*string{"max.message.bytes": ptr("2048")}, t)
	if err != nil || cr.Err != nil {
		return "", fmt.Errorf("create: %v %v", err, cr.Err)
	}
	conf := "absent"
	rcs, err := adm.DescribeTopicConfigs(ctx, t)
	if err != nil {
		return "", err
	}
	for _, rc := range rcs {
		for _, c := range rc.Configs {
			if c.Key == "max.message.bytes" && c.Value != nil {
				conf = fmt.Sprintf("%s/%d", *c.Value, c.Source)
			}
		}
	}
	cl, err := newClient(broker, kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err != nil {
		return "", err
	}
	defer cl.Close()
	over := cl.ProduceSync(ctx, &kgo.Record{Topic: t, Value: make([]byte, 4000)}).FirstErr()
	res := cl.ProduceSync(ctx, &kgo.Record{Topic: t, Value: make([]byte, 1000)})
	if err := res.FirstErr(); err != nil {
		return "", fmt.Errorf("under limit: %w", err)
	}
	return fmt.Sprintf("config=%s over_limit=%s under_limit_offset=%d", conf, errName(over), res[0].Record.Offset), nil
}

func manyPartitions(broker, topic string) (string, error) {
	const n = 50
	cl, err := newClient(broker)
	if err != nil {
		return "", err
	}
	defer cl.Close()
	var rs []*kgo.Record
	for i := int32(0); i < n; i++ {
		rs = append(rs, rec(topic, i, fmt.Sprintf("mp%d", i)))
	}
	ctx, cancel := ctxFor(60 * time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, rs...).FirstErr(); err != nil {
		return "", err
	}
	adm := kadm.NewClient(cl)
	md, err := adm.Metadata(ctx, topic)
	if err != nil {
		return "", err
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return "", err
	}
	var total int64
	ends.Each(func(o kadm.ListedOffset) { total += o.Offset })
	parts := map[string]map[int32]kgo.Offset{topic: {}}
	for i := int32(0); i < n; i++ {
		parts[topic][i] = start()
	}
	c, err := newClient(broker, kgo.ConsumePartitions(parts))
	if err != nil {
		return "", err
	}
	defer c.Close()
	got, err := poll(c, n, 0)
	if err != nil {
		return "", err
	}
	seen := map[int32]bool{}
	for _, r := range got {
		seen[r.Partition] = true
	}
	return fmt.Sprintf("described=%d end_sum=%d read_partitions=%d", len(md.Topics[topic].Partitions), total, len(seen)), nil
}

func manyRecords(broker, topic string) (string, error) {
	const n = 20000
	cl, err := newClient(broker, kgo.ProducerLinger(20*time.Millisecond), kgo.MaxBufferedRecords(n))
	if err != nil {
		return "", err
	}
	defer cl.Close()
	var sent [][]byte
	var rs []*kgo.Record
	for i := 0; i < n; i++ {
		v := []byte(strings.Repeat(fmt.Sprintf("%08d-", i), 10))
		sent = append(sent, v)
		rs = append(rs, &kgo.Record{Topic: topic, Value: v})
	}
	ctx, cancel := ctxFor(120 * time.Second)
	err = cl.ProduceSync(ctx, rs...).FirstErr()
	cancel()
	if err != nil {
		return "", err
	}
	got, err := read(broker, topic, 0, start(), n, 0)
	if err != nil {
		return "", err
	}
	var rd [][]byte
	for _, r := range got {
		rd = append(rd, r.Value)
	}
	return fmt.Sprintf("count=%d crc=%s same=%v", len(rd), crc(rd), crc(rd) == crc(sent)), nil
}

func seekIntoBatch(broker, topic string) (string, error) {
	t0 := time.UnixMilli(1_700_000_000_000)
	cl, err := newClient(broker, kgo.ProducerLinger(time.Second))
	if err != nil {
		return "", err
	}
	defer cl.Close()
	var rs []*kgo.Record
	for i := 0; i < 100; i++ {
		rs = append(rs, &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("b%d", i)), Timestamp: t0.Add(time.Duration(i) * time.Millisecond)})
	}
	ctx, cancel := ctxFor(60 * time.Second)
	defer cancel()
	res := cl.ProduceSync(ctx, rs...)
	if err := res.FirstErr(); err != nil {
		return "", err
	}
	contiguous := true
	for i, r := range res {
		contiguous = contiguous && r.Record.Offset == int64(i)
	}
	at := func(off int64, want int) ([]*kgo.Record, error) {
		return read(broker, topic, 0, kgo.NewOffset().At(off), want, 3*time.Second)
	}
	first, err := at(37, 1)
	if err != nil {
		return "", err
	}
	five, err := at(37, 5)
	if err != nil {
		return "", err
	}
	last, err := at(99, 1)
	if err != nil {
		return "", err
	}
	past, err := at(100, 0)
	if err != nil {
		return "", err
	}
	l, err := kadm.NewClient(cl).ListOffsetsAfterMilli(ctx, t0.UnixMilli()+50, topic)
	if err != nil {
		return "", err
	}
	mid, _ := l.Lookup(topic, 0)
	return fmt.Sprintf("contiguous=%v first=%d five=%s last=%s past_end=%d by_time_mid=%d", contiguous, first[0].Offset,
		values(five[:5]), values(last), len(past), mid.Offset), nil
}

var fastMetadata = []kgo.Opt{kgo.MetadataMaxAge(time.Second), kgo.MetadataMinAge(250 * time.Millisecond)}

func groupPartitionGrowth(broker, topic string) (string, error) {
	g := topic + "-g"
	m, err := startMember(broker, topic, g, append([]kgo.Opt{eager}, fastMetadata...)...)
	if err != nil {
		return "", err
	}
	defer m.close()
	adm, closeAdm, err := admin(broker)
	if err != nil {
		return "", err
	}
	defer closeAdm()
	deadline := time.Now().Add(60 * time.Second)
	for len(m.ownedSet()) == 0 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	before := len(m.ownedSet())
	ctx, cancel := ctxFor(30 * time.Second)
	defer cancel()
	if r, err := adm.UpdatePartitions(ctx, 3, topic); err != nil || r[topic].Err != nil {
		return "", fmt.Errorf("grow: %v %v", err, r[topic].Err)
	}
	for len(m.ownedSet()) < 3 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	after := len(m.ownedSet())
	if err := produceSpread(broker, topic, 9); err != nil {
		return "", err
	}
	return fmt.Sprintf("before=%d after=%d %s", before, after, consumeAll(9, m)), nil
}

func compactedNullKey(broker, topic string) (string, error) {
	adm, closeAdm, err := admin(broker)
	if err != nil {
		return "", err
	}
	defer closeAdm()
	ctx, cancel := ctxFor(60 * time.Second)
	defer cancel()
	t := topic + "-c"
	cr, err := adm.CreateTopic(ctx, 1, 1, map[string]*string{"cleanup.policy": ptr("compact")}, t)
	if err != nil || cr.Err != nil {
		return "", fmt.Errorf("create: %v %v", err, cr.Err)
	}
	cl, err := newClient(broker)
	if err != nil {
		return "", err
	}
	defer cl.Close()
	nullKey := cl.ProduceSync(ctx, &kgo.Record{Topic: t, Value: []byte("v")}).FirstErr()
	keyed := cl.ProduceSync(ctx, &kgo.Record{Topic: t, Key: []byte("k"), Value: []byte("v")})
	if err := keyed.FirstErr(); err != nil {
		return "", err
	}
	tomb := cl.ProduceSync(ctx, &kgo.Record{Topic: t, Key: []byte("k")})
	if err := tomb.FirstErr(); err != nil {
		return "", err
	}
	return fmt.Sprintf("null_key=%s keyed_offset=%d tombstone_offset=%d", errName(nullKey), keyed[0].Record.Offset, tomb[0].Record.Offset), nil
}
