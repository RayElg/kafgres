// A franz-go scenario runner for the conformance suite. franz-go speaks the highest version
// both sides support and treats a missing API key as an old broker. One line per scenario.
package main

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type scenarioFunc func(broker, topic string) (string, error)

func main() {
	if len(os.Args) < 4 {
		fmt.Println("usage: franz-conformance <broker> <scenario> <topic>")
		os.Exit(2)
	}
	broker, scenario, topic := os.Args[1], os.Args[2], os.Args[3]
	run, ok := scenarios()[scenario]
	if !ok {
		fmt.Printf("ERROR unknown scenario %s\n", scenario)
		os.Exit(2)
	}
	line, err := run(broker, topic)
	if err != nil {
		fmt.Printf("ERROR %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK " + line)
}

// errName is the error a program would branch on: the broker's code, never the text.
func errName(err error) string {
	if err == nil {
		return "none"
	}
	var ke *kerr.Error
	if errors.As(err, &ke) {
		return fmt.Sprintf("kerr%d", ke.Code)
	}
	return strings.SplitN(err.Error(), ":", 2)[0]
}

func str(b []byte) string {
	if b == nil {
		return "~"
	}
	return string(b)
}

func join[T any](xs []T) string {
	out := make([]string, len(xs))
	for i, x := range xs {
		out[i] = fmt.Sprint(x)
	}
	return strings.Join(out, ",")
}

func crc(parts [][]byte) string {
	var c uint32
	for _, p := range parts {
		c = crc32.Update(c, crc32.IEEETable, p)
	}
	return fmt.Sprintf("%x", c)
}

func ptr(s string) *string { return &s }

func newClient(broker string, opts ...kgo.Opt) (*kgo.Client, error) {
	base := []kgo.Opt{
		kgo.SeedBrokers(broker),
		kgo.ClientID("franz-conformance"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		// No RecordDeliveryTimeout: franz-go measures it from the record timestamp, and
		// scenarios backdate records.
		kgo.RetryTimeout(40 * time.Second),
	}
	return kgo.NewClient(append(base, opts...)...)
}

func ctxFor(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

func rec(topic string, partition int32, v string) *kgo.Record {
	return &kgo.Record{Topic: topic, Partition: partition, Value: []byte(v)}
}

func produceOne(cl *kgo.Client, topic string, partition int32, v string) (int64, error) {
	ctx, cancel := ctxFor(40 * time.Second)
	defer cancel()
	res := cl.ProduceSync(ctx, rec(topic, partition, v))
	if err := res.FirstErr(); err != nil {
		return -1, err
	}
	return res[0].Record.Offset, nil
}

func values(rs []*kgo.Record) string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = str(r.Value)
	}
	return strings.Join(out, ",")
}

// read consumes one partition from `from` until `want` records arrive (want 0: until quiet).
func read(broker, topic string, partition int32, from kgo.Offset, want int, quiet time.Duration,
	extra ...kgo.Opt) ([]*kgo.Record, error) {
	opts := append([]kgo.Opt{
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {partition: from}}),
	}, extra...)
	cl, err := newClient(broker, opts...)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	return poll(cl, want, quiet)
}

func poll(cl *kgo.Client, want int, quiet time.Duration) ([]*kgo.Record, error) {
	var got []*kgo.Record
	deadline := time.Now().Add(60 * time.Second)
	// The first fetch includes connecting and resolving offsets; afterwards `quiet` applies.
	wait := 20 * time.Second
	for time.Now().Before(deadline) && (want <= 0 || len(got) < want) {
		ctx, cancel := ctxFor(wait)
		fetches := cl.PollFetches(ctx)
		cancel()
		if fetches.IsClientClosed() {
			break
		}
		var fatal error
		fetches.EachError(func(_ string, _ int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				fatal = err
			}
		})
		if fatal != nil {
			return got, fatal
		}
		n := len(got)
		fetches.EachRecord(func(r *kgo.Record) { got = append(got, r) })
		if len(got) == n && want <= 0 {
			break
		}
		if want <= 0 {
			wait = quiet
		}
	}
	if want > 0 && len(got) < want {
		return got, fmt.Errorf("timed out after %d of %d records", len(got), want)
	}
	return got, nil
}

func start() kgo.Offset { return kgo.NewOffset().AtStart() }

func committed() []kgo.Opt { return []kgo.Opt{kgo.FetchIsolationLevel(kgo.ReadCommitted())} }

func admin(broker string) (*kadm.Client, func(), error) {
	cl, err := newClient(broker)
	if err != nil {
		return nil, nil, err
	}
	return kadm.NewClient(cl), cl.Close, nil
}

func scenarios() map[string]scenarioFunc {
	return map[string]scenarioFunc{
		"produce-consume":               produceConsume,
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
		"group-cooperative":             groupCooperative,
		"group-admin":                   groupAdmin,
		"admin-topics":                  adminTopics,
		"admin-configs":                 adminConfigs,
		"admin-cluster":                 adminCluster,
		"admin-delete-records":          adminDeleteRecords,
		"admin-offsets":                 adminOffsets,
		"admin-producers":               adminProducers,
		"admin-quotas":                  adminQuotas,
		"metadata":                      metadata,
		"unknown-topic":                 unknownTopic,
	}
}

func produceConsume(broker, topic string) (string, error) {
	cl, err := newClient(broker)
	if err != nil {
		return "", err
	}
	defer cl.Close()
	var offs []int64
	for _, v := range []string{"f1", "f2", "f3"} {
		o, err := produceOne(cl, topic, 0, v)
		if err != nil {
			return "", err
		}
		offs = append(offs, o)
	}
	got, err := read(broker, topic, 0, start(), 3, 0)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("offsets=%s values=%s", join(offs), values(got)), nil
}

func recordsShape(broker, topic string) (string, error) {
	cl, err := newClient(broker)
	if err != nil {
		return "", err
	}
	defer cl.Close()
	t0 := time.UnixMilli(1_700_000_000_000)
	rs := []*kgo.Record{
		{Key: []byte("k1"), Value: []byte("v1"), Timestamp: t0,
			Headers: []kgo.RecordHeader{{Key: "a", Value: []byte("1")}, {Key: "b", Value: []byte("2")}}},
		{Value: []byte("v2"), Timestamp: t0.Add(time.Second)},
		{Key: []byte("k3"), Timestamp: t0.Add(2 * time.Second)},
		{Key: []byte{}, Value: []byte{}, Timestamp: t0.Add(3 * time.Second)},
		{Key: []byte("k5"), Value: []byte("v5"), Timestamp: t0.Add(4 * time.Second),
			Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("x")}, {Key: "h", Value: []byte("y")}, {Key: "n"}}},
	}
	for _, r := range rs {
		r.Topic = topic
		ctx, cancel := ctxFor(40 * time.Second)
		err := cl.ProduceSync(ctx, r).FirstErr()
		cancel()
		if err != nil {
			return "", fmt.Errorf("produce: %w", err)
		}
	}
	got, err := read(broker, topic, 0, start(), 5, 0)
	if err != nil {
		return "", err
	}
	var out []string
	for _, r := range got {
		var h strings.Builder
		for _, x := range r.Headers {
			fmt.Fprintf(&h, "%s=%s;", x.Key, str(x.Value))
		}
		out = append(out, fmt.Sprintf("%s:%s@%d[%s]", str(r.Key), str(r.Value), r.Timestamp.UnixMilli(), h.String()))
	}
	return "records=" + strings.Join(out, " "), nil
}

func compression(broker, topic string) (string, error) {
	codecs := []struct {
		name  string
		codec kgo.CompressionCodec
	}{
		{"none", kgo.NoCompression()}, {"gzip", kgo.GzipCompression()}, {"snappy", kgo.SnappyCompression()},
		{"lz4", kgo.Lz4Compression()}, {"zstd", kgo.ZstdCompression()},
	}
	var out []string
	for _, cd := range codecs {
		cl, err := newClient(broker, kgo.ProducerBatchCompression(cd.codec), kgo.ProducerLinger(100*time.Millisecond))
		if err != nil {
			return "", err
		}
		var sent [][]byte
		var rs []*kgo.Record
		for i := 0; i < 20; i++ {
			v := []byte(strings.Repeat(fmt.Sprintf("payload-%s-%d-", cd.name, i), 40))
			sent = append(sent, v)
			rs = append(rs, &kgo.Record{Topic: topic, Value: v})
		}
		ctx, cancel := ctxFor(40 * time.Second)
		res := cl.ProduceSync(ctx, rs...)
		cancel()
		cl.Close()
		if err := res.FirstErr(); err != nil {
			return "", fmt.Errorf("%s produce: %w", cd.name, err)
		}
		first := res[0].Record.Offset
		got, err := read(broker, topic, 0, kgo.NewOffset().At(first), 20, 0)
		if err != nil {
			return "", fmt.Errorf("%s read: %w", cd.name, err)
		}
		var read [][]byte
		for _, r := range got {
			read = append(read, r.Value)
		}
		mark := ""
		if crc(read) != crc(sent) {
			mark = ":MISMATCH"
		}
		out = append(out, fmt.Sprintf("%s=%d:%s%s", cd.name, len(read), crc(read), mark))
	}
	return strings.Join(out, " "), nil
}

func largeRecords(broker, topic string) (string, error) {
	cl, err := newClient(broker, kgo.ProducerLinger(50*time.Millisecond))
	if err != nil {
		return "", err
	}
	big := make([]byte, 900_000)
	for i := range big {
		big[i] = byte(i * 31)
	}
	sent := [][]byte{big}
	rs := []*kgo.Record{{Topic: topic, Value: big}}
	for i := 0; i < 200; i++ {
		v := make([]byte, 10_000)
		for j := range v {
			v[j] = byte(i)
		}
		sent = append(sent, v)
		rs = append(rs, &kgo.Record{Topic: topic, Value: v})
	}
	ctx, cancel := ctxFor(60 * time.Second)
	err = cl.ProduceSync(ctx, rs...).FirstErr()
	cancel()
	cl.Close()
	if err != nil {
		return "", fmt.Errorf("produce: %w", err)
	}
	got, err := read(broker, topic, 0, start(), len(sent), 0)
	if err != nil {
		return "", err
	}
	var read [][]byte
	n := 0
	for _, r := range got {
		read = append(read, r.Value)
		n += len(r.Value)
	}
	return fmt.Sprintf("count=%d bytes=%d crc=%s same=%v", len(read), n, crc(read), crc(read) == crc(sent)), nil
}

func recordTooLarge(broker, topic string) (string, error) {
	cl, err := newClient(broker, kgo.ProducerBatchMaxBytes(8<<20), kgo.MaxBufferedBytes(64<<20),
		// Zeros compress to almost nothing, and the broker's limit is on the compressed batch.
		kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err != nil {
		return "", err
	}
	defer cl.Close()
	ctx, cancel := ctxFor(40 * time.Second)
	res := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: make([]byte, 2<<20)})
	cancel()
	next, err := produceOne(cl, topic, 0, "after")
	if err != nil {
		return "", fmt.Errorf("send after: %w", err)
	}
	return fmt.Sprintf("error=%s next_offset=%d", errName(res.FirstErr()), next), nil
}

func multiPartition(broker, topic string) (string, error) {
	cl, err := newClient(broker)
	if err != nil {
		return "", err
	}
	defer cl.Close()
	var offs, reads []string
	for p := int32(0); p < 3; p++ {
		var o []int64
		for i := 0; i < 5; i++ {
			off, err := produceOne(cl, topic, p, fmt.Sprintf("p%d-%d", p, i))
			if err != nil {
				return "", err
			}
			o = append(o, off)
		}
		offs = append(offs, fmt.Sprintf("p%d=%s", p, join(o)))
		got, err := read(broker, topic, p, start(), 5, 0)
		if err != nil {
			return "", err
		}
		reads = append(reads, fmt.Sprintf("%d=[%s]", p, values(got)))
	}
	adm := kadm.NewClient(cl)
	ctx, cancel := ctxFor(30 * time.Second)
	defer cancel()
	begin, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return "", err
	}
	end, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return "", err
	}
	var b, e []string
	for p := int32(0); p < 3; p++ {
		bo, _ := begin.Lookup(topic, p)
		eo, _ := end.Lookup(topic, p)
		b = append(b, fmt.Sprintf("%d=%d", p, bo.Offset))
		e = append(e, fmt.Sprintf("%d=%d", p, eo.Offset))
	}
	md, err := adm.Metadata(ctx, topic)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s read=%s begin=%s end=%s partitions=%d", strings.Join(offs, ","), strings.Join(reads, " "),
		strings.Join(b, ","), strings.Join(e, ","), len(md.Topics[topic].Partitions)), nil
}

func offsetsByTime(broker, topic string) (string, error) {
	cl, err := newClient(broker)
	if err != nil {
		return "", err
	}
	defer cl.Close()
	t0 := time.UnixMilli(1_700_000_000_000)
	for i := 0; i < 4; i++ {
		ctx, cancel := ctxFor(40 * time.Second)
		err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("t%d", i)),
			Timestamp: t0.Add(time.Duration(i) * time.Second)}).FirstErr()
		cancel()
		if err != nil {
			return "", err
		}
	}
	adm := kadm.NewClient(cl)
	ctx, cancel := ctxFor(30 * time.Second)
	defer cancel()
	var out []string
	for _, off := range []int64{-1, 0, 1500, 3000, 5000} {
		l, err := adm.ListOffsetsAfterMilli(ctx, t0.UnixMilli()+off, topic)
		if err != nil {
			return "", err
		}
		o, _ := l.Lookup(topic, 0)
		out = append(out, fmt.Sprintf("%d->%d(%s)", off, o.Offset, errName(o.Err)))
	}
	s, _ := adm.ListStartOffsets(ctx, topic)
	e, _ := adm.ListEndOffsets(ctx, topic)
	so, _ := s.Lookup(topic, 0)
	eo, _ := e.Lookup(topic, 0)
	return fmt.Sprintf("by_time=%s begin=%d end=%d", strings.Join(out, " "), so.Offset, eo.Offset), nil
}

func seek(broker, topic string) (string, error) {
	cl, err := newClient(broker)
	if err != nil {
		return "", err
	}
	defer cl.Close()
	for i := 0; i < 10; i++ {
		if _, err := produceOne(cl, topic, 0, fmt.Sprintf("r%d", i)); err != nil {
			return "", err
		}
	}
	tail, err := read(broker, topic, 0, kgo.NewOffset().At(5), 5, 0)
	if err != nil {
		return "", fmt.Errorf("tail: %w", err)
	}
	// The end offset is resolved by the first fetch, so poll once (empty) before producing.
	endc, err := newClient(broker, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtEnd()}}))
	if err != nil {
		return "", err
	}
	defer endc.Close()
	pctx, pcancel := ctxFor(3 * time.Second)
	endc.PollFetches(pctx)
	pcancel()
	if _, err := produceOne(cl, topic, 0, "r10"); err != nil {
		return "", err
	}
	live, err := poll(endc, 1, 0)
	if err != nil {
		return "", fmt.Errorf("live: %w", err)
	}
	third, err := read(broker, topic, 0, kgo.NewOffset().At(3), 1, 0)
	if err != nil {
		return "", fmt.Errorf("third: %w", err)
	}
	return fmt.Sprintf("tail=%s live=%s third=%d:%s", values(tail), values(live), third[0].Offset, str(third[0].Value)), nil
}

func longPoll(broker, topic string) (string, error) {
	cl, err := newClient(broker, kgo.FetchMaxWait(1500*time.Millisecond),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: start()}}))
	if err != nil {
		return "", err
	}
	defer cl.Close()
	ctx, cancel := ctxFor(3 * time.Second)
	empty := cl.PollFetches(ctx).NumRecords()
	cancel()
	p, err := newClient(broker)
	if err != nil {
		return "", err
	}
	defer p.Close()
	started := time.Now()
	if _, err := produceOne(p, topic, 0, "late"); err != nil {
		return "", err
	}
	late, err := poll(cl, 1, 0)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("empty=%d late=%s prompt=%v", empty, values(late), time.Since(started) < 15*time.Second), nil
}

func offsetOutOfRange(broker, topic string) (string, error) {
	p, err := newClient(broker)
	if err != nil {
		return "", err
	}
	defer p.Close()
	for i := 0; i < 3; i++ {
		if _, err := produceOne(p, topic, 0, fmt.Sprintf("o%d", i)); err != nil {
			return "", err
		}
	}
	var out []string
	for _, policy := range []struct {
		name  string
		reset kgo.Offset
	}{{"none", kgo.NoResetOffset()}, {"earliest", kgo.NewOffset().AtStart()}, {"latest", kgo.NewOffset().AtEnd()}} {
		got, err := read(broker, topic, 0, kgo.NewOffset().At(99), 0, 4*time.Second, kgo.ConsumeResetOffset(policy.reset))
		out = append(out, fmt.Sprintf("%s:read=%s,err=%s", policy.name, values(got), errName(err)))
	}
	return strings.Join(out, " "), nil
}

func idempotentProduce(broker, topic string) (string, error) {
	cl, err := newClient(broker, kgo.ProducerLinger(5*time.Millisecond))
	if err != nil {
		return "", err
	}
	defer cl.Close()
	var rs []*kgo.Record
	for i := 0; i < 50; i++ {
		rs = append(rs, rec(topic, 0, fmt.Sprintf("i%d", i)))
	}
	ctx, cancel := ctxFor(40 * time.Second)
	res := cl.ProduceSync(ctx, rs...)
	cancel()
	if err := res.FirstErr(); err != nil {
		return "", err
	}
	contiguous := true
	for i, r := range res {
		contiguous = contiguous && r.Record.Offset == int64(i)
	}
	got, err := read(broker, topic, 0, start(), 50, 0)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("contiguous=%v first=%d last=%d read=%d", contiguous, res[0].Record.Offset, res[49].Record.Offset, len(got)), nil
}

func acks(broker, topic string) (string, error) {
	var out []string
	for _, a := range []struct {
		name string
		opts []kgo.Opt
	}{
		{"0", []kgo.Opt{kgo.RequiredAcks(kgo.NoAck()), kgo.DisableIdempotentWrite()}},
		{"1", []kgo.Opt{kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite()}},
		{"all", nil},
	} {
		cl, err := newClient(broker, a.opts...)
		if err != nil {
			return "", err
		}
		off, err := produceOne(cl, topic, 0, "acks-"+a.name)
		cl.Close()
		if err != nil {
			return "", fmt.Errorf("acks=%s: %w", a.name, err)
		}
		if a.name == "0" {
			out = append(out, "acks0")
		} else {
			out = append(out, fmt.Sprintf("acks%s@%d", a.name, off))
		}
	}
	got, err := read(broker, topic, 0, start(), 3, 0)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s read=%s", strings.Join(out, " "), values(got)), nil
}

func txnClient(broker, id string, extra ...kgo.Opt) (*kgo.Client, error) {
	return newClient(broker, append([]kgo.Opt{kgo.TransactionalID(id), kgo.TransactionTimeout(60 * time.Second)}, extra...)...)
}

func endTxn(cl *kgo.Client, commit bool) error {
	ctx, cancel := ctxFor(40 * time.Second)
	defer cancel()
	want := kgo.TryAbort
	if commit {
		want = kgo.TryCommit
	}
	return cl.EndTransaction(ctx, want)
}

// state is what a consumer at the given isolation reads from the start of partition 0.
func state(broker, topic string, extra ...kgo.Opt) string {
	got, err := read(broker, topic, 0, start(), 0, 4*time.Second, extra...)
	return fmt.Sprintf("values=%s err=%s", values(got), errName(err))
}

func txnReadCommitted(broker, topic string) (string, error) {
	t, err := txnClient(broker, "franz-rc-"+topic)
	if err != nil {
		return "", err
	}
	defer t.Close()
	plain, err := newClient(broker)
	if err != nil {
		return "", err
	}
	defer plain.Close()
	send := func(vs ...string) error {
		for _, v := range vs {
			if _, err := produceOne(t, topic, 0, v); err != nil {
				return fmt.Errorf("send %s: %w", v, err)
			}
		}
		return nil
	}
	steps := []struct {
		vals   []string
		commit bool
	}{{[]string{"a0", "a1"}, true}, {[]string{"b0", "b1"}, false}}
	for _, s := range steps {
		if err := t.BeginTransaction(); err != nil {
			return "", err
		}
		if err := send(s.vals...); err != nil {
			return "", err
		}
		if err := endTxn(t, s.commit); err != nil {
			return "", fmt.Errorf("end: %w", err)
		}
	}
	if _, err := produceOne(plain, topic, 0, "n0"); err != nil {
		return "", err
	}
	if err := t.BeginTransaction(); err != nil {
		return "", err
	}
	if err := send("c0"); err != nil {
		return "", err
	}
	if err := endTxn(t, true); err != nil {
		return "", err
	}
	if err := t.BeginTransaction(); err != nil {
		return "", err
	}
	if err := send("o0"); err != nil {
		return "", err
	}
	settled := state(broker, topic, committed()...)
	open := state(broker, topic)
	if err := endTxn(t, true); err != nil {
		return "", err
	}
	after := state(broker, topic, committed()...)
	return fmt.Sprintf("settled=[%s] open_uncommitted=[%s] after_commit=[%s]", settled, open, after), nil
}

func committedOffset(adm *kadm.Client, group, topic string) string {
	out := "-1"
	for i := 0; i < 40 && out == "-1"; i++ {
		ctx, cancel := ctxFor(20 * time.Second)
		resp, err := adm.FetchOffsets(ctx, group)
		cancel()
		if err != nil {
			return errName(err)
		}
		if o, ok := resp.Lookup(topic, 0); ok && o.At >= 0 {
			out = fmt.Sprint(o.At)
		} else {
			time.Sleep(250 * time.Millisecond)
		}
	}
	return out
}

func txnConsumeTransformProduce(broker, topic string) (string, error) {
	outTopic, group := topic+"-out", topic+"-g"
	adm, closeAdm, err := admin(broker)
	if err != nil {
		return "", err
	}
	defer closeAdm()
	ctx, cancel := ctxFor(30 * time.Second)
	cr, err := adm.CreateTopic(ctx, 1, 1, nil, outTopic)
	cancel()
	if err != nil || cr.Err != nil {
		return "", fmt.Errorf("create %s: %v %v", outTopic, err, cr.Err)
	}
	plain, err := newClient(broker)
	if err != nil {
		return "", err
	}
	defer plain.Close()
	for i := 0; i < 5; i++ {
		if _, err := produceOne(plain, topic, 0, fmt.Sprintf("in%d", i)); err != nil {
			return "", err
		}
	}
	sess, err := kgo.NewGroupTransactSession(
		kgo.SeedBrokers(broker), kgo.ClientID("franz-conformance"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.TransactionalID("franz-cpt-"+topic), kgo.TransactionTimeout(60*time.Second),
		kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(start()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		return "", err
	}
	defer sess.Close()

	batch := func(want int) ([]*kgo.Record, error) {
		var got []*kgo.Record
		deadline := time.Now().Add(60 * time.Second)
		for len(got) < want && time.Now().Before(deadline) {
			pctx, pcancel := ctxFor(5 * time.Second)
			f := sess.PollFetches(pctx)
			pcancel()
			f.EachRecord(func(r *kgo.Record) { got = append(got, r) })
		}
		if len(got) < want {
			return got, fmt.Errorf("polled %d of %d", len(got), want)
		}
		return got, nil
	}
	transact := func(in []*kgo.Record, outValue func(*kgo.Record) string, commit bool) (bool, error) {
		if err := sess.Begin(); err != nil {
			return false, err
		}
		for _, r := range in {
			pctx, pcancel := ctxFor(40 * time.Second)
			err := sess.ProduceSync(pctx, &kgo.Record{Topic: outTopic, Value: []byte(outValue(r))}).FirstErr()
			pcancel()
			if err != nil {
				return false, err
			}
		}
		ectx, ecancel := ctxFor(40 * time.Second)
		defer ecancel()
		want := kgo.TryAbort
		if commit {
			want = kgo.TryCommit
		}
		return sess.End(ectx, want)
	}

	first, err := batch(5)
	if err != nil {
		return "", err
	}
	ok, err := transact(first, func(r *kgo.Record) string { return strings.ToUpper(string(r.Value)) }, true)
	if err != nil || !ok {
		return "", fmt.Errorf("commit: committed=%v err=%v", ok, err)
	}
	afterCommit := committedOffset(adm, group, topic)

	if _, err := produceOne(plain, topic, 0, "in5"); err != nil {
		return "", err
	}
	second, err := batch(1)
	if err != nil {
		return "", err
	}
	if _, err := transact(second, func(*kgo.Record) string { return "SHOULD-NOT-APPEAR" }, false); err != nil {
		return "", fmt.Errorf("abort: %w", err)
	}
	afterAbort := committedOffset(adm, group, topic)
	got, err := read(broker, outTopic, 0, start(), 0, 4*time.Second, committed()...)
	if err != nil {
		return "", fmt.Errorf("read output: %w", err)
	}
	return fmt.Sprintf("processed=%d committed_after_commit=%s second_batch=%d committed_after_abort=%s output=%s",
		len(first), afterCommit, len(second), afterAbort, values(got)), nil
}

func txnFenced(broker, topic string) (string, error) {
	id := "franz-fence-" + topic
	first, err := txnClient(broker, id)
	if err != nil {
		return "", err
	}
	defer first.Close()
	if err := first.BeginTransaction(); err != nil {
		return "", err
	}
	if _, err := produceOne(first, topic, 0, "stale"); err != nil {
		return "", err
	}
	// InitProducerId over an open transaction aborts it and answers CONCURRENT_TRANSACTIONS
	// until the abort completes. franz-go treats that as fatal, so retry with fresh clients.
	var second *kgo.Client
	for i := 0; i < 40; i++ {
		c, err := txnClient(broker, id)
		if err != nil {
			return "", err
		}
		if err = c.BeginTransaction(); err == nil {
			second = c
			break
		}
		c.Close()
		if !strings.Contains(err.Error(), "CONCURRENT_TRANSACTIONS") {
			return "", err
		}
		time.Sleep(250 * time.Millisecond)
	}
	if second == nil {
		return "", fmt.Errorf("could not take over the transactional id")
	}
	defer second.Close()
	if _, err := produceOne(second, topic, 0, "fresh"); err != nil {
		return "", err
	}
	failed := "none"
	if _, err := produceOne(first, topic, 0, "stale-2"); err != nil {
		failed = errName(err)
	} else if err := endTxn(first, true); err != nil {
		failed = errName(err)
	}
	if err := endTxn(second, true); err != nil {
		return "", fmt.Errorf("second commit: %w", err)
	}
	return fmt.Sprintf("first_failed_with=%s read_committed=[%s]", failed, state(broker, topic, committed()...)), nil
}

type member struct {
	cl      *kgo.Client
	mu      sync.Mutex
	owned   map[int32]bool
	revoked []int
	got     []string
	errs    map[string]bool
	done    chan struct{}
}

func startMember(broker, topic, group string, extra ...kgo.Opt) (*member, error) {
	m := &member{owned: map[int32]bool{}, errs: map[string]bool{}, done: make(chan struct{})}
	opts := append([]kgo.Opt{
		kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(start()),
		kgo.SessionTimeout(30 * time.Second), kgo.HeartbeatInterval(3 * time.Second),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, a map[string][]int32) {
			m.mu.Lock()
			defer m.mu.Unlock()
			for _, ps := range a {
				for _, p := range ps {
					m.owned[p] = true
				}
			}
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, r map[string][]int32) {
			m.mu.Lock()
			defer m.mu.Unlock()
			n := 0
			for _, ps := range r {
				for _, p := range ps {
					delete(m.owned, p)
					n++
				}
			}
			m.revoked = append(m.revoked, n)
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, l map[string][]int32) {
			m.mu.Lock()
			defer m.mu.Unlock()
			for _, ps := range l {
				for _, p := range ps {
					delete(m.owned, p)
				}
			}
		}),
	}, extra...)
	cl, err := newClient(broker, opts...)
	if err != nil {
		return nil, err
	}
	m.cl = cl
	go func() {
		defer close(m.done)
		for {
			ctx, cancel := ctxFor(time.Second)
			f := cl.PollFetches(ctx)
			cancel()
			if f.IsClientClosed() {
				return
			}
			f.EachError(func(_ string, _ int32, err error) {
				if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
					m.mu.Lock()
					m.errs[errName(err)] = true
					m.mu.Unlock()
				}
			})
			f.EachRecord(func(r *kgo.Record) {
				m.mu.Lock()
				m.got = append(m.got, string(r.Value))
				m.mu.Unlock()
			})
		}
	}()
	return m, nil
}

func (m *member) close() {
	m.cl.Close()
	<-m.done
}

func (m *member) ownedSet() map[int32]bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[int32]bool{}
	for p := range m.owned {
		out[p] = true
	}
	return out
}

func (m *member) received() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.got...)
}

func (m *member) errNames() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n []string
	for k := range m.errs {
		n = append(n, k)
	}
	sort.Strings(n)
	if len(n) == 0 {
		return "none"
	}
	return strings.Join(n, "+")
}

func sameSet(a, b map[int32]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func settle(partitions int, ms ...*member) error {
	stable := 0
	deadline := time.Now().Add(90 * time.Second)
	for stable < 15 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
		all := map[int32]bool{}
		sum, ok := 0, true
		for _, m := range ms {
			o := m.ownedSet()
			if len(o) == 0 {
				ok = false
			}
			for p := range o {
				all[p] = true
			}
			sum += len(o)
		}
		if ok && len(all) == partitions && sum == partitions {
			stable++
		} else {
			stable = 0
		}
	}
	if stable < 15 {
		return fmt.Errorf("assignment never settled")
	}
	return nil
}

func sizes(ms ...*member) string {
	var n []int
	for _, m := range ms {
		n = append(n, len(m.ownedSet()))
	}
	sort.Ints(n)
	return join(n)
}

func produceSpread(broker, topic string, n int) error {
	cl, err := newClient(broker)
	if err != nil {
		return err
	}
	defer cl.Close()
	for i := 0; i < n; i++ {
		if _, err := produceOne(cl, topic, int32(i%3), fmt.Sprintf("m%d", i)); err != nil {
			return err
		}
	}
	return nil
}

func consumeAll(n int, ms ...*member) string {
	deadline := time.Now().Add(60 * time.Second)
	total := func() int {
		t := 0
		for _, m := range ms {
			t += len(m.received())
		}
		return t
	}
	for total() < n && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(1500 * time.Millisecond)
	uniq := map[string]bool{}
	all := 0
	for _, m := range ms {
		for _, v := range m.received() {
			uniq[v] = true
			all++
		}
	}
	return fmt.Sprintf("consumed=%d unique=%d", all, len(uniq))
}

var eager = kgo.Balancers(kgo.RangeBalancer())

func groupTwoMembers(broker, topic string) (string, error) {
	g := topic + "-g"
	a, err := startMember(broker, topic, g, eager)
	if err != nil {
		return "", err
	}
	defer a.close()
	b, err := startMember(broker, topic, g, eager)
	if err != nil {
		return "", err
	}
	defer b.close()
	if err := settle(3, a, b); err != nil {
		return "", err
	}
	before := sizes(a, b)
	if err := produceSpread(broker, topic, 30); err != nil {
		return "", err
	}
	return fmt.Sprintf("sizes=%s %s", before, consumeAll(30, a, b)), nil
}

func groupCooperative(broker, topic string) (string, error) {
	g := topic + "-g"
	coop := kgo.Balancers(kgo.CooperativeStickyBalancer())
	a, err := startMember(broker, topic, g, coop)
	if err != nil {
		return "", err
	}
	defer a.close()
	deadline := time.Now().Add(60 * time.Second)
	for len(a.ownedSet()) < 3 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	b, err := startMember(broker, topic, g, coop)
	if err != nil {
		return "", err
	}
	defer b.close()
	if err := settle(3, a, b); err != nil {
		return "", err
	}
	sz := sizes(a, b)
	// Under eager rebalancing A would give up all three; cooperative takes only what moves.
	everything := false
	a.mu.Lock()
	for _, n := range a.revoked {
		everything = everything || n == 3
	}
	a.mu.Unlock()
	if err := produceSpread(broker, topic, 12); err != nil {
		return "", err
	}
	return fmt.Sprintf("sizes=%s revoked_everything=%v %s", sz, everything, consumeAll(12, a, b)), nil
}

func groupRebalanceLeave(broker, topic string) (string, error) {
	g := topic + "-g"
	a, err := startMember(broker, topic, g, eager)
	if err != nil {
		return "", err
	}
	defer a.close()
	b, err := startMember(broker, topic, g, eager)
	if err != nil {
		return "", err
	}
	if err := settle(3, a, b); err != nil {
		b.close()
		return "", err
	}
	before := sizes(a, b)
	b.close()
	deadline := time.Now().Add(60 * time.Second)
	for len(a.ownedSet()) < 3 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	after := len(a.ownedSet())
	if err := produceSpread(broker, topic, 9); err != nil {
		return "", err
	}
	return fmt.Sprintf("before=%s after_leave=%d %s", before, after, consumeAll(9, a)), nil
}

func groupResume(broker, topic string) (string, error) {
	g := topic + "-g"
	p, err := newClient(broker)
	if err != nil {
		return "", err
	}
	defer p.Close()
	for i := 0; i < 5; i++ {
		if _, err := produceOne(p, topic, 0, fmt.Sprintf("first-%d", i)); err != nil {
			return "", err
		}
	}
	m, err := startMember(broker, topic, g, kgo.DisableAutoCommit())
	if err != nil {
		return "", err
	}
	deadline := time.Now().Add(60 * time.Second)
	for len(m.received()) < 5 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	first := len(m.received())
	cctx, ccancel := ctxFor(30 * time.Second)
	m.cl.CommitOffsetsSync(cctx, map[string]map[int32]kgo.EpochOffset{topic: {0: {Epoch: -1, Offset: 5}}},
		func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, e error) {
			err = e
			if e == nil && resp != nil {
				for _, t := range resp.Topics {
					for _, p := range t.Partitions {
						if p.ErrorCode != 0 {
							err = kerr.ErrorForCode(p.ErrorCode)
						}
					}
				}
			}
		})
	ccancel()
	m.close()
	if err != nil {
		return "", fmt.Errorf("commit: %w", err)
	}
	adm := kadm.NewClient(p)
	stored := committedOffset(adm, g, topic)
	for i := 0; i < 3; i++ {
		if _, err := produceOne(p, topic, 0, fmt.Sprintf("second-%d", i)); err != nil {
			return "", err
		}
	}
	m2, err := startMember(broker, topic, g)
	if err != nil {
		return "", err
	}
	defer m2.close()
	deadline = time.Now().Add(60 * time.Second)
	for len(m2.received()) < 3 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(1500 * time.Millisecond)
	return fmt.Sprintf("first=%d committed=%s second=%s", first, stored, strings.Join(m2.received(), ",")), nil
}

func groupStatic(broker, topic string) (string, error) {
	g := topic + "-g"
	a, err := startMember(broker, topic, g, eager, kgo.InstanceID("inst-a"))
	if err != nil {
		return "", err
	}
	defer a.close()
	b, err := startMember(broker, topic, g, eager, kgo.InstanceID("inst-b"))
	if err != nil {
		return "", err
	}
	if err := settle(3, a, b); err != nil {
		b.close()
		return "", err
	}
	bHad, aHad := b.ownedSet(), a.ownedSet()
	// A static member that goes away keeps its slot.
	b.close()
	time.Sleep(3 * time.Second)
	aStable := sameSet(a.ownedSet(), aHad)
	b2, err := startMember(broker, topic, g, eager, kgo.InstanceID("inst-b"))
	if err != nil {
		return "", err
	}
	defer b2.close()
	deadline := time.Now().Add(60 * time.Second)
	for len(b2.ownedSet()) == 0 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	same := sameSet(b2.ownedSet(), bHad)
	time.Sleep(2 * time.Second)
	aAfter := sameSet(a.ownedSet(), aHad)
	usurper, err := startMember(broker, topic, g, eager, kgo.InstanceID("inst-b"))
	if err != nil {
		return "", err
	}
	defer usurper.close()
	fenced := "none"
	until := time.Now().Add(30 * time.Second)
	for fenced == "none" && time.Now().Before(until) {
		time.Sleep(500 * time.Millisecond)
		if n := b2.errNames(); n != "none" {
			fenced = n
		}
	}
	return fmt.Sprintf("a_stable_while_b_away=%v b_same_assignment=%v a_stable_after=%v duplicate_instance=%s",
		aStable, same, aAfter, fenced), nil
}

func groupAdmin(broker, topic string) (string, error) {
	g := topic + "-g"
	p, err := newClient(broker)
	if err != nil {
		return "", err
	}
	defer p.Close()
	for i := 0; i < 6; i++ {
		if _, err := produceOne(p, topic, 0, fmt.Sprintf("ga%d", i)); err != nil {
			return "", err
		}
	}
	m, err := startMember(broker, topic, g)
	if err != nil {
		return "", err
	}
	defer m.close()
	deadline := time.Now().Add(60 * time.Second)
	for len(m.received()) < 6 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(6 * time.Second) // autocommit
	adm := kadm.NewClient(p)
	ctx, cancel := ctxFor(30 * time.Second)
	defer cancel()
	var out []string
	listed, err := adm.ListGroups(ctx)
	if err != nil {
		return "", fmt.Errorf("list groups: %w", err)
	}
	dg, err := adm.DescribeGroups(ctx, g)
	if err != nil {
		return "", fmt.Errorf("describe: %w", err)
	}
	d := dg[g]
	assigned := 0
	for _, ps := range d.AssignedPartitions() {
		assigned += len(ps)
	}
	out = append(out, fmt.Sprintf("listed=%v state=%s members=%d assigned=%d", listed.Groups()[0] == g || contains(listed.Groups(), g),
		d.State, len(d.Members), assigned))
	out = append(out, "offsets=0:"+committedOffset(adm, g, topic))
	dres, err := adm.DeleteGroups(ctx, g)
	if err != nil {
		return "", err
	}
	out = append(out, "delete_while_active="+errName(dres[g].Err))
	m.close()
	for i := 0; i < 40; i++ {
		d, err := adm.DescribeGroups(ctx, g)
		if err == nil && len(d[g].Members) == 0 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	del := kadm.TopicsSet{}
	del.Add(topic, 0)
	doff, err := adm.DeleteOffsets(ctx, g, del)
	if err != nil {
		out = append(out, "delete_offset="+errName(err))
	} else {
		out = append(out, "delete_offset="+errName(doff.Error()))
	}
	dres, err = adm.DeleteGroups(ctx, g)
	if err != nil {
		return "", err
	}
	out = append(out, "delete="+errName(dres[g].Err))
	listed, _ = adm.ListGroups(ctx)
	out = append(out, fmt.Sprintf("gone=%v", !contains(listed.Groups(), g)))
	return strings.Join(out, " "), nil
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

func adminTopics(broker, topic string) (string, error) {
	adm, closeAdm, err := admin(broker)
	if err != nil {
		return "", err
	}
	defer closeAdm()
	ctx, cancel := ctxFor(60 * time.Second)
	defer cancel()
	t := topic + "-a"
	var out []string
	cr, err := adm.CreateTopic(ctx, 3, 1, map[string]*string{"retention.ms": ptr("3600000")}, t)
	if err != nil || cr.Err != nil {
		return "", fmt.Errorf("create: %v %v", err, cr.Err)
	}
	try := func(name string, parts int32, rf int16, cfg map[string]*string, topic string) {
		r, err := adm.CreateTopic(ctx, parts, rf, cfg, topic)
		if err != nil {
			out = append(out, name+"="+errName(err))
			return
		}
		out = append(out, name+"="+errName(r.Err))
	}
	try("again", 3, 1, nil, t)
	try("zero_partitions", 0, 1, nil, t+"-z")
	try("rf2", 1, 2, nil, t+"-r")
	try("bad_config", 1, 1, map[string]*string{"no.such.config": ptr("1")}, t+"-c")
	try("bad_name", 1, 1, nil, "bad name!")

	md, err := adm.Metadata(ctx, t)
	if err != nil {
		return "", err
	}
	td := md.Topics[t]
	p0 := td.Partitions[0]
	out = append(out, fmt.Sprintf("partitions=%d rf=%d isr=%d internal=%v has_id=%v", len(td.Partitions),
		len(p0.Replicas), len(p0.ISR), td.IsInternal, td.ID != kadm.TopicID{}))
	lt, err := adm.ListTopics(ctx)
	if err != nil {
		return "", err
	}
	out = append(out, fmt.Sprintf("listed=%v", lt.Has(t)))
	cfgs, err := adm.DescribeTopicConfigs(ctx, t)
	if err != nil {
		return "", err
	}
	for _, rc := range cfgs {
		for _, c := range rc.Configs {
			if c.Key == "retention.ms" && c.Value != nil {
				out = append(out, fmt.Sprintf("retention.ms=%s/%d", *c.Value, c.Source))
			}
		}
	}
	gr, err := adm.CreatePartitions(ctx, 2, t)
	if err != nil {
		return "", err
	}
	out = append(out, "grow="+errName(gr[t].Err))
	md, _ = adm.Metadata(ctx, t)
	out = append(out, fmt.Sprintf("grown=%d", len(md.Topics[t].Partitions)))
	sr, err := adm.UpdatePartitions(ctx, 2, t)
	if err != nil {
		return "", err
	}
	out = append(out, "shrink="+errName(sr[t].Err))
	mr, err := adm.CreatePartitions(ctx, 2, t+"-nope")
	if err != nil {
		out = append(out, "grow_missing="+errName(err))
	} else {
		out = append(out, "grow_missing="+errName(mr[t+"-nope"].Err))
	}
	dr, err := adm.DeleteTopics(ctx, t)
	if err != nil {
		return "", err
	}
	out = append(out, "delete="+errName(dr[t].Err))
	dr, err = adm.DeleteTopics(ctx, t)
	if err != nil {
		return "", err
	}
	out = append(out, "delete_again="+errName(dr[t].Err))
	return strings.Join(out, " "), nil
}

func adminConfigs(broker, topic string) (string, error) {
	adm, closeAdm, err := admin(broker)
	if err != nil {
		return "", err
	}
	defer closeAdm()
	ctx, cancel := ctxFor(60 * time.Second)
	defer cancel()
	entry := func(key string) string {
		rcs, err := adm.DescribeTopicConfigs(ctx, topic)
		if err != nil {
			return errName(err)
		}
		for _, rc := range rcs {
			for _, c := range rc.Configs {
				if c.Key == key {
					v := "~"
					if c.Value != nil {
						v = *c.Value
					}
					return fmt.Sprintf("%s/%d", v, c.Source)
				}
			}
		}
		return "absent"
	}
	var out []string
	for _, k := range []string{"retention.ms", "cleanup.policy", "max.message.bytes", "segment.bytes",
		"min.compaction.lag.ms", "compression.type", "message.timestamp.type"} {
		out = append(out, k+"="+entry(k))
	}
	alter := func(op kadm.IncrementalOp, key string, v *string) string {
		res, err := adm.AlterTopicConfigs(ctx, []kadm.AlterConfig{{Op: op, Name: key, Value: v}}, topic)
		if err != nil {
			return errName(err)
		}
		for _, r := range res {
			return errName(r.Err)
		}
		return "empty"
	}
	if r := alter(kadm.SetConfig, "retention.ms", ptr("12345")); r != "none" {
		return "", fmt.Errorf("set: %s", r)
	}
	out = append(out, "after_set="+entry("retention.ms"))
	if r := alter(kadm.DeleteConfig, "retention.ms", nil); r != "none" {
		return "", fmt.Errorf("delete: %s", r)
	}
	out = append(out, "after_delete="+entry("retention.ms"))
	out = append(out, "unknown_key="+alter(kadm.SetConfig, "no.such.config", ptr("1")))
	out = append(out, "bad_value="+alter(kadm.SetConfig, "retention.ms", ptr("not-a-number")))
	missing, err := adm.DescribeTopicConfigs(ctx, topic+"-missing")
	if err != nil {
		out = append(out, "describe_missing="+errName(err))
	} else {
		code := "none"
		for _, rc := range missing {
			code = errName(rc.Err)
		}
		out = append(out, "describe_missing="+code)
	}
	brokers, err := adm.ListBrokers(ctx)
	if err != nil || len(brokers) == 0 {
		return "", fmt.Errorf("brokers: %v", err)
	}
	bc, err := adm.DescribeBrokerConfigs(ctx, brokers[0].NodeID)
	has := false
	for _, rc := range bc {
		has = has || len(rc.Configs) > 0
	}
	out = append(out, fmt.Sprintf("broker_config=%v", err == nil && has))
	return strings.Join(out, " "), nil
}

func adminCluster(broker, topic string) (string, error) {
	adm, closeAdm, err := admin(broker)
	if err != nil {
		return "", err
	}
	defer closeAdm()
	ctx, cancel := ctxFor(60 * time.Second)
	defer cancel()
	md, err := adm.BrokerMetadata(ctx)
	if err != nil {
		return "", err
	}
	listed := false
	var ids []int32
	for _, b := range md.Brokers {
		ids = append(ids, b.NodeID)
		listed = listed || b.NodeID == md.Controller
	}
	set := kadm.TopicsSet{}
	set.Add(topic, 0)
	el, err := adm.ElectLeaders(ctx, kadm.ElectPreferredReplica, set)
	elected := "none"
	if err != nil {
		elected = errName(err)
	} else {
		for _, parts := range el {
			for _, r := range parts {
				elected = errName(r.Err)
			}
		}
	}
	re, err := adm.ListPartitionReassignments(ctx, set)
	if err != nil {
		return "", fmt.Errorf("reassignments: %w", err)
	}
	dirs, err := adm.DescribeAllLogDirs(ctx, set)
	if err != nil {
		return "", fmt.Errorf("log dirs: %w", err)
	}
	covers := false
	dirs.Each(func(d kadm.DescribedLogDir) {
		d.Topics.Each(func(p kadm.DescribedLogDirPartition) {
			covers = covers || (p.Topic == topic && p.Partition == 0)
		})
	})
	versions, err := adm.ApiVersions(ctx)
	if err != nil {
		return "", fmt.Errorf("api versions: %w", err)
	}
	return fmt.Sprintf("nodes=%d controller_listed=%v cluster_id=%v elect_preferred=%s reassignments=%d log_dirs_cover_partition=%v api_versions=%v",
		len(md.Brokers), listed, md.Cluster != "", elected, len(re), covers, len(versions) == len(ids)), nil
}

func adminDeleteRecords(broker, topic string) (string, error) {
	cl, err := newClient(broker)
	if err != nil {
		return "", err
	}
	defer cl.Close()
	for i := 0; i < 10; i++ {
		if _, err := produceOne(cl, topic, 0, fmt.Sprintf("d%d", i)); err != nil {
			return "", err
		}
	}
	adm := kadm.NewClient(cl)
	ctx, cancel := ctxFor(60 * time.Second)
	defer cancel()
	del := func(at int64) string {
		offs := kadm.Offsets{}
		offs.Add(kadm.Offset{Topic: topic, Partition: 0, At: at})
		res, err := adm.DeleteRecords(ctx, offs)
		if err != nil {
			return errName(err)
		}
		r, _ := res.Lookup(topic, 0)
		return errName(r.Err)
	}
	var out []string
	out = append(out, "delete="+del(4))
	s, _ := adm.ListStartOffsets(ctx, topic)
	so, _ := s.Lookup(topic, 0)
	out = append(out, fmt.Sprintf("begin=%d", so.Offset))
	got, err := read(broker, topic, 0, start(), 6, 0)
	if err != nil {
		return "", err
	}
	out = append(out, "read="+values(got))
	out = append(out, "beyond_end="+del(99))
	out = append(out, "to_end="+del(-1))
	s, _ = adm.ListStartOffsets(ctx, topic)
	so, _ = s.Lookup(topic, 0)
	out = append(out, fmt.Sprintf("begin_after=%d", so.Offset))
	return strings.Join(out, " "), nil
}

func adminOffsets(broker, topic string) (string, error) {
	cl, err := newClient(broker)
	if err != nil {
		return "", err
	}
	defer cl.Close()
	t0 := time.UnixMilli(1_700_000_000_000)
	for i, off := range []int{2000, 0, 5000, 1000} {
		ctx, cancel := ctxFor(40 * time.Second)
		err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("x%d", i)),
			Timestamp: t0.Add(time.Duration(off) * time.Millisecond)}).FirstErr()
		cancel()
		if err != nil {
			return "", err
		}
	}
	adm := kadm.NewClient(cl)
	ctx, cancel := ctxFor(30 * time.Second)
	defer cancel()
	s, _ := adm.ListStartOffsets(ctx, topic)
	e, _ := adm.ListEndOffsets(ctx, topic)
	so, _ := s.Lookup(topic, 0)
	eo, _ := e.Lookup(topic, 0)
	out := []string{fmt.Sprintf("earliest=%d(%s)", so.Offset, errName(so.Err)), fmt.Sprintf("latest=%d(%s)", eo.Offset, errName(eo.Err))}
	for _, off := range []int64{1500, 99999} {
		l, err := adm.ListOffsetsAfterMilli(ctx, t0.UnixMilli()+off, topic)
		if err != nil {
			return "", err
		}
		o, _ := l.Lookup(topic, 0)
		out = append(out, fmt.Sprintf("at_t0+%d=%d(%s)", off, o.Offset, errName(o.Err)))
	}
	return strings.Join(out, " "), nil
}

func adminProducers(broker, topic string) (string, error) {
	var out []string
	id := "franz-admin-" + topic
	t, err := txnClient(broker, id)
	if err != nil {
		return "", err
	}
	defer t.Close()
	if err := t.BeginTransaction(); err != nil {
		return "", err
	}
	if _, err := produceOne(t, topic, 0, "open-a"); err != nil {
		return "", err
	}
	adm := kadm.NewClient(t)
	ctx, cancel := ctxFor(30 * time.Second)
	defer cancel()
	set := kadm.TopicsSet{}
	set.Add(topic, 0)
	dp, err := adm.DescribeProducers(ctx, set)
	if err != nil {
		return "", fmt.Errorf("describe producers: %w", err)
	}
	open := int64(-1)
	n := 0
	dp.Each(func(t kadm.DescribedProducersTopic) {
		for _, part := range t.Partitions {
			for _, p := range part.ActiveProducers {
				n++
				if p.CurrentTxnStartOffset >= 0 {
					open = p.CurrentTxnStartOffset
				}
			}
		}
	})
	out = append(out, fmt.Sprintf("producers=%d open_txn_start=%d", n, open))
	lt, err := adm.ListTransactions(ctx, nil, nil)
	if err != nil {
		return "", fmt.Errorf("list transactions: %w", err)
	}
	state := "absent"
	for _, tx := range lt {
		if tx.TxnID == id {
			state = tx.State
		}
	}
	out = append(out, "listed_state="+state)
	dt, err := adm.DescribeTransactions(ctx, id)
	if err != nil {
		return "", fmt.Errorf("describe transactions: %w", err)
	}
	out = append(out, fmt.Sprintf("described_state=%s", dt[id].State))
	if err := endTxn(t, false); err != nil {
		return "", err
	}
	return strings.Join(out, " "), nil
}

func adminQuotas(broker, topic string) (string, error) {
	adm, closeAdm, err := admin(broker)
	if err != nil {
		return "", err
	}
	defer closeAdm()
	ctx, cancel := ctxFor(60 * time.Second)
	defer cancel()
	user := "conf-user-" + topic
	entity := kadm.ClientQuotaEntity{{Type: "user", Name: &user}}
	set := func(key string, v float64, remove bool) string {
		res, err := adm.AlterClientQuotas(ctx, []kadm.AlterClientQuotaEntry{{
			Entity: entity, Ops: []kadm.AlterClientQuotaOp{{Key: key, Value: v, Remove: remove}}}})
		if err != nil {
			return errName(err)
		}
		for _, r := range res {
			return errName(r.Err)
		}
		return "empty"
	}
	describe := func(wantSome bool) string {
		var got string
		for i := 0; i < 40; i++ {
			d, err := adm.DescribeClientQuotas(ctx, true, []kadm.DescribeClientQuotaComponent{{
				Type: "user", MatchType: 0, MatchName: &user}})
			if err != nil {
				return errName(err)
			}
			got = fmt.Sprint(len(d))
			if (got != "0") == wantSome {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		return got
	}
	var out []string
	out = append(out, "set="+set("producer_byte_rate", 1048576, false))
	out = append(out, "described="+describe(true))
	out = append(out, "remove="+set("producer_byte_rate", 0, true))
	out = append(out, "after_remove="+describe(false))
	out = append(out, "unknown_quota="+set("no_such_quota", 1, false))
	return strings.Join(out, " "), nil
}

func metadata(broker, topic string) (string, error) {
	adm, closeAdm, err := admin(broker)
	if err != nil {
		return "", err
	}
	defer closeAdm()
	ctx, cancel := ctxFor(30 * time.Second)
	defer cancel()
	md, err := adm.Metadata(ctx, topic)
	if err != nil {
		return "", err
	}
	lt, err := adm.ListTopics(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("partitions=%d brokers=%d controller=%v listed=%v", len(md.Topics[topic].Partitions),
		len(md.Brokers), md.Controller >= 0, lt.Has(topic)), nil
}

func unknownTopic(broker, topic string) (string, error) {
	adm, closeAdm, err := admin(broker)
	if err != nil {
		return "", err
	}
	defer closeAdm()
	ctx, cancel := ctxFor(30 * time.Second)
	defer cancel()
	missing := topic + "-missing"
	md, err := adm.Metadata(ctx, missing)
	if err != nil {
		return "", err
	}
	td := md.Topics[missing]
	return fmt.Sprintf("partitions=%d error=%s", len(td.Partitions), errName(td.Err)), nil
}
