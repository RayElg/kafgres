package main

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// errName is the broker's numeric error code; Sarama's message text is not compared.
func errName(err error) string {
	if err == nil {
		return "none"
	}
	var k sarama.KError
	if errors.As(err, &k) {
		return fmt.Sprintf("kerr%d", int16(k))
	}
	var pe *sarama.ProducerError
	if errors.As(err, &pe) {
		return errName(pe.Err)
	}
	var te *sarama.TopicError
	if errors.As(err, &te) {
		return errName(te.Err)
	}
	return strings.SplitN(err.Error(), ":", 2)[0]
}

func str(b []byte) string {
	if b == nil {
		return "~"
	}
	return string(b)
}

func values(ms []*sarama.ConsumerMessage) string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = str(m.Value)
	}
	return strings.Join(out, ",")
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

func producerConfig() *sarama.Config {
	c := config()
	c.Producer.Partitioner = sarama.NewManualPartitioner
	return c
}

func syncProducer(broker string, c *sarama.Config) (sarama.SyncProducer, error) {
	p, err := sarama.NewSyncProducer([]string{broker}, c)
	if err != nil {
		return nil, fmt.Errorf("producer: %w", err)
	}
	return p, nil
}

func sendTo(p sarama.SyncProducer, topic string, partition int32, v string) (int64, error) {
	_, off, err := p.SendMessage(&sarama.ProducerMessage{
		Topic: topic, Partition: partition, Value: sarama.StringEncoder(v),
	})
	return off, err
}

// readPartition reads from `from` until `want` records arrive or it is quiet for `idle`.
func readPartition(broker, topic string, partition int32, from int64, c *sarama.Config,
	want int, idle time.Duration) ([]*sarama.ConsumerMessage, error) {
	c.Consumer.Return.Errors = true
	cons, err := sarama.NewConsumer([]string{broker}, c)
	if err != nil {
		return nil, fmt.Errorf("consumer: %w", err)
	}
	defer cons.Close()
	pc, err := cons.ConsumePartition(topic, partition, from)
	if err != nil {
		return nil, err
	}
	defer pc.Close()
	return collect(pc, want, idle)
}

func collect(pc sarama.PartitionConsumer, want int, idle time.Duration) ([]*sarama.ConsumerMessage, error) {
	var got []*sarama.ConsumerMessage
	for want <= 0 || len(got) < want {
		select {
		case m := <-pc.Messages():
			got = append(got, m)
		case err := <-pc.Errors():
			return got, err.Err
		case <-time.After(idle):
			if want > 0 {
				return got, fmt.Errorf("timed out after %d of %d records", len(got), want)
			}
			return got, nil
		}
	}
	return got, nil
}

func committedConfig() *sarama.Config {
	c := config()
	c.Consumer.IsolationLevel = sarama.ReadCommitted
	return c
}

type member struct {
	group sarama.ConsumerGroup
	mu    sync.Mutex
	owned map[int32]bool
	got   []string
	errs  map[string]bool
	sess  sarama.ConsumerGroupSession
	meta  string
	stop  chan struct{}
}

func (m *member) Setup(s sarama.ConsumerGroupSession) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.owned = map[int32]bool{}
	m.sess = s
	for _, ps := range s.Claims() {
		for _, p := range ps {
			m.owned[p] = true
		}
	}
	return nil
}

func (m *member) Cleanup(sarama.ConsumerGroupSession) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.owned = map[int32]bool{}
	return nil
}

func (m *member) ConsumeClaim(s sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		m.mu.Lock()
		m.got = append(m.got, string(msg.Value))
		m.mu.Unlock()
		s.MarkMessage(msg, m.meta)
	}
	return nil
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

func startMember(broker, topic, groupID string, c *sarama.Config) (*member, error) {
	g, err := sarama.NewConsumerGroup([]string{broker}, groupID, c)
	if err != nil {
		return nil, fmt.Errorf("group: %w", err)
	}
	m := &member{group: g, owned: map[int32]bool{}, errs: map[string]bool{}, stop: make(chan struct{})}
	go func() {
		for {
			select {
			case <-m.stop:
				return
			default:
			}
			// Consume returns after every rebalance; joining again is the loop Sarama documents.
			if err := g.Consume(newCancelOn(m.stop), []string{topic}, m); err != nil {
				m.mu.Lock()
				m.errs[errName(err)] = true
				m.mu.Unlock()
				select {
				case <-m.stop:
					return
				case <-time.After(200 * time.Millisecond):
				}
			}
		}
	}()
	return m, nil
}

func (m *member) close() {
	select {
	case <-m.stop:
	default:
		close(m.stop)
	}
	_ = m.group.Close()
}

// settle waits until assignments are disjoint, non-empty, cover every partition, and hold.
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
	p, err := syncProducer(broker, producerConfig())
	if err != nil {
		return err
	}
	defer p.Close()
	for i := 0; i < n; i++ {
		if _, err := sendTo(p, topic, int32(i%3), fmt.Sprintf("m%d", i)); err != nil {
			return fmt.Errorf("send: %w", err)
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
	// Linger to catch a duplicate delivered after the last expected record.
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

func (m *member) commit() {
	m.mu.Lock()
	s := m.sess
	m.mu.Unlock()
	if s != nil {
		s.Commit()
	}
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
