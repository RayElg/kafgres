package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

func recordsShape(broker, topic string) error {
	t0 := time.UnixMilli(1_700_000_000_000)
	c := producerConfig()
	p, err := syncProducer(broker, c)
	if err != nil {
		return err
	}
	defer p.Close()
	empty := sarama.ByteEncoder([]byte{})
	msgs := []*sarama.ProducerMessage{
		{Key: sarama.StringEncoder("k1"), Value: sarama.StringEncoder("v1"), Timestamp: t0,
			Headers: []sarama.RecordHeader{{Key: []byte("a"), Value: []byte("1")}, {Key: []byte("b"), Value: []byte("2")}}},
		{Value: sarama.StringEncoder("v2"), Timestamp: t0.Add(1 * time.Second)},
		{Key: sarama.StringEncoder("k3"), Timestamp: t0.Add(2 * time.Second)},
		{Key: empty, Value: empty, Timestamp: t0.Add(3 * time.Second)},
		{Key: sarama.StringEncoder("k5"), Value: sarama.StringEncoder("v5"), Timestamp: t0.Add(4 * time.Second),
			Headers: []sarama.RecordHeader{{Key: []byte("h"), Value: []byte("x")}, {Key: []byte("h"), Value: []byte("y")},
				{Key: []byte("n"), Value: nil}}},
	}
	for _, m := range msgs {
		m.Topic = topic
		if _, _, err := p.SendMessage(m); err != nil {
			return fmt.Errorf("send: %w", err)
		}
	}
	got, err := readPartition(broker, topic, 0, sarama.OffsetOldest, config(), 5, 30*time.Second)
	if err != nil {
		return err
	}
	var out []string
	for _, m := range got {
		var h strings.Builder
		for _, x := range m.Headers {
			fmt.Fprintf(&h, "%s=%s;", x.Key, str(x.Value))
		}
		out = append(out, fmt.Sprintf("%s:%s@%d[%s]", str(m.Key), str(m.Value), m.Timestamp.UnixMilli(), h.String()))
	}
	fmt.Printf("OK records=%s\n", strings.Join(out, " "))
	return nil
}

func compression(broker, topic string) error {
	codecs := []struct {
		name  string
		codec sarama.CompressionCodec
	}{
		{"none", sarama.CompressionNone}, {"gzip", sarama.CompressionGZIP},
		{"snappy", sarama.CompressionSnappy}, {"lz4", sarama.CompressionLZ4},
		{"zstd", sarama.CompressionZSTD},
	}
	var out []string
	for _, cd := range codecs {
		c := producerConfig()
		c.Producer.Compression = cd.codec
		p, err := syncProducer(broker, c)
		if err != nil {
			return err
		}
		var sent [][]byte
		var msgs []*sarama.ProducerMessage
		for i := 0; i < 20; i++ {
			v := []byte(strings.Repeat(fmt.Sprintf("payload-%s-%d-", cd.name, i), 40))
			sent = append(sent, v)
			msgs = append(msgs, &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(v)})
		}
		if err := p.SendMessages(msgs); err != nil {
			p.Close()
			return fmt.Errorf("%s send: %w", cd.name, err)
		}
		p.Close()
		first := msgs[0].Offset
		got, err := readPartition(broker, topic, 0, first, config(), 20, 30*time.Second)
		if err != nil {
			return fmt.Errorf("%s read: %w", cd.name, err)
		}
		var read [][]byte
		for _, m := range got {
			read = append(read, m.Value)
		}
		mark := ""
		if crc(read) != crc(sent) {
			mark = ":MISMATCH"
		}
		out = append(out, fmt.Sprintf("%s=%d:%s%s", cd.name, len(read), crc(read), mark))
	}
	fmt.Printf("OK %s\n", strings.Join(out, " "))
	return nil
}

func largeRecords(broker, topic string) error {
	c := producerConfig()
	p, err := syncProducer(broker, c)
	if err != nil {
		return err
	}
	big := make([]byte, 900_000)
	for i := range big {
		big[i] = byte(i * 31)
	}
	sent := [][]byte{big}
	if _, _, err := p.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(big)}); err != nil {
		p.Close()
		return fmt.Errorf("big send: %w", err)
	}
	var msgs []*sarama.ProducerMessage
	for i := 0; i < 200; i++ {
		v := make([]byte, 10_000)
		for j := range v {
			v[j] = byte(i)
		}
		sent = append(sent, v)
		msgs = append(msgs, &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(v)})
	}
	if err := p.SendMessages(msgs); err != nil {
		p.Close()
		return fmt.Errorf("send: %w", err)
	}
	p.Close()
	got, err := readPartition(broker, topic, 0, sarama.OffsetOldest, config(), len(sent), 60*time.Second)
	if err != nil {
		return err
	}
	var read [][]byte
	n := 0
	for _, m := range got {
		read = append(read, m.Value)
		n += len(m.Value)
	}
	fmt.Printf("OK count=%d bytes=%d crc=%s same=%v\n", len(read), n, crc(read), crc(read) == crc(sent))
	return nil
}

func recordTooLarge(broker, topic string) error {
	c := producerConfig()
	c.Producer.MaxMessageBytes = 8 << 20
	p, err := syncProducer(broker, c)
	if err != nil {
		return err
	}
	defer p.Close()
	_, _, serr := p.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(make([]byte, 2<<20))})
	next, err := sendTo(p, topic, 0, "after")
	if err != nil {
		return fmt.Errorf("send after: %w", err)
	}
	fmt.Printf("OK error=%s next_offset=%d\n", errName(serr), next)
	return nil
}

func multiPartition(broker, topic string) error {
	p, err := syncProducer(broker, producerConfig())
	if err != nil {
		return err
	}
	defer p.Close()
	var offs []string
	for part := int32(0); part < 3; part++ {
		var o []int64
		for i := 0; i < 5; i++ {
			off, err := sendTo(p, topic, part, fmt.Sprintf("p%d-%d", part, i))
			if err != nil {
				return fmt.Errorf("send: %w", err)
			}
			o = append(o, off)
		}
		offs = append(offs, fmt.Sprintf("p%d=%s", part, join(o)))
	}
	client, err := sarama.NewClient([]string{broker}, config())
	if err != nil {
		return err
	}
	defer client.Close()
	parts, err := client.Partitions(topic)
	if err != nil {
		return err
	}
	var read, begin, end []string
	for _, part := range parts {
		got, err := readPartition(broker, topic, part, sarama.OffsetOldest, config(), 5, 30*time.Second)
		if err != nil {
			return err
		}
		read = append(read, fmt.Sprintf("%d=[%s]", part, values(got)))
		b, _ := client.GetOffset(topic, part, sarama.OffsetOldest)
		e, _ := client.GetOffset(topic, part, sarama.OffsetNewest)
		begin = append(begin, fmt.Sprintf("%d=%d", part, b))
		end = append(end, fmt.Sprintf("%d=%d", part, e))
	}
	fmt.Printf("OK %s read=%s begin=%s end=%s partitions=%d\n", strings.Join(offs, ","),
		strings.Join(read, " "), strings.Join(begin, ","), strings.Join(end, ","), len(parts))
	return nil
}

func offsetsByTime(broker, topic string) error {
	t0 := time.UnixMilli(1_700_000_000_000)
	p, err := syncProducer(broker, producerConfig())
	if err != nil {
		return err
	}
	for i := 0; i < 4; i++ {
		if _, _, err := p.SendMessage(&sarama.ProducerMessage{
			Topic: topic, Value: sarama.StringEncoder(fmt.Sprintf("t%d", i)),
			Timestamp: t0.Add(time.Duration(i) * time.Second),
		}); err != nil {
			p.Close()
			return fmt.Errorf("send: %w", err)
		}
	}
	p.Close()
	client, err := sarama.NewClient([]string{broker}, config())
	if err != nil {
		return err
	}
	defer client.Close()
	var out []string
	for _, off := range []int64{-1, 0, 1500, 3000, 5000} {
		o, err := client.GetOffset(topic, 0, t0.UnixMilli()+off)
		out = append(out, fmt.Sprintf("%d->%d(%s)", off, o, errName(err)))
	}
	b, _ := client.GetOffset(topic, 0, sarama.OffsetOldest)
	e, _ := client.GetOffset(topic, 0, sarama.OffsetNewest)
	fmt.Printf("OK by_time=%s begin=%d end=%d\n", strings.Join(out, " "), b, e)
	return nil
}

func seek(broker, topic string) error {
	p, err := syncProducer(broker, producerConfig())
	if err != nil {
		return err
	}
	defer p.Close()
	for i := 0; i < 10; i++ {
		if _, err := sendTo(p, topic, 0, fmt.Sprintf("r%d", i)); err != nil {
			return err
		}
	}
	tail, err := readPartition(broker, topic, 0, 5, config(), 5, 30*time.Second)
	if err != nil {
		return fmt.Errorf("tail: %w", err)
	}
	// Sarama has no seek; the equivalent is a new partition consumer at the new position.
	cons, err := sarama.NewConsumer([]string{broker}, config())
	if err != nil {
		return err
	}
	defer cons.Close()
	pc, err := cons.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("newest: %w", err)
	}
	if _, err := sendTo(p, topic, 0, "r10"); err != nil {
		return err
	}
	live, err := collect(pc, 1, 30*time.Second)
	pc.Close()
	if err != nil {
		return fmt.Errorf("live: %w", err)
	}
	third, err := readPartition(broker, topic, 0, 3, config(), 1, 30*time.Second)
	if err != nil {
		return fmt.Errorf("third: %w", err)
	}
	fmt.Printf("OK tail=%s live=%s third=%d:%s\n", values(tail), values(live), third[0].Offset, str(third[0].Value))
	return nil
}

func longPoll(broker, topic string) error {
	c := config()
	c.Consumer.MaxWaitTime = 1500 * time.Millisecond
	cons, err := sarama.NewConsumer([]string{broker}, c)
	if err != nil {
		return err
	}
	defer cons.Close()
	pc, err := cons.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		return err
	}
	defer pc.Close()
	empty, err := collect(pc, 0, 3*time.Second)
	if err != nil {
		return fmt.Errorf("empty poll: %w", err)
	}
	p, err := syncProducer(broker, producerConfig())
	if err != nil {
		return err
	}
	defer p.Close()
	started := time.Now()
	if _, err := sendTo(p, topic, 0, "late"); err != nil {
		return err
	}
	late, err := collect(pc, 1, 20*time.Second)
	if err != nil {
		return err
	}
	fmt.Printf("OK empty=%d late=%s prompt=%v\n", len(empty), values(late), time.Since(started) < 15*time.Second)
	return nil
}

func offsetOutOfRange(broker, topic string) error {
	p, err := syncProducer(broker, producerConfig())
	if err != nil {
		return err
	}
	for i := 0; i < 3; i++ {
		if _, err := sendTo(p, topic, 0, fmt.Sprintf("o%d", i)); err != nil {
			p.Close()
			return err
		}
	}
	p.Close()
	cons, err := sarama.NewConsumer([]string{broker}, config())
	if err != nil {
		return err
	}
	defer cons.Close()
	_, serr := cons.ConsumePartition(topic, 0, 99)
	_, below := cons.ConsumePartition(topic, 0, -50)
	got, err := readPartition(broker, topic, 0, sarama.OffsetOldest, config(), 3, 30*time.Second)
	if err != nil {
		return err
	}
	fmt.Printf("OK past_end=%s read_oldest=%s\n", errName(serr), values(got))
	_ = below
	return nil
}

func idempotentProduce(broker, topic string) error {
	c := producerConfig()
	c.Producer.Idempotent = true
	c.Producer.RequiredAcks = sarama.WaitForAll
	c.Net.MaxOpenRequests = 1
	p, err := syncProducer(broker, c)
	if err != nil {
		return err
	}
	var msgs []*sarama.ProducerMessage
	for i := 0; i < 50; i++ {
		msgs = append(msgs, &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(fmt.Sprintf("i%d", i))})
	}
	if err := p.SendMessages(msgs); err != nil {
		p.Close()
		return fmt.Errorf("send: %w", err)
	}
	p.Close()
	contiguous := true
	for i, m := range msgs {
		contiguous = contiguous && m.Offset == int64(i)
	}
	got, err := readPartition(broker, topic, 0, sarama.OffsetOldest, config(), 50, 30*time.Second)
	if err != nil {
		return err
	}
	fmt.Printf("OK contiguous=%v first=%d last=%d read=%d\n", contiguous, msgs[0].Offset, msgs[49].Offset, len(got))
	return nil
}

func acks(broker, topic string) error {
	var out []string
	for _, a := range []struct {
		name string
		acks sarama.RequiredAcks
	}{{"0", sarama.NoResponse}, {"1", sarama.WaitForLocal}, {"all", sarama.WaitForAll}} {
		c := producerConfig()
		c.Producer.RequiredAcks = a.acks
		p, err := syncProducer(broker, c)
		if err != nil {
			return err
		}
		off, err := sendTo(p, topic, 0, "acks-"+a.name)
		p.Close()
		if err != nil {
			return fmt.Errorf("acks=%s: %w", a.name, err)
		}
		if a.name == "0" {
			out = append(out, "acks0")
		} else {
			out = append(out, fmt.Sprintf("acks%s@%d", a.name, off))
		}
	}
	got, err := readPartition(broker, topic, 0, sarama.OffsetOldest, config(), 3, 30*time.Second)
	if err != nil {
		return err
	}
	fmt.Printf("OK %s read=%s\n", strings.Join(out, " "), values(got))
	return nil
}

var _ = time.Second
