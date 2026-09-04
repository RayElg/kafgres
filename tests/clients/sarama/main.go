// A Sarama-driven scenario runner for the conformance suite (independent client; catches shared assumptions).
package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

func init() {
	if os.Getenv("SARAMA_DEBUG") != "" {
		sarama.Logger = log.New(os.Stderr, "[sarama] ", log.Ltime)
	}
}

func config() *sarama.Config {
	c := sarama.NewConfig()
	c.Version = sarama.V3_6_0_0
	c.Producer.Return.Successes = true
	c.Producer.RequiredAcks = sarama.WaitForLocal
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	c.Metadata.AllowAutoTopicCreation = false
	c.ClientID = "sarama-conformance"
	return c
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("usage: sarama-conformance <broker> <scenario> <topic>")
		os.Exit(2)
	}
	broker, scenario, topic := os.Args[1], os.Args[2], os.Args[3]

	var err error
	switch scenario {
	case "produce-consume":
		err = produceConsume(broker, topic)
	case "group-consume":
		err = groupConsume(broker, topic)
	case "metadata":
		err = metadata(broker, topic)
	case "txn-commit":
		err = transaction(broker, topic, true)
	case "txn-abort":
		err = transaction(broker, topic, false)
	case "txn-abort-big":
		err = transactionBig(broker, topic)
	case "txn-open":
		err = transactionHeldOpen(broker, topic)
	case "txn-second":
		err = transactionThenOpenAnother(broker, topic)
	case "txn-offsets-commit":
		err = transactionWithOffsets(broker, topic, true)
	case "txn-offsets-abort":
		err = transactionWithOffsets(broker, topic, false)
	case "unknown-topic":
		err = unknownTopic(broker)
	default:
		fmt.Printf("ERROR unknown scenario %s\n", scenario)
		os.Exit(2)
	}
	if err != nil {
		fmt.Printf("ERROR %v\n", err)
		os.Exit(1)
	}
}

func produceConsume(broker, topic string) error {
	producer, err := sarama.NewSyncProducer([]string{broker}, config())
	if err != nil {
		return fmt.Errorf("producer: %w", err)
	}
	defer producer.Close()

	var offsets []string
	for _, v := range []string{"s1", "s2", "s3"} {
		_, offset, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(v),
		})
		if err != nil {
			return fmt.Errorf("send: %w", err)
		}
		offsets = append(offsets, fmt.Sprint(offset))
	}

	consumer, err := sarama.NewConsumer([]string{broker}, config())
	if err != nil {
		return fmt.Errorf("consumer: %w", err)
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		return fmt.Errorf("consume partition: %w", err)
	}
	defer pc.Close()

	var got []string
	timeout := time.After(30 * time.Second)
	for len(got) < 3 {
		select {
		case msg := <-pc.Messages():
			got = append(got, string(msg.Value))
		case <-timeout:
			return fmt.Errorf("timed out after %d messages", len(got))
		}
	}
	fmt.Printf("OK offsets=%s values=%s\n", strings.Join(offsets, ","), strings.Join(got, ","))
	return nil
}

func groupConsume(broker, topic string) error {
	producer, err := sarama.NewSyncProducer([]string{broker}, config())
	if err != nil {
		return fmt.Errorf("producer: %w", err)
	}
	for _, v := range []string{"g1", "g2"} {
		if _, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic, Value: sarama.StringEncoder(v),
		}); err != nil {
			producer.Close()
			return fmt.Errorf("send: %w", err)
		}
	}
	producer.Close()

	group, err := sarama.NewConsumerGroup([]string{broker}, topic+"-sarama-group", config())
	if err != nil {
		return fmt.Errorf("group: %w", err)
	}
	defer group.Close()

	h := &collector{want: 2, done: make(chan struct{})}
	go func() {
		_ = group.Consume(newCancelOn(h.done), []string{topic}, h)
	}()

	select {
	case <-h.done:
	case <-time.After(60 * time.Second):
		return fmt.Errorf("group timed out after %d messages", len(h.got))
	}
	sort.Strings(h.got)
	fmt.Printf("OK values=%s\n", strings.Join(h.got, ","))
	return nil
}

func metadata(broker, topic string) error {
	client, err := sarama.NewClient([]string{broker}, config())
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}
	defer client.Close()

	parts, err := client.Partitions(topic)
	if err != nil {
		return fmt.Errorf("partitions: %w", err)
	}
	brokers := client.Brokers()
	controller, err := client.Controller()
	if err != nil {
		return fmt.Errorf("controller: %w", err)
	}
	fmt.Printf("OK brokers=%d partitions=%d controller=%d\n",
		len(brokers), len(parts), controller.ID())
	return nil
}

func unknownTopic(broker string) error {
	client, err := sarama.NewClient([]string{broker}, config())
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}
	defer client.Close()
	_, err = client.Partitions("definitely-no-such-topic-conformance")
	if err == nil {
		return fmt.Errorf("expected an error for an unknown topic")
	}
	fmt.Printf("OK err=%v\n", err)
	return nil
}

type collector struct {
	want int
	got  []string
	done chan struct{}
}

func (c *collector) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (c *collector) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (c *collector) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		c.got = append(c.got, string(msg.Value))
		sess.MarkMessage(msg, "")
		if len(c.got) >= c.want {
			select {
			case <-c.done:
			default:
				close(c.done)
			}
			return nil
		}
	}
	return nil
}

func transaction(broker, topic string, commit bool) error {
	c := config()
	c.Producer.Idempotent = true
	c.Producer.RequiredAcks = sarama.WaitForAll
	c.Producer.Transaction.ID = "kafgres-eos-test"
	c.Net.MaxOpenRequests = 1

	producer, err := sarama.NewSyncProducer([]string{broker}, c)
	if err != nil {
		return fmt.Errorf("txn producer: %w", err)
	}
	defer producer.Close()

	if err := producer.BeginTxn(); err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	outcome := "abort"
	if commit {
		outcome = "commit"
	}
	for i := 0; i < 3; i++ {
		if _, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(fmt.Sprintf("%s-%d", outcome, i)),
		}); err != nil {
			return fmt.Errorf("send: %w", err)
		}
	}
	if commit {
		if err := producer.CommitTxn(); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
	} else {
		if err := producer.AbortTxn(); err != nil {
			return fmt.Errorf("abort: %w", err)
		}
	}
	fmt.Printf("OK %s\n", outcome)
	return nil
}

func transactionBig(broker, topic string) error {
	p, err := txnProducer(broker, "kafgres-eos-big")
	if err != nil {
		return err
	}
	defer p.Close()

	if err := p.BeginTxn(); err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	for i := 0; i < 40; i++ {
		if _, _, err := p.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(fmt.Sprintf("big-%02d", i)),
		}); err != nil {
			return fmt.Errorf("send: %w", err)
		}
	}
	if err := p.AbortTxn(); err != nil {
		return fmt.Errorf("abort: %w", err)
	}
	fmt.Printf("OK abort-big\n")
	return nil
}

func transactionHeldOpen(broker, topic string) error {
	p, err := txnProducer(broker, "kafgres-eos-open")
	if err != nil {
		return err
	}
	defer p.Close()

	if err := p.BeginTxn(); err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	for i := 0; i < 3; i++ {
		if _, _, err := p.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(fmt.Sprintf("open-%d", i)),
		}); err != nil {
			return fmt.Errorf("send: %w", err)
		}
	}
	fmt.Printf("OK open\n")
	os.Stdout.Sync()
	time.Sleep(25 * time.Second)
	if err := p.AbortTxn(); err != nil {
		return fmt.Errorf("abort: %w", err)
	}
	return nil
}

func transactionThenOpenAnother(broker, topic string) error {
	p, err := txnProducer(broker, "kafgres-eos-two")
	if err != nil {
		return err
	}
	defer p.Close()

	if err := p.BeginTxn(); err != nil {
		return fmt.Errorf("begin 1: %w", err)
	}
	for i := 0; i < 3; i++ {
		if _, _, err := p.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(fmt.Sprintf("first-%d", i)),
		}); err != nil {
			return fmt.Errorf("send 1: %w", err)
		}
	}
	if err := p.CommitTxn(); err != nil {
		return fmt.Errorf("commit 1: %w", err)
	}

	time.Sleep(4 * time.Second)

	if err := p.BeginTxn(); err != nil {
		return fmt.Errorf("begin 2: %w", err)
	}
	if _, _, err := p.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder("second-0"),
	}); err != nil {
		return fmt.Errorf("send 2: %w", err)
	}
	fmt.Printf("OK second\n")
	os.Stdout.Sync()
	time.Sleep(25 * time.Second)
	if err := p.AbortTxn(); err != nil {
		return fmt.Errorf("abort 2: %w", err)
	}
	return nil
}

func txnProducer(broker, id string) (sarama.SyncProducer, error) {
	c := config()
	c.Producer.Idempotent = true
	c.Producer.RequiredAcks = sarama.WaitForAll
	c.Producer.Transaction.ID = id
	c.Net.MaxOpenRequests = 1
	p, err := sarama.NewSyncProducer([]string{broker}, c)
	if err != nil {
		return nil, fmt.Errorf("txn producer: %w", err)
	}
	return p, nil
}

func transactionWithOffsets(broker, topic string, commit bool) error {
	c := config()
	c.Producer.Idempotent = true
	c.Producer.RequiredAcks = sarama.WaitForAll
	c.Producer.Transaction.ID = "kafgres-eos-offsets"
	c.Net.MaxOpenRequests = 1

	producer, err := sarama.NewSyncProducer([]string{broker}, c)
	if err != nil {
		return fmt.Errorf("txn producer: %w", err)
	}
	defer producer.Close()

	if err := producer.BeginTxn(); err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	outcome := "abort"
	if commit {
		outcome = "commit"
	}
	if _, _, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic, Value: sarama.StringEncoder("offsets-" + outcome),
	}); err != nil {
		return fmt.Errorf("send: %w", err)
	}

	offsets := map[string][]*sarama.PartitionOffsetMetadata{
		topic: {{Partition: 0, Offset: 42}},
	}
	if err := producer.AddOffsetsToTxn(offsets, "eos-group"); err != nil {
		return fmt.Errorf("add offsets: %w", err)
	}

	if commit {
		if err := producer.CommitTxn(); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
	} else {
		if err := producer.AbortTxn(); err != nil {
			return fmt.Errorf("abort: %w", err)
		}
	}
	fmt.Printf("OK offsets-%s\n", outcome)
	return nil
}
