package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

func groupTwoMembers(broker, topic string) error {
	g := topic + "-g"
	a, err := startMember(broker, topic, g, config())
	if err != nil {
		return err
	}
	defer a.close()
	b, err := startMember(broker, topic, g, config())
	if err != nil {
		return err
	}
	defer b.close()
	if err := settle(3, a, b); err != nil {
		return err
	}
	before := sizes(a, b)
	if err := produceSpread(broker, topic, 30); err != nil {
		return err
	}
	fmt.Printf("OK sizes=%s %s\n", before, consumeAll(30, a, b))
	return nil
}

func groupRebalanceLeave(broker, topic string) error {
	g := topic + "-g"
	a, err := startMember(broker, topic, g, config())
	if err != nil {
		return err
	}
	defer a.close()
	b, err := startMember(broker, topic, g, config())
	if err != nil {
		return err
	}
	if err := settle(3, a, b); err != nil {
		b.close()
		return err
	}
	before := sizes(a, b)
	b.close()
	deadline := time.Now().Add(60 * time.Second)
	for len(a.ownedSet()) < 3 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	after := len(a.ownedSet())
	if err := produceSpread(broker, topic, 9); err != nil {
		return err
	}
	fmt.Printf("OK before=%s after_leave=%d %s\n", before, after, consumeAll(9, a))
	return nil
}

func groupResume(broker, topic string) error {
	g := topic + "-g"
	p, err := syncProducer(broker, producerConfig())
	if err != nil {
		return err
	}
	defer p.Close()
	for i := 0; i < 5; i++ {
		if _, err := sendTo(p, topic, 0, fmt.Sprintf("first-%d", i)); err != nil {
			return err
		}
	}
	m, err := startMember(broker, topic, g, config())
	if err != nil {
		return err
	}
	m.meta = "meta-x"
	deadline := time.Now().Add(60 * time.Second)
	for len(m.received()) < 5 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	first := len(m.received())
	m.commit()
	m.close()

	admin, err := sarama.NewClusterAdmin([]string{broker}, config())
	if err != nil {
		return err
	}
	defer admin.Close()
	r, err := admin.ListConsumerGroupOffsets(g, map[string][]int32{topic: {0}})
	if err != nil {
		return fmt.Errorf("list offsets: %w", err)
	}
	stored := r.GetBlock(topic, 0)

	for i := 0; i < 3; i++ {
		if _, err := sendTo(p, topic, 0, fmt.Sprintf("second-%d", i)); err != nil {
			return err
		}
	}
	m2, err := startMember(broker, topic, g, config())
	if err != nil {
		return err
	}
	defer m2.close()
	deadline = time.Now().Add(60 * time.Second)
	for len(m2.received()) < 3 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(1500 * time.Millisecond)
	fmt.Printf("OK first=%d committed=%d/%s second=%s\n", first, stored.Offset, stored.Metadata,
		strings.Join(m2.received(), ","))
	return nil
}

func staticConfig(instance string) *sarama.Config {
	c := config()
	c.Consumer.Group.InstanceId = instance
	return c
}

func groupStatic(broker, topic string) error {
	g := topic + "-g"
	a, err := startMember(broker, topic, g, staticConfig("inst-a"))
	if err != nil {
		return err
	}
	defer a.close()
	b, err := startMember(broker, topic, g, staticConfig("inst-b"))
	if err != nil {
		return err
	}
	if err := settle(3, a, b); err != nil {
		b.close()
		return err
	}
	bHad, aHad := b.ownedSet(), a.ownedSet()
	// A static member that goes away keeps its slot.
	b.close()
	time.Sleep(3 * time.Second)
	aStable := sameSet(a.ownedSet(), aHad)
	b2, err := startMember(broker, topic, g, staticConfig("inst-b"))
	if err != nil {
		return err
	}
	defer b2.close()
	deadline := time.Now().Add(60 * time.Second)
	for len(b2.ownedSet()) == 0 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	same := sameSet(b2.ownedSet(), bHad)
	time.Sleep(2 * time.Second)
	aAfter := sameSet(a.ownedSet(), aHad)
	// Two live consumers cannot share an instance id.
	usurper, err := startMember(broker, topic, g, staticConfig("inst-b"))
	if err != nil {
		return err
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
	fmt.Printf("OK a_stable_while_b_away=%v b_same_assignment=%v a_stable_after=%v duplicate_instance=%s\n",
		aStable, same, aAfter, fenced)
	return nil
}

func groupAdmin(broker, topic string) error {
	g := topic + "-g"
	p, err := syncProducer(broker, producerConfig())
	if err != nil {
		return err
	}
	defer p.Close()
	for i := 0; i < 6; i++ {
		if _, err := sendTo(p, topic, 0, fmt.Sprintf("ga%d", i)); err != nil {
			return err
		}
	}
	m, err := startMember(broker, topic, g, config())
	if err != nil {
		return err
	}
	defer m.close()
	deadline := time.Now().Add(60 * time.Second)
	for len(m.received()) < 6 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	m.commit()
	admin, err := sarama.NewClusterAdmin([]string{broker}, config())
	if err != nil {
		return err
	}
	defer admin.Close()

	var out []string
	listed, err := admin.ListConsumerGroups()
	if err != nil {
		return fmt.Errorf("list groups: %w", err)
	}
	_, isListed := listed[g]
	ds, err := admin.DescribeConsumerGroups([]string{g})
	if err != nil || len(ds) != 1 {
		return fmt.Errorf("describe: %v", err)
	}
	assigned := 0
	for _, mem := range ds[0].Members {
		if a, err := mem.GetMemberAssignment(); err == nil {
			for _, ps := range a.Topics {
				assigned += len(ps)
			}
		}
	}
	out = append(out, fmt.Sprintf("listed=%v state=%s members=%d assigned=%d", isListed, ds[0].State, len(ds[0].Members), assigned))
	offs := func() string {
		r, err := admin.ListConsumerGroupOffsets(g, map[string][]int32{topic: {0}})
		if err != nil {
			return errName(err)
		}
		return fmt.Sprint(r.GetBlock(topic, 0).Offset)
	}
	out = append(out, "offsets=0:"+offs())
	out = append(out, "delete_while_active="+errName(admin.DeleteConsumerGroup(g)))
	m.close()
	for i := 0; i < 40; i++ {
		d, err := admin.DescribeConsumerGroups([]string{g})
		if err == nil && len(d) == 1 && len(d[0].Members) == 0 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	out = append(out, "delete_offset="+errName(admin.DeleteConsumerGroupOffset(g, topic, 0)))
	out = append(out, "offset_after_delete="+offs())
	out = append(out, "delete="+errName(admin.DeleteConsumerGroup(g)))
	listed, _ = admin.ListConsumerGroups()
	_, still := listed[g]
	out = append(out, fmt.Sprintf("gone=%v", !still))
	fmt.Printf("OK %s\n", strings.Join(out, " "))
	return nil
}

var _ = sort.Strings
