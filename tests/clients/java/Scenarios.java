import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Java client conformance scenarios: one machine-readable line per scenario, diffed
 * between kafgres and a reference Kafka.
 */
public class Scenarios {
    static String broker;
    static String topic;

    interface Scenario { String run() throws Exception; }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("usage: java-conformance <broker> <scenario> <topic>");
            System.exit(2);
        }
        broker = args[0];
        topic = args[2];
        Map<String, Scenario> all = new LinkedHashMap<>();
        all.put("produce-consume", Scenarios::produceConsume);
        all.put("records-shape", Scenarios::recordsShape);
        all.put("compression", Scenarios::compression);
        all.put("large-records", Scenarios::largeRecords);
        all.put("record-too-large", Scenarios::recordTooLarge);
        all.put("multi-partition", Scenarios::multiPartition);
        all.put("offsets-by-time", Scenarios::offsetsByTime);
        all.put("seek", Scenarios::seek);
        all.put("long-poll", Scenarios::longPoll);
        all.put("offset-out-of-range", Scenarios::offsetOutOfRange);
        all.put("idempotent-produce", Scenarios::idempotentProduce);
        all.put("acks", Scenarios::acks);
        all.put("txn-read-committed", Scenarios::txnReadCommitted);
        all.put("txn-consume-transform-produce", Scenarios::txnConsumeTransformProduce);
        all.put("txn-fenced", Scenarios::txnFenced);
        all.put("group-two-members", Scenarios::groupTwoMembers);
        all.put("group-resume", Scenarios::groupResume);
        all.put("group-rebalance-leave", Scenarios::groupRebalanceLeave);
        all.put("group-static", Scenarios::groupStatic);
        all.put("group-cooperative", Scenarios::groupCooperative);
        all.put("group-kip848", Scenarios::groupKip848);
        all.put("group-admin", Scenarios::groupAdmin);
        all.put("admin-topics", Scenarios::adminTopics);
        all.put("admin-configs", Scenarios::adminConfigs);
        all.put("admin-cluster", Scenarios::adminCluster);
        all.put("admin-delete-records", Scenarios::adminDeleteRecords);
        all.put("admin-offsets", Scenarios::adminOffsets);
        all.put("admin-producers", Scenarios::adminProducers);
        all.put("admin-quotas", Scenarios::adminQuotas);
        all.put("share-consume", Scenarios::shareConsume);
        all.put("topic-recreate", Scenarios::topicRecreate);
        all.put("topic-limits", Scenarios::topicLimits);
        all.put("many-partitions", Scenarios::manyPartitions);
        all.put("many-records", Scenarios::manyRecords);
        all.put("seek-into-batch", Scenarios::seekIntoBatch);
        all.put("group-partition-growth", Scenarios::groupPartitionGrowth);
        all.put("group-pattern", Scenarios::groupPattern);
        all.put("group-list-filters", Scenarios::groupListFilters);
        all.put("txn-timeout", Scenarios::txnTimeout);
        all.put("compacted-null-key", Scenarios::compactedNullKey);
        all.put("metadata", Scenarios::metadata);
        all.put("unknown-topic", Scenarios::unknownTopic);

        Scenario s = all.get(args[1]);
        if (s == null) {
            System.out.println("ERROR unknown scenario " + args[1]);
            System.exit(2);
        }
        try {
            System.out.println("OK " + s.run());
            System.exit(0);
        } catch (Throwable t) {
            System.out.println("ERROR " + err(t) + ": " + t);
            System.exit(1);
        }
    }

    static String err(Throwable t) {
        while ((t instanceof ExecutionException || t instanceof java.util.concurrent.CompletionException)
                && t.getCause() != null) {
            t = t.getCause();
        }
        return t.getClass().getSimpleName();
    }

    static String s(byte[] b) { return b == null ? "~" : new String(b, StandardCharsets.UTF_8); }

    static byte[] b(String s) { return s == null ? null : s.getBytes(StandardCharsets.UTF_8); }

    static Properties base() {
        Properties p = new Properties();
        p.put("bootstrap.servers", broker);
        p.put("client.id", "java-conformance");
        return p;
    }

    static KafkaProducer<byte[], byte[]> producer(Properties extra) {
        Properties p = base();
        p.put("key.serializer", ByteArraySerializer.class.getName());
        p.put("value.serializer", ByteArraySerializer.class.getName());
        p.put("max.block.ms", "30000");
        p.put("request.timeout.ms", "30000");
        p.putAll(extra);
        return new KafkaProducer<>(p);
    }

    static KafkaProducer<byte[], byte[]> producer() { return producer(new Properties()); }

    static KafkaConsumer<byte[], byte[]> consumer(Properties extra) {
        Properties p = base();
        p.put("key.deserializer", ByteArrayDeserializer.class.getName());
        p.put("value.deserializer", ByteArrayDeserializer.class.getName());
        p.put("auto.offset.reset", "earliest");
        p.put("enable.auto.commit", "false");
        // Both brokers have auto-creation off.
        p.put("allow.auto.create.topics", "false");
        p.put("default.api.timeout.ms", "30000");
        p.putAll(extra);
        return new KafkaConsumer<>(p);
    }

    static Admin admin() {
        Properties p = base();
        p.put("default.api.timeout.ms", "30000");
        p.put("request.timeout.ms", "30000");
        return Admin.create(p);
    }

    static RecordMetadata send(KafkaProducer<byte[], byte[]> p, ProducerRecord<byte[], byte[]> r)
            throws Exception {
        return p.send(r).get(30, TimeUnit.SECONDS);
    }

    static RecordMetadata send(KafkaProducer<byte[], byte[]> p, String t, int partition, String v)
            throws Exception {
        return send(p, new ProducerRecord<>(t, partition, null, b(v)));
    }

    static List<ConsumerRecord<byte[], byte[]>> drain(KafkaConsumer<byte[], byte[]> c, int want, long ms) {
        List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>();
        long deadline = System.currentTimeMillis() + ms;
        while (out.size() < want && System.currentTimeMillis() < deadline) {
            for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(500))) out.add(r);
        }
        return out;
    }

    /** Every record in [beginning, end) of `tp`, with the consumer's own isolation level. */
    static List<ConsumerRecord<byte[], byte[]>> readPartition(Properties extra, TopicPartition tp) {
        try (KafkaConsumer<byte[], byte[]> c = consumer(extra)) {
            c.assign(List.of(tp));
            c.seekToBeginning(List.of(tp));
            long end = c.endOffsets(List.of(tp)).get(tp);
            List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 30000;
            while (c.position(tp) < end && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(500))) out.add(r);
            }
            if (c.position(tp) < end) throw new IllegalStateException("stalled at " + c.position(tp) + " of " + end);
            return out;
        }
    }

    static String values(List<ConsumerRecord<byte[], byte[]>> rs) {
        return rs.stream().map(r -> s(r.value())).collect(Collectors.joining(","));
    }

    static String crc(List<byte[]> parts) {
        CRC32 c = new CRC32();
        for (byte[] p : parts) c.update(p);
        return Long.toHexString(c.getValue());
    }

    static String join(Collection<?> c) {
        return c.stream().map(String::valueOf).collect(Collectors.joining(","));
    }

    static TopicPartition tp(int p) { return new TopicPartition(topic, p); }

    static String produceConsume() throws Exception {
        List<Long> offsets = new ArrayList<>();
        try (KafkaProducer<byte[], byte[]> p = producer()) {
            for (String v : List.of("j1", "j2", "j3")) offsets.add(send(p, topic, 0, v).offset());
        }
        try (KafkaConsumer<byte[], byte[]> c = consumer(new Properties())) {
            c.assign(List.of(tp(0)));
            List<ConsumerRecord<byte[], byte[]>> got = drain(c, 3, 30000);
            return "offsets=" + join(offsets) + " values=" + values(got);
        }
    }

    static String recordsShape() throws Exception {
        long t0 = 1_700_000_000_000L;
        List<ProducerRecord<byte[], byte[]>> sent = new ArrayList<>();
        sent.add(new ProducerRecord<>(topic, 0, t0, b("k1"), b("v1"),
                List.of(new RecordHeader("a", b("1")), new RecordHeader("b", b("2")))));
        sent.add(new ProducerRecord<>(topic, 0, t0 + 1000, null, b("v2")));
        sent.add(new ProducerRecord<>(topic, 0, t0 + 2000, b("k3"), (byte[]) null));
        sent.add(new ProducerRecord<>(topic, 0, t0 + 3000, b(""), b("")));
        sent.add(new ProducerRecord<>(topic, 0, t0 + 4000, b("k5"), b("v5"),
                List.of(new RecordHeader("h", b("x")), new RecordHeader("h", b("y")),
                        new RecordHeader("n", null))));
        try (KafkaProducer<byte[], byte[]> p = producer()) {
            for (ProducerRecord<byte[], byte[]> r : sent) send(p, r);
        }
        try (KafkaConsumer<byte[], byte[]> c = consumer(new Properties())) {
            c.assign(List.of(tp(0)));
            List<String> out = new ArrayList<>();
            for (ConsumerRecord<byte[], byte[]> r : drain(c, 5, 30000)) {
                StringBuilder h = new StringBuilder();
                for (Header x : r.headers()) h.append(x.key()).append('=').append(s(x.value())).append(';');
                out.add(s(r.key()) + ":" + s(r.value()) + "@" + r.timestamp() + "/" + r.timestampType()
                        + "[" + h + "]");
            }
            return "records=" + String.join(" ", out);
        }
    }

    static String compression() throws Exception {
        List<String> out = new ArrayList<>();
        for (String codec : List.of("none", "gzip", "snappy", "lz4", "zstd")) {
            Properties pp = new Properties();
            pp.put("compression.type", codec);
            pp.put("linger.ms", "100");
            pp.put("batch.size", "65536");
            List<byte[]> sent = new ArrayList<>();
            List<Future<RecordMetadata>> pending = new ArrayList<>();
            try (KafkaProducer<byte[], byte[]> p = producer(pp)) {
                for (int i = 0; i < 20; i++) {
                    byte[] v = b(("payload-" + codec + "-" + i + "-").repeat(40));
                    sent.add(v);
                    pending.add(p.send(new ProducerRecord<>(topic, 0, null, v)));
                }
                p.flush();
                for (Future<RecordMetadata> f : pending) f.get(30, TimeUnit.SECONDS);
            }
            long first = pending.get(0).get().offset();
            try (KafkaConsumer<byte[], byte[]> c = consumer(new Properties())) {
                c.assign(List.of(tp(0)));
                c.seek(tp(0), first);
                List<byte[]> got = drain(c, 20, 30000).stream().map(ConsumerRecord::value)
                        .collect(Collectors.toList());
                out.add(codec + "=" + got.size() + ":" + crc(got) + (crc(got).equals(crc(sent)) ? "" : ":MISMATCH"));
            }
        }
        return String.join(" ", out);
    }

    static String largeRecords() throws Exception {
        byte[] big = new byte[900_000];
        for (int i = 0; i < big.length; i++) big[i] = (byte) (i * 31);
        List<byte[]> sent = new ArrayList<>();
        sent.add(big);
        Properties pp = new Properties();
        pp.put("linger.ms", "50");
        try (KafkaProducer<byte[], byte[]> p = producer(pp)) {
            send(p, new ProducerRecord<>(topic, 0, null, big));
            List<Future<RecordMetadata>> f = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                byte[] v = new byte[10_000];
                Arrays.fill(v, (byte) i);
                sent.add(v);
                f.add(p.send(new ProducerRecord<>(topic, 0, null, v)));
            }
            for (Future<RecordMetadata> x : f) x.get(30, TimeUnit.SECONDS);
        }
        try (KafkaConsumer<byte[], byte[]> c = consumer(new Properties())) {
            c.assign(List.of(tp(0)));
            List<byte[]> got = drain(c, sent.size(), 60000).stream().map(ConsumerRecord::value)
                    .collect(Collectors.toList());
            return "count=" + got.size() + " bytes=" + got.stream().mapToInt(x -> x.length).sum()
                    + " crc=" + crc(got) + " same=" + crc(got).equals(crc(sent));
        }
    }

    static String recordTooLarge() throws Exception {
        Properties pp = new Properties();
        pp.put("max.request.size", "8388608");
        String error = "none";
        try (KafkaProducer<byte[], byte[]> p = producer(pp)) {
            try {
                send(p, new ProducerRecord<>(topic, 0, null, new byte[2 * 1024 * 1024]));
            } catch (Exception e) {
                error = err(e);
            }
            long next = send(p, topic, 0, "after").offset();
            return "error=" + error + " next_offset=" + next;
        }
    }

    static String multiPartition() throws Exception {
        List<String> offs = new ArrayList<>();
        try (KafkaProducer<byte[], byte[]> p = producer()) {
            for (int part = 0; part < 3; part++) {
                List<Long> o = new ArrayList<>();
                for (int i = 0; i < 5; i++) o.add(send(p, topic, part, "p" + part + "-" + i).offset());
                offs.add("p" + part + "=" + join(o));
            }
        }
        try (KafkaConsumer<byte[], byte[]> c = consumer(new Properties())) {
            List<TopicPartition> tps = List.of(tp(0), tp(1), tp(2));
            c.assign(tps);
            Map<Integer, List<String>> got = new TreeMap<>();
            for (ConsumerRecord<byte[], byte[]> r : drain(c, 15, 30000)) {
                got.computeIfAbsent(r.partition(), k -> new ArrayList<>()).add(s(r.value()));
            }
            return join(offs) + " read=" + got + " begin=" + new TreeMap<>(offsetsOf(c.beginningOffsets(tps)))
                    + " end=" + new TreeMap<>(offsetsOf(c.endOffsets(tps)))
                    + " partitions=" + c.partitionsFor(topic).size();
        }
    }

    static Map<Integer, Long> offsetsOf(Map<TopicPartition, Long> m) {
        Map<Integer, Long> o = new TreeMap<>();
        m.forEach((k, v) -> o.put(k.partition(), v));
        return o;
    }

    static String offsetsByTime() throws Exception {
        long t0 = 1_700_000_000_000L;
        try (KafkaProducer<byte[], byte[]> p = producer()) {
            for (int i = 0; i < 4; i++) {
                send(p, new ProducerRecord<>(topic, 0, t0 + i * 1000L, null, b("t" + i)));
            }
        }
        try (KafkaConsumer<byte[], byte[]> c = consumer(new Properties())) {
            Map<TopicPartition, Long> q = new HashMap<>();
            List<String> out = new ArrayList<>();
            for (long at : new long[]{t0 - 1, t0, t0 + 1500, t0 + 3000, t0 + 5000}) {
                q.put(tp(0), at);
                OffsetAndTimestamp o = c.offsetsForTimes(q).get(tp(0));
                out.add((at - t0) + "->" + (o == null ? "null" : o.offset() + "@" + (o.timestamp() - t0)));
            }
            return "by_time=" + String.join(" ", out) + " begin=" + c.beginningOffsets(List.of(tp(0))).get(tp(0))
                    + " end=" + c.endOffsets(List.of(tp(0))).get(tp(0));
        }
    }

    static String seek() throws Exception {
        try (KafkaProducer<byte[], byte[]> p = producer()) {
            for (int i = 0; i < 10; i++) send(p, topic, 0, "r" + i);
            try (KafkaConsumer<byte[], byte[]> c = consumer(new Properties())) {
                c.assign(List.of(tp(0)));
                c.seek(tp(0), 5);
                String tail = values(drain(c, 5, 30000));
                c.seekToEnd(List.of(tp(0)));
                long end = c.position(tp(0));
                send(p, topic, 0, "r10");
                String live = values(drain(c, 1, 30000));
                c.seekToBeginning(List.of(tp(0)));
                long begin = c.position(tp(0));
                c.seek(tp(0), 3);
                ConsumerRecord<byte[], byte[]> third = drain(c, 1, 30000).get(0);
                return "tail=" + tail + " end=" + end + " live=" + live + " begin=" + begin
                        + " third=" + third.offset() + ":" + s(third.value());
            }
        }
    }

    static String longPoll() throws Exception {
        Properties cp = new Properties();
        cp.put("fetch.max.wait.ms", "1500");
        try (KafkaConsumer<byte[], byte[]> c = consumer(cp);
             KafkaProducer<byte[], byte[]> p = producer()) {
            c.assign(List.of(tp(0)));
            int empty = c.poll(Duration.ofMillis(3000)).count();
            Thread t = new Thread(() -> {
                try {
                    Thread.sleep(1500);
                    send(p, topic, 0, "late");
                } catch (Exception ignored) { }
            });
            t.start();
            long started = System.currentTimeMillis();
            List<ConsumerRecord<byte[], byte[]>> got = drain(c, 1, 20000);
            t.join();
            return "empty=" + empty + " late=" + values(got)
                    + " prompt=" + (System.currentTimeMillis() - started < 15000);
        }
    }

    static String offsetOutOfRange() throws Exception {
        try (KafkaProducer<byte[], byte[]> p = producer()) {
            for (int i = 0; i < 3; i++) send(p, topic, 0, "o" + i);
        }
        List<String> out = new ArrayList<>();
        for (String policy : List.of("none", "earliest", "latest")) {
            Properties cp = new Properties();
            cp.put("auto.offset.reset", policy);
            try (KafkaConsumer<byte[], byte[]> c = consumer(cp)) {
                c.assign(List.of(tp(0)));
                c.seek(tp(0), 99);
                String res;
                try {
                    List<ConsumerRecord<byte[], byte[]>> got = new ArrayList<>();
                    long deadline = System.currentTimeMillis() + 10000;
                    while (got.isEmpty() && System.currentTimeMillis() < deadline
                            && !(policy.equals("latest") && c.position(tp(0)) == 3)) {
                        for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(500))) got.add(r);
                    }
                    res = "read=" + values(got) + ",pos=" + c.position(tp(0));
                } catch (OffsetOutOfRangeException e) {
                    res = "OffsetOutOfRangeException(" + offsetsOf(e.offsetOutOfRangePartitions()) + ")";
                }
                out.add(policy + ":" + res);
            }
        }
        return String.join(" ", out);
    }

    static String idempotentProduce() throws Exception {
        Properties pp = new Properties();
        pp.put("enable.idempotence", "true");
        pp.put("acks", "all");
        pp.put("max.in.flight.requests.per.connection", "5");
        pp.put("linger.ms", "5");
        List<Future<RecordMetadata>> f = new ArrayList<>();
        try (KafkaProducer<byte[], byte[]> p = producer(pp)) {
            for (int i = 0; i < 50; i++) f.add(p.send(new ProducerRecord<>(topic, 0, null, b("i" + i))));
            p.flush();
        }
        List<Long> offs = new ArrayList<>();
        for (Future<RecordMetadata> x : f) offs.add(x.get().offset());
        boolean contiguous = true;
        for (int i = 0; i < offs.size(); i++) contiguous &= offs.get(i) == i;
        try (KafkaConsumer<byte[], byte[]> c = consumer(new Properties())) {
            c.assign(List.of(tp(0)));
            return "contiguous=" + contiguous + " first=" + offs.get(0) + " last=" + offs.get(49)
                    + " read=" + drain(c, 50, 30000).size();
        }
    }

    static String acks() throws Exception {
        List<String> out = new ArrayList<>();
        for (String a : List.of("0", "1", "all")) {
            Properties pp = new Properties();
            pp.put("acks", a);
            if (a.equals("0")) pp.put("enable.idempotence", "false");
            try (KafkaProducer<byte[], byte[]> p = producer(pp)) {
                RecordMetadata m = send(p, topic, 0, "acks-" + a);
                out.add("acks" + a + (a.equals("0") ? "" : "@" + m.offset()));
            }
        }
        try (KafkaConsumer<byte[], byte[]> c = consumer(new Properties())) {
            c.assign(List.of(tp(0)));
            return String.join(" ", out) + " read=" + values(drain(c, 3, 30000));
        }
    }

    static KafkaProducer<byte[], byte[]> txnProducer(String id) {
        Properties pp = new Properties();
        pp.put("transactional.id", id);
        pp.put("transaction.timeout.ms", "60000");
        KafkaProducer<byte[], byte[]> p = producer(pp);
        p.initTransactions();
        return p;
    }

    static void sendAll(KafkaProducer<byte[], byte[]> p, String t, int partition, String... vs) throws Exception {
        for (String v : vs) send(p, t, partition, v);
    }

    static String txnReadCommitted() throws Exception {
        Properties committed = new Properties();
        committed.put("isolation.level", "read_committed");
        String id = "java-rc-" + topic;
        try (KafkaProducer<byte[], byte[]> t = txnProducer(id);
             KafkaProducer<byte[], byte[]> plain = producer()) {
            t.beginTransaction();
            sendAll(t, topic, 0, "a0", "a1");
            t.commitTransaction();
            t.beginTransaction();
            sendAll(t, topic, 0, "b0", "b1");
            t.abortTransaction();
            send(plain, topic, 0, "n0");
            t.beginTransaction();
            sendAll(t, topic, 0, "c0");
            t.commitTransaction();

            t.beginTransaction();
            sendAll(t, topic, 0, "o0");
            String open = state(committed) + " | " + state(new Properties());
            t.commitTransaction();
            return "settled=[" + open.split(" \\| ")[0] + "] open_uncommitted=[" + open.split(" \\| ")[1]
                    + "] after_commit=[" + state(committed) + "]";
        }
    }

    /** What a consumer at `extra`'s isolation level sees: the values and the end offset. */
    static String state(Properties extra) {
        try (KafkaConsumer<byte[], byte[]> c = consumer(extra)) {
            c.assign(List.of(tp(0)));
            long end = c.endOffsets(List.of(tp(0))).get(tp(0));
            List<String> got = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 30000;
            while (c.position(tp(0)) < end && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(500))) got.add(s(r.value()));
            }
            return "end=" + end + " pos=" + c.position(tp(0)) + " values=" + String.join(",", got);
        }
    }

    static String txnConsumeTransformProduce() throws Exception {
        String out = topic + "-out";
        String group = topic + "-g";
        try (Admin a = admin()) {
            a.createTopics(List.of(new NewTopic(out, 1, (short) 1))).all().get();
        }
        Properties committed = new Properties();
        committed.put("isolation.level", "read_committed");
        try (KafkaProducer<byte[], byte[]> plain = producer()) {
            for (int i = 0; i < 5; i++) send(plain, topic, 0, "in" + i);
        }
        Properties cp = new Properties(committed);
        cp.put("isolation.level", "read_committed");
        cp.put("group.id", group);
        try (KafkaProducer<byte[], byte[]> t = txnProducer("java-cpt-" + topic);
             KafkaConsumer<byte[], byte[]> c = consumer(cp)) {
            c.assign(List.of(tp(0)));
            List<ConsumerRecord<byte[], byte[]>> batch = drain(c, 5, 30000);
            t.beginTransaction();
            for (ConsumerRecord<byte[], byte[]> r : batch) {
                send(t, out, 0, s(r.value()).toUpperCase());
            }
            t.sendOffsetsToTransaction(Map.of(tp(0), new OffsetAndMetadata(c.position(tp(0)))), c.groupMetadata());
            t.commitTransaction();
            long afterCommit = committedOffset(c);

            try (KafkaProducer<byte[], byte[]> plain = producer()) {
                send(plain, topic, 0, "in5");
            }
            List<ConsumerRecord<byte[], byte[]>> more = drain(c, 1, 30000);
            t.beginTransaction();
            send(t, out, 0, "SHOULD-NOT-APPEAR");
            t.sendOffsetsToTransaction(Map.of(tp(0), new OffsetAndMetadata(c.position(tp(0)))), c.groupMetadata());
            t.abortTransaction();
            long afterAbort = committedOffset(c);

            List<ConsumerRecord<byte[], byte[]>> outRead =
                    readPartition(committed, new TopicPartition(out, 0));
            return "processed=" + batch.size() + " committed_after_commit=" + afterCommit
                    + " second_batch=" + more.size() + " committed_after_abort=" + afterAbort
                    + " output=" + values(outRead);
        }
    }

    /** Retried: Kafka writes transaction markers, and so the offsets, after commit returns. */
    static long committedOffset(KafkaConsumer<byte[], byte[]> c) throws Exception {
        long offset = -1;
        for (int i = 0; i < 40 && offset < 0; i++) {
            OffsetAndMetadata m = c.committed(Set.of(tp(0))).get(tp(0));
            offset = m == null ? -1 : m.offset();
            if (offset < 0) Thread.sleep(250);
        }
        return offset;
    }

    static String txnFenced() throws Exception {
        String id = "java-fence-" + topic;
        KafkaProducer<byte[], byte[]> first = txnProducer(id);
        String fenced = "none";
        String winner;
        try {
            first.beginTransaction();
            send(first, topic, 0, "stale");
            try (KafkaProducer<byte[], byte[]> second = txnProducer(id)) {
                second.beginTransaction();
                send(second, topic, 0, "fresh");
                try {
                    send(first, topic, 0, "stale-2");
                    first.commitTransaction();
                } catch (Exception e) {
                    fenced = err(e);
                }
                second.commitTransaction();
            }
        } finally {
            try { first.close(Duration.ofSeconds(5)); } catch (Exception ignored) { }
        }
        Properties committed = new Properties();
        committed.put("isolation.level", "read_committed");
        winner = values(readPartition(committed, tp(0)));
        return "first_failed_with=" + fenced + " read_committed=" + winner;
    }

    static class Member {
        final KafkaConsumer<byte[], byte[]> c;
        final Set<TopicPartition> owned = new TreeSet<>(Comparator.comparingInt(TopicPartition::partition));
        final List<Integer> revocations = new ArrayList<>();
        final List<String> got = new ArrayList<>();

        Member(String group, Properties extra) {
            Properties p = new Properties();
            p.put("group.id", group);
            // The consumer protocol takes its timeouts from the broker and refuses these.
            if (!"consumer".equals(extra.get("group.protocol"))) p.put("session.timeout.ms", "30000");
            p.putAll(extra);
            c = consumer(p);
            c.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                public void onPartitionsRevoked(Collection<TopicPartition> ps) {
                    revocations.add(ps.size());
                    owned.removeAll(ps);
                }
                public void onPartitionsAssigned(Collection<TopicPartition> ps) { owned.addAll(ps); }
                public void onPartitionsLost(Collection<TopicPartition> ps) { owned.removeAll(ps); }
            });
        }

        void poll() {
            for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(200))) got.add(s(r.value()));
        }
    }

    /** Poll every member until the assignments cover `partitions` with no overlap and stay so. */
    static void settle(int partitions, Member... ms) {
        int stable = 0;
        long deadline = System.currentTimeMillis() + 90000;
        while (stable < 15 && System.currentTimeMillis() < deadline) {
            for (Member m : ms) m.poll();
            Set<TopicPartition> all = new HashSet<>();
            int sum = 0;
            for (Member m : ms) { all.addAll(m.owned); sum += m.owned.size(); }
            boolean ok = all.size() == partitions && sum == partitions;
            for (Member m : ms) ok &= !m.owned.isEmpty() || ms.length == 0;
            stable = ok ? stable + 1 : 0;
        }
        if (stable < 15) throw new IllegalStateException("assignment never settled");
    }

    static String sizes(Member... ms) {
        return Arrays.stream(ms).map(m -> m.owned.size()).sorted().map(String::valueOf)
                .collect(Collectors.joining(","));
    }

    static void produceSpread(int n) throws Exception {
        try (KafkaProducer<byte[], byte[]> p = producer()) {
            for (int i = 0; i < n; i++) send(p, topic, i % 3, "m" + i);
        }
    }

    static String consumeAll(int n, Member... ms) {
        long deadline = System.currentTimeMillis() + 60000;
        int total = 0;
        while (total < n && System.currentTimeMillis() < deadline) {
            for (Member m : ms) m.poll();
            total = Arrays.stream(ms).mapToInt(m -> m.got.size()).sum();
        }
        // Drain a little longer to catch a duplicate delivered after the last expected record.
        for (int i = 0; i < 5; i++) for (Member m : ms) m.poll();
        Set<String> uniq = new HashSet<>();
        int all = 0;
        for (Member m : ms) { uniq.addAll(m.got); all += m.got.size(); }
        return "consumed=" + all + " unique=" + uniq.size();
    }

    static String groupTwoMembers() throws Exception {
        String g = topic + "-g";
        Member a = new Member(g, new Properties());
        Member b = new Member(g, new Properties());
        try {
            settle(3, a, b);
            String before = sizes(a, b);
            produceSpread(30);
            return "sizes=" + before + " " + consumeAll(30, a, b);
        } finally {
            a.c.close();
            b.c.close();
        }
    }

    static String groupRebalanceLeave() throws Exception {
        String g = topic + "-g";
        Member a = new Member(g, new Properties());
        Member b = new Member(g, new Properties());
        boolean bOpen = true;
        try {
            settle(3, a, b);
            String before = sizes(a, b);
            b.c.close();
            bOpen = false;
            long deadline = System.currentTimeMillis() + 60000;
            while (a.owned.size() < 3 && System.currentTimeMillis() < deadline) a.poll();
            String after = String.valueOf(a.owned.size());
            produceSpread(9);
            return "before=" + before + " after_leave=" + after + " " + consumeAll(9, a);
        } finally {
            if (bOpen) b.c.close();
            a.c.close();
        }
    }

    static String groupResume() throws Exception {
        String g = topic + "-g";
        try (KafkaProducer<byte[], byte[]> p = producer()) {
            for (int i = 0; i < 5; i++) send(p, topic, 0, "first-" + i);
        }
        Properties gp = new Properties();
        gp.put("group.id", g);
        List<String> firstRun;
        try (KafkaConsumer<byte[], byte[]> c = consumer(gp)) {
            c.subscribe(List.of(topic));
            firstRun = drain(c, 5, 60000).stream().map(r -> s(r.value())).collect(Collectors.toList());
            c.commitSync();
            c.commitSync(Map.of(tp(0), new OffsetAndMetadata(5, Optional.empty(), "meta-x")));
        }
        try (KafkaProducer<byte[], byte[]> p = producer()) {
            for (int i = 0; i < 3; i++) send(p, topic, 0, "second-" + i);
        }
        try (KafkaConsumer<byte[], byte[]> c = consumer(gp)) {
            OffsetAndMetadata stored = c.committed(Set.of(tp(0))).get(tp(0));
            c.subscribe(List.of(topic));
            List<String> secondRun = drain(c, 3, 60000).stream().map(r -> s(r.value())).collect(Collectors.toList());
            return "first=" + firstRun.size() + " committed=" + stored.offset() + "/" + stored.metadata()
                    + " second=" + String.join(",", secondRun);
        }
    }

    static String groupStatic() throws Exception {
        String g = topic + "-g";
        Properties pa = new Properties();
        pa.put("group.instance.id", "inst-a");
        Properties pb = new Properties();
        pb.put("group.instance.id", "inst-b");
        Member a = new Member(g, pa);
        Member b = new Member(g, pb);
        Member b2 = null;
        Member usurper = null;
        try {
            settle(3, a, b);
            Set<TopicPartition> bHad = new TreeSet<>(Comparator.comparingInt(TopicPartition::partition));
            bHad.addAll(b.owned);
            Set<TopicPartition> aHad = new TreeSet<>(Comparator.comparingInt(TopicPartition::partition));
            aHad.addAll(a.owned);
            // A static member that goes away without leaving keeps its slot.
            b.c.close();
            for (int i = 0; i < 10; i++) a.poll();
            boolean aStable = a.owned.equals(aHad);
            b2 = new Member(g, pb);
            long deadline = System.currentTimeMillis() + 60000;
            while (b2.owned.isEmpty() && System.currentTimeMillis() < deadline) { a.poll(); b2.poll(); }
            boolean same = b2.owned.equals(bHad);
            boolean aStillStable = a.owned.equals(aHad);
            // Two live consumers cannot share an instance id.
            usurper = new Member(g, pb);
            String fenced = "none";
            long until = System.currentTimeMillis() + 60000;
            while (fenced.equals("none") && System.currentTimeMillis() < until) {
                try {
                    usurper.poll();
                    b2.poll();
                } catch (Exception e) {
                    fenced = err(e);
                }
                if (fenced.equals("none")) { }
            }
            return "a_stable_while_b_away=" + aStable + " b_same_assignment=" + same
                    + " a_stable_after=" + aStillStable + " duplicate_instance=" + fenced;
        } finally {
            for (Member m : new Member[]{a, b2, usurper}) {
                if (m != null) try { m.c.close(Duration.ofSeconds(5)); } catch (Exception ignored) { }
            }
        }
    }

    static String groupCooperative() throws Exception {
        String g = topic + "-g";
        Properties cp = new Properties();
        cp.put("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        Member a = new Member(g, cp);
        try {
            long deadline = System.currentTimeMillis() + 60000;
            while (a.owned.size() < 3 && System.currentTimeMillis() < deadline) a.poll();
            Member b = new Member(g, cp);
            try {
                settle(3, a, b);
                // Eager rebalancing revokes all three from A; cooperative revokes only what moves.
                int max = a.revocations.stream().mapToInt(Integer::intValue).max().orElse(0);
                String sz = sizes(a, b);
                produceSpread(12);
                return "sizes=" + sz + " revoked_everything=" + (max == 3) + " " + consumeAll(12, a, b);
            } finally {
                b.c.close();
            }
        } finally {
            a.c.close();
        }
    }

    static String groupKip848() throws Exception {
        String g = topic + "-g";
        Properties cp = new Properties();
        cp.put("group.protocol", "consumer");
        Member a = new Member(g, cp);
        Member b = new Member(g, cp);
        try {
            settle(3, a, b);
            String sz = sizes(a, b);
            produceSpread(12);
            String consumed = consumeAll(12, a, b);
            try (Admin admin = admin()) {
                ConsumerGroupDescription d = admin.describeConsumerGroups(List.of(g)).all().get().get(g);
                return "sizes=" + sz + " " + consumed + " type=" + d.type() + " state=" + d.groupState()
                        + " members=" + d.members().size();
            }
        } finally {
            a.c.close();
            b.c.close();
        }
    }

    static String groupAdmin() throws Exception {
        String g = topic + "-g";
        try (KafkaProducer<byte[], byte[]> p = producer()) {
            for (int i = 0; i < 6; i++) send(p, topic, 0, "ga" + i);
        }
        Member m = new Member(g, new Properties());
        try (Admin a = admin()) {
            long deadline = System.currentTimeMillis() + 60000;
            while (m.got.size() < 6 && System.currentTimeMillis() < deadline) m.poll();
            m.c.commitSync();
            List<String> out = new ArrayList<>();
            boolean listed = a.listGroups().all().get().stream().anyMatch(x -> x.groupId().equals(g));
            ConsumerGroupDescription d = a.describeConsumerGroups(List.of(g)).all().get().get(g);
            out.add("listed=" + listed + " state=" + d.groupState() + " members=" + d.members().size()
                    + " assigned=" + d.members().iterator().next().assignment().topicPartitions().size()
                    + " assignor_set=" + (d.partitionAssignor() != null && !d.partitionAssignor().isEmpty()));
            out.add("offsets=" + a.listConsumerGroupOffsets(g).partitionsToOffsetAndMetadata().get()
                    .entrySet().stream().map(e -> e.getKey().partition() + ":" + e.getValue().offset())
                    .collect(Collectors.joining(",")));
            String busy;
            try {
                a.alterConsumerGroupOffsets(g, Map.of(tp(0), new OffsetAndMetadata(2))).all().get();
                busy = "altered";
            } catch (Exception e) {
                busy = err(e);
            }
            out.add("alter_while_active=" + busy);
            try {
                a.deleteConsumerGroups(List.of(g)).all().get();
                busy = "deleted";
            } catch (Exception e) {
                busy = err(e);
            }
            out.add("delete_while_active=" + busy);
            m.c.close();
            // The group empties a moment after the LeaveGroup that close sends.
            for (int i = 0; i < 40; i++) {
                if (a.describeConsumerGroups(List.of(g)).all().get().get(g).members().isEmpty()) break;
                Thread.sleep(250);
            }
            a.alterConsumerGroupOffsets(g, Map.of(tp(0), new OffsetAndMetadata(2))).all().get();
            out.add("after_alter=" + a.listConsumerGroupOffsets(g).partitionsToOffsetAndMetadata().get()
                    .get(tp(0)).offset());
            a.deleteConsumerGroupOffsets(g, Set.of(tp(0))).all().get();
            out.add("offsets_after_delete=" + a.listConsumerGroupOffsets(g).partitionsToOffsetAndMetadata().get()
                    .size());
            a.deleteConsumerGroups(List.of(g)).all().get();
            boolean gone = a.listGroups().all().get().stream().noneMatch(x -> x.groupId().equals(g));
            out.add("gone=" + gone);
            return String.join(" ", out);
        } finally {
            try { m.c.close(Duration.ofSeconds(5)); } catch (Exception ignored) { }
        }
    }

    static String attempt(Callable0 c) {
        try {
            c.call();
            return "ok";
        } catch (Exception e) {
            return err(e);
        }
    }

    interface Callable0 { void call() throws Exception; }

    static String adminTopics() throws Exception {
        String t = topic + "-a";
        try (Admin a = admin()) {
            List<String> out = new ArrayList<>();
            Map<String, String> cfg = new HashMap<>();
            cfg.put("retention.ms", "3600000");
            a.createTopics(List.of(new NewTopic(t, 3, (short) 1).configs(cfg))).all().get();
            out.add("again=" + attempt(() -> a.createTopics(List.of(new NewTopic(t, 3, (short) 1))).all().get()));
            out.add("zero_partitions=" + attempt(() -> a.createTopics(List.of(new NewTopic(t + "-z", 0, (short) 1))).all().get()));
            out.add("rf2=" + attempt(() -> a.createTopics(List.of(new NewTopic(t + "-r", 1, (short) 2))).all().get()));
            out.add("bad_config=" + attempt(() -> a.createTopics(List.of(new NewTopic(t + "-c", 1, (short) 1)
                    .configs(Map.of("no.such.config", "1")))).all().get()));
            out.add("bad_name=" + attempt(() -> a.createTopics(List.of(new NewTopic("bad name!", 1, (short) 1))).all().get()));

            TopicDescription d = a.describeTopics(List.of(t)).allTopicNames().get().get(t);
            out.add("partitions=" + d.partitions().size() + " rf=" + d.partitions().get(0).replicas().size()
                    + " isr=" + d.partitions().get(0).isr().size() + " internal=" + d.isInternal()
                    + " has_id=" + (d.topicId() != null && !d.topicId().equals(Uuid.ZERO_UUID)));
            out.add("listed=" + a.listTopics().names().get().contains(t));
            Config c = a.describeConfigs(List.of(new ConfigResource(ConfigResource.Type.TOPIC, t))).all().get()
                    .values().iterator().next();
            out.add("retention.ms=" + c.get("retention.ms").value() + "/" + c.get("retention.ms").source());
            a.createPartitions(Map.of(t, NewPartitions.increaseTo(5))).all().get();
            out.add("grown=" + a.describeTopics(List.of(t)).allTopicNames().get().get(t).partitions().size());
            out.add("shrink=" + attempt(() -> a.createPartitions(Map.of(t, NewPartitions.increaseTo(2))).all().get()));
            out.add("grow_missing=" + attempt(() -> a.createPartitions(Map.of(t + "-nope", NewPartitions.increaseTo(2))).all().get()));
            a.deleteTopics(List.of(t)).all().get();
            out.add("delete_again=" + attempt(() -> a.deleteTopics(List.of(t)).all().get()));
            out.add("describe_deleted=" + attempt(() -> a.describeTopics(List.of(t)).allTopicNames().get()));
            return String.join(" ", out);
        }
    }

    static String adminConfigs() throws Exception {
        try (Admin a = admin()) {
            ConfigResource r = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            List<String> out = new ArrayList<>();
            Config def = a.describeConfigs(List.of(r)).all().get().get(r);
            for (String k : List.of("retention.ms", "cleanup.policy", "max.message.bytes", "segment.bytes",
                    "min.compaction.lag.ms", "compression.type", "message.timestamp.type")) {
                ConfigEntry e = def.get(k);
                out.add(k + "=" + (e == null ? "absent" : e.value() + "/" + e.source()));
            }
            a.incrementalAlterConfigs(Map.of(r, List.of(new AlterConfigOp(
                    new ConfigEntry("retention.ms", "12345"), AlterConfigOp.OpType.SET)))).all().get();
            ConfigEntry set = a.describeConfigs(List.of(r)).all().get().get(r).get("retention.ms");
            out.add("after_set=" + set.value() + "/" + set.source());
            a.incrementalAlterConfigs(Map.of(r, List.of(new AlterConfigOp(
                    new ConfigEntry("retention.ms", null), AlterConfigOp.OpType.DELETE)))).all().get();
            ConfigEntry reset = a.describeConfigs(List.of(r)).all().get().get(r).get("retention.ms");
            out.add("after_delete=" + reset.value() + "/" + reset.source());
            out.add("unknown_key=" + attempt(() -> a.incrementalAlterConfigs(Map.of(r, List.of(new AlterConfigOp(
                    new ConfigEntry("no.such.config", "1"), AlterConfigOp.OpType.SET)))).all().get()));
            out.add("bad_value=" + attempt(() -> a.incrementalAlterConfigs(Map.of(r, List.of(new AlterConfigOp(
                    new ConfigEntry("retention.ms", "not-a-number"), AlterConfigOp.OpType.SET)))).all().get()));
            ConfigResource missing = new ConfigResource(ConfigResource.Type.TOPIC, topic + "-missing");
            out.add("describe_missing=" + attempt(() -> a.describeConfigs(List.of(missing)).all().get()));
            String id = a.describeCluster().nodes().get().iterator().next().idString();
            Config broker = a.describeConfigs(List.of(new ConfigResource(ConfigResource.Type.BROKER, id)))
                    .all().get().values().iterator().next();
            out.add("broker_config=" + (broker.entries().size() > 0));
            return String.join(" ", out);
        }
    }

    static String adminCluster() throws Exception {
        try (Admin a = admin()) {
            DescribeClusterResult dc = a.describeCluster();
            Collection<Node> nodes = dc.nodes().get();
            Node controller = dc.controller().get();
            List<String> out = new ArrayList<>();
            out.add("nodes=" + nodes.size() + " controller_listed="
                    + nodes.stream().anyMatch(n -> n.id() == controller.id())
                    + " cluster_id=" + (dc.clusterId().get() != null && !dc.clusterId().get().isEmpty()));
            FeatureMetadata f = a.describeFeatures().featureMetadata().get();
            for (String name : List.of("transaction.version", "group.version")) {
                FinalizedVersionRange r = f.finalizedFeatures().get(name);
                out.add(name + "=" + (r == null ? "absent" : r.maxVersionLevel()));
            }
            TopicPartition p0 = tp(0);
            out.add("elect_preferred=" + electResult(a, p0));
            out.add("reassignments=" + a.listPartitionReassignments().reassignments().get().size());
            Map<Integer, Map<String, LogDirDescription>> dirs =
                    a.describeLogDirs(nodes.stream().map(Node::id).collect(Collectors.toList())).allDescriptions().get();
            boolean hasPartition = dirs.values().stream().flatMap(m -> m.values().stream())
                    .anyMatch(d -> d.replicaInfos().containsKey(p0));
            out.add("log_dirs_cover_partition=" + hasPartition);
            return String.join(" ", out);
        }
    }

    static String electResult(Admin a, TopicPartition p) throws Exception {
        Map<TopicPartition, Optional<Throwable>> res =
                a.electLeaders(ElectionType.PREFERRED, Set.of(p)).partitions().get();
        Optional<Throwable> e = res.get(p);
        return e.isPresent() ? err(e.get()) : "elected";
    }

    static String adminDeleteRecords() throws Exception {
        try (KafkaProducer<byte[], byte[]> p = producer()) {
            for (int i = 0; i < 10; i++) send(p, topic, 0, "d" + i);
        }
        try (Admin a = admin()) {
            List<String> out = new ArrayList<>();
            long low = a.deleteRecords(Map.of(tp(0), RecordsToDelete.beforeOffset(4))).lowWatermarks()
                    .get(tp(0)).get().lowWatermark();
            out.add("low=" + low);
            try (KafkaConsumer<byte[], byte[]> c = consumer(new Properties())) {
                c.assign(List.of(tp(0)));
                out.add("begin=" + c.beginningOffsets(List.of(tp(0))).get(tp(0)));
                out.add("read=" + values(drain(c, 6, 30000)));
            }
            out.add("beyond_end=" + attempt(() -> a.deleteRecords(Map.of(tp(0), RecordsToDelete.beforeOffset(99)))
                    .all().get()));
            long all = a.deleteRecords(Map.of(tp(0), RecordsToDelete.beforeOffset(-1))).lowWatermarks()
                    .get(tp(0)).get().lowWatermark();
            out.add("to_end=" + all);
            return String.join(" ", out);
        }
    }

    static String adminOffsets() throws Exception {
        long t0 = 1_700_000_000_000L;
        long[] stamps = {t0 + 2000, t0, t0 + 5000, t0 + 1000};
        try (KafkaProducer<byte[], byte[]> p = producer()) {
            for (int i = 0; i < stamps.length; i++) {
                send(p, new ProducerRecord<>(topic, 0, stamps[i], null, b("x" + i)));
            }
        }
        try (Admin a = admin()) {
            List<String> out = new ArrayList<>();
            Map<String, OffsetSpec> specs = new LinkedHashMap<>();
            specs.put("earliest", OffsetSpec.earliest());
            specs.put("latest", OffsetSpec.latest());
            specs.put("max_timestamp", OffsetSpec.maxTimestamp());
            specs.put("at_t0+1500", OffsetSpec.forTimestamp(t0 + 1500));
            specs.put("at_far_future", OffsetSpec.forTimestamp(t0 + 99999));
            for (Map.Entry<String, OffsetSpec> e : specs.entrySet()) {
                ListOffsetsResult.ListOffsetsResultInfo i =
                        a.listOffsets(Map.of(tp(0), e.getValue())).partitionResult(tp(0)).get();
                out.add(e.getKey() + "=" + i.offset() + (e.getKey().startsWith("at_") || e.getKey().equals("max_timestamp")
                        ? "@" + (i.timestamp() < 0 ? i.timestamp() : i.timestamp() - t0) : ""));
            }
            return String.join(" ", out);
        }
    }

    static String adminProducers() throws Exception {
        Properties pp = new Properties();
        pp.put("enable.idempotence", "true");
        pp.put("acks", "all");
        List<String> out = new ArrayList<>();
        try (KafkaProducer<byte[], byte[]> p = producer(pp); Admin a = admin()) {
            for (int i = 0; i < 4; i++) send(p, topic, 0, "ip" + i);
            DescribeProducersResult.PartitionProducerState st =
                    a.describeProducers(List.of(tp(0))).partitionResult(tp(0)).get();
            out.add("idempotent_producers=" + st.activeProducers().size()
                    + " last_sequence=" + st.activeProducers().get(0).lastSequence());
        }
        String id = "java-admin-" + topic;
        try (KafkaProducer<byte[], byte[]> t = txnProducer(id); Admin a = admin()) {
            t.beginTransaction();
            send(t, topic, 0, "open-a");
            DescribeProducersResult.PartitionProducerState st =
                    a.describeProducers(List.of(tp(0))).partitionResult(tp(0)).get();
            ProducerState open = st.activeProducers().stream()
                    .filter(x -> x.currentTransactionStartOffset().isPresent()).findFirst().get();
            out.add("open_txn_start=" + open.currentTransactionStartOffset().getAsLong());
            Collection<TransactionListing> listing = a.listTransactions().all().get();
            TransactionListing mine = listing.stream().filter(x -> x.transactionalId().equals(id)).findFirst().get();
            out.add("listed_state=" + mine.state());
            TransactionDescription td = a.describeTransactions(List.of(id)).description(id).get();
            out.add("described_state=" + td.state() + " partitions=" + td.topicPartitions().size());
            a.abortTransaction(new AbortTransactionSpec(tp(0), open.producerId(), (short) open.producerEpoch(),
                    open.coordinatorEpoch().orElse(0))).all().get();
            Properties committed = new Properties();
            committed.put("isolation.level", "read_committed");
            out.add("after_admin_abort=[" + state(committed) + "]");
        }
        return String.join(" ", out);
    }

    /** One user's quotas, retried: a broker may apply an alteration late. */
    static Map<ClientQuotaEntity, Map<String, Double>> quotasFor(Admin a, String user, boolean wantSome)
            throws Exception {
        Map<ClientQuotaEntity, Map<String, Double>> got = Map.of();
        for (int i = 0; i < 40; i++) {
            got = a.describeClientQuotas(ClientQuotaFilter.contains(List.of(
                    ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, user)))).entities().get();
            if (got.isEmpty() != wantSome) break;
            Thread.sleep(250);
        }
        return got;
    }

    static String adminQuotas() throws Exception {
        String user = "conf-user-" + topic;
        try (Admin a = admin()) {
            ClientQuotaEntity e = new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, user));
            List<String> out = new ArrayList<>();
            a.alterClientQuotas(List.of(new ClientQuotaAlteration(e,
                    List.of(new ClientQuotaAlteration.Op("producer_byte_rate", 1048576.0))))).all().get();
            out.add("set=" + quotasFor(a, user, true).values());
            a.alterClientQuotas(List.of(new ClientQuotaAlteration(e,
                    List.of(new ClientQuotaAlteration.Op("producer_byte_rate", null))))).all().get();
            out.add("after_remove=" + quotasFor(a, user, false).size());
            out.add("unknown_quota=" + attempt(() -> a.alterClientQuotas(List.of(new ClientQuotaAlteration(e,
                    List.of(new ClientQuotaAlteration.Op("no_such_quota", 1.0))))).all().get()));
            return String.join(" ", out);
        }
    }

    static String shareConsume() throws Exception {
        String g = topic + "-share";
        Properties sp = base();
        sp.put("group.id", g);
        sp.put("key.deserializer", ByteArrayDeserializer.class.getName());
        sp.put("value.deserializer", ByteArrayDeserializer.class.getName());
        sp.put("share.acknowledgement.mode", "explicit");
        try (KafkaShareConsumer<byte[], byte[]> c = new KafkaShareConsumer<>(sp)) {
            c.subscribe(List.of(topic));
            // A new share group starts at the log end, set some time after the first heartbeat,
            // so probe until one arrives before sending the scenario's records.
            boolean reading = false;
            try (KafkaProducer<byte[], byte[]> p = producer()) {
                long ready = System.currentTimeMillis() + 90000;
                while (!reading && System.currentTimeMillis() < ready) {
                    send(p, topic, 0, "probe");
                    for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(1500))) {
                        reading = true;
                        c.acknowledge(r, AcknowledgeType.ACCEPT);
                    }
                    c.commitSync();
                }
                if (!reading) throw new IllegalStateException("the share group never started reading");
                for (int i = 0; i < 3; i++) send(p, topic, 0, "sh" + i);
            }
            Map<String, AcknowledgeType> verdict = Map.of(
                    "sh0", AcknowledgeType.ACCEPT, "sh1", AcknowledgeType.RELEASE, "sh2", AcknowledgeType.REJECT);
            List<String> first = new ArrayList<>();
            String redelivered = "none";
            long deadline = System.currentTimeMillis() + 90000;
            while (System.currentTimeMillis() < deadline && !(first.size() >= 3 && !redelivered.equals("none"))) {
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(1000))) {
                    String v = s(r.value());
                    if (v.equals("probe")) {
                        c.acknowledge(r, AcknowledgeType.ACCEPT);
                        continue;
                    }
                    int count = r.deliveryCount().orElse((short) 1);
                    if (count == 1) {
                        first.add(v);
                        c.acknowledge(r, verdict.get(v));
                    } else {
                        redelivered = v + "@" + count;
                        c.acknowledge(r, AcknowledgeType.ACCEPT);
                    }
                }
                c.commitSync();
            }
            // Nothing else may come back: the accepted and rejected records are finished.
            int extra = 0;
            for (int i = 0; i < 3; i++) extra += c.poll(Duration.ofMillis(1000)).count();
            Collections.sort(first);
            return "first=" + String.join(",", first) + " redelivered=" + redelivered + " extra=" + extra;
        }
    }

    /** A Member polled on its own thread, for scenarios where the main thread has other work. */
    static class BgMember {
        final Member m;
        volatile Set<TopicPartition> ownedSnap = Set.of();
        volatile List<String> gotSnap = List.of();
        volatile boolean stop;
        final Thread t;

        BgMember(String group, Properties extra) {
            m = new Member(group, extra);
            t = new Thread(() -> {
                while (!stop) {
                    try {
                        m.poll();
                    } catch (Exception e) {
                        if (stop) return;
                    }
                    ownedSnap = new HashSet<>(m.owned);
                    gotSnap = new ArrayList<>(m.got);
                }
            });
            t.setDaemon(true);
            t.start();
        }

        Set<TopicPartition> owned() { return ownedSnap; }

        List<String> received() { return gotSnap; }

        void close() throws InterruptedException {
            stop = true;
            t.join(30000);
            m.c.close();
        }
    }

    static void createTopic(Admin a, String name, int partitions, Map<String, String> configs) throws Exception {
        a.createTopics(List.of(new NewTopic(name, partitions, (short) 1).configs(configs))).all().get();
    }

    static String topicRecreate() throws Exception {
        try (KafkaProducer<byte[], byte[]> p = producer(); Admin a = admin()) {
            for (int i = 0; i < 3; i++) send(p, topic, 0, "old" + i);
            Uuid before = a.describeTopics(List.of(topic)).allTopicNames().get().get(topic).topicId();
            a.deleteTopics(List.of(topic)).all().get();
            for (int i = 0; i < 60; i++) {
                if (attempt(() -> a.describeTopics(List.of(topic)).allTopicNames().get()).equals("UnknownTopicOrPartitionException")) break;
                Thread.sleep(250);
            }
            String created = "TopicExistsException";
            for (int i = 0; i < 60 && created.equals("TopicExistsException"); i++) {
                created = attempt(() -> createTopic(a, topic, 1, Map.of()));
                if (!created.equals("ok")) Thread.sleep(250);
            }
            Uuid after = a.describeTopics(List.of(topic)).allTopicNames().get().get(topic).topicId();
            // A fresh producer, since the first caches the old topic's metadata.
            try (KafkaProducer<byte[], byte[]> p2 = producer()) {
                long o0 = send(p2, topic, 0, "new0").offset();
                long o1 = send(p2, topic, 0, "new1").offset();
                try (KafkaConsumer<byte[], byte[]> c = consumer(new Properties())) {
                    c.assign(List.of(tp(0)));
                    return "recreated=" + created + " new_id=" + !before.equals(after) + " offsets=" + o0 + "," + o1
                            + " read=" + values(drain(c, 2, 30000))
                            + " begin=" + c.beginningOffsets(List.of(tp(0))).get(tp(0))
                            + " end=" + c.endOffsets(List.of(tp(0))).get(tp(0));
                }
            }
        }
    }

    static String topicLimits() throws Exception {
        String t = topic + "-lim";
        try (Admin a = admin()) {
            createTopic(a, t, 1, Map.of("max.message.bytes", "2048"));
            ConfigResource r = new ConfigResource(ConfigResource.Type.TOPIC, t);
            ConfigEntry e = a.describeConfigs(List.of(r)).all().get().get(r).get("max.message.bytes");
            try (KafkaProducer<byte[], byte[]> p = producer()) {
                String big = "none";
                try {
                    send(p, new ProducerRecord<>(t, 0, null, new byte[4000]));
                } catch (Exception ex) {
                    big = err(ex);
                }
                long small = send(p, new ProducerRecord<>(t, 0, null, new byte[1000])).offset();
                return "config=" + e.value() + "/" + e.source() + " over_limit=" + big + " under_limit_offset=" + small;
            }
        }
    }

    static String manyPartitions() throws Exception {
        int n = 50;
        try (KafkaProducer<byte[], byte[]> p = producer()) {
            for (int i = 0; i < n; i++) send(p, topic, i, "mp" + i);
        }
        List<TopicPartition> tps = new ArrayList<>();
        for (int i = 0; i < n; i++) tps.add(tp(i));
        try (Admin a = admin(); KafkaConsumer<byte[], byte[]> c = consumer(new Properties())) {
            int described = a.describeTopics(List.of(topic)).allTopicNames().get().get(topic).partitions().size();
            long total = 0;
            Map<TopicPartition, OffsetSpec> specs = new HashMap<>();
            for (TopicPartition t : tps) specs.put(t, OffsetSpec.latest());
            for (ListOffsetsResult.ListOffsetsResultInfo i : a.listOffsets(specs).all().get().values()) total += i.offset();
            c.assign(tps);
            Set<Integer> seen = new TreeSet<>();
            for (ConsumerRecord<byte[], byte[]> r : drain(c, n, 60000)) seen.add(r.partition());
            return "described=" + described + " end_sum=" + total + " read_partitions=" + seen.size();
        }
    }

    static String manyRecords() throws Exception {
        int n = 20000;
        Properties pp = new Properties();
        pp.put("linger.ms", "20");
        pp.put("batch.size", "65536");
        List<byte[]> sent = new ArrayList<>();
        try (KafkaProducer<byte[], byte[]> p = producer(pp)) {
            List<Future<RecordMetadata>> f = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                byte[] v = b(String.format("%08d-", i).repeat(10));
                sent.add(v);
                f.add(p.send(new ProducerRecord<>(topic, 0, null, v)));
            }
            p.flush();
            for (Future<RecordMetadata> x : f) x.get(60, TimeUnit.SECONDS);
        }
        try (KafkaConsumer<byte[], byte[]> c = consumer(new Properties())) {
            c.assign(List.of(tp(0)));
            List<byte[]> got = drain(c, n, 120000).stream().map(ConsumerRecord::value).collect(Collectors.toList());
            return "count=" + got.size() + " crc=" + crc(got) + " same=" + crc(got).equals(crc(sent));
        }
    }

    static String seekIntoBatch() throws Exception {
        long t0 = 1_700_000_000_000L;
        Properties pp = new Properties();
        pp.put("linger.ms", "1000");
        pp.put("batch.size", "1048576");
        List<Future<RecordMetadata>> f = new ArrayList<>();
        try (KafkaProducer<byte[], byte[]> p = producer(pp)) {
            for (int i = 0; i < 100; i++) f.add(p.send(new ProducerRecord<>(topic, 0, t0 + i, null, b("b" + i))));
            p.flush();
        }
        boolean oneBatch = true;
        for (Future<RecordMetadata> x : f) oneBatch &= x.get().offset() == f.indexOf(x);
        try (KafkaConsumer<byte[], byte[]> c = consumer(new Properties())) {
            c.assign(List.of(tp(0)));
            c.seek(tp(0), 37);
            ConsumerRecord<byte[], byte[]> first = drain(c, 1, 30000).get(0);
            c.seek(tp(0), 37);
            String five = values(drain(c, 5, 30000).subList(0, 5));
            c.seek(tp(0), 99);
            String last = values(drain(c, 1, 30000));
            c.seek(tp(0), 100);
            int past = c.poll(Duration.ofMillis(1500)).count();
            OffsetAndTimestamp mid = c.offsetsForTimes(Map.of(tp(0), t0 + 50)).get(tp(0));
            return "contiguous=" + oneBatch + " first=" + first.offset() + " five=" + five + " last=" + last
                    + " past_end=" + past + " by_time_mid=" + mid.offset();
        }
    }

    static String groupPartitionGrowth() throws Exception {
        String g = topic + "-g";
        Properties cp = new Properties();
        cp.put("metadata.max.age.ms", "1000");
        BgMember m = new BgMember(g, cp);
        try (Admin a = admin()) {
            long deadline = System.currentTimeMillis() + 60000;
            while (m.owned().isEmpty() && System.currentTimeMillis() < deadline) Thread.sleep(200);
            int before = m.owned().size();
            a.createPartitions(Map.of(topic, NewPartitions.increaseTo(3))).all().get();
            while (m.owned().size() < 3 && System.currentTimeMillis() < deadline) Thread.sleep(200);
            int after = m.owned().size();
            produceSpread(9);
            while (m.received().size() < 9 && System.currentTimeMillis() < deadline) Thread.sleep(200);
            Thread.sleep(1500);
            return "before=" + before + " after=" + after + " consumed=" + m.received().size()
                    + " unique=" + new HashSet<>(m.received()).size();
        } finally {
            m.close();
        }
    }

    static String groupPattern() throws Exception {
        String g = topic + "-g";
        String prefix = topic + "-p";
        try (Admin a = admin()) {
            createTopic(a, prefix + "1", 1, Map.of());
            createTopic(a, prefix + "2", 1, Map.of());
            Properties cp = new Properties();
            cp.put("group.id", g);
            cp.put("metadata.max.age.ms", "1000");
            try (KafkaConsumer<byte[], byte[]> c = consumer(cp)) {
                c.subscribe(java.util.regex.Pattern.compile(java.util.regex.Pattern.quote(prefix) + "\\d"));
                long deadline = System.currentTimeMillis() + 60000;
                while (c.assignment().size() < 2 && System.currentTimeMillis() < deadline) c.poll(Duration.ofMillis(200));
                int before = c.assignment().size();
                createTopic(a, prefix + "3", 1, Map.of());
                while (c.assignment().size() < 3 && System.currentTimeMillis() < deadline) c.poll(Duration.ofMillis(200));
                int after = c.assignment().size();
                try (KafkaProducer<byte[], byte[]> p = producer()) {
                    for (int i = 1; i <= 3; i++) send(p, prefix + i, 0, "pat" + i);
                }
                List<String> got = drain(c, 3, 60000).stream().map(r -> s(r.value())).sorted().collect(Collectors.toList());
                return "before=" + before + " after=" + after + " read=" + String.join(",", got);
            }
        }
    }

    static String groupListFilters() throws Exception {
        String stable = topic + "-gs", empty = topic + "-ge";
        try (KafkaProducer<byte[], byte[]> p = producer()) {
            send(p, topic, 0, "lf");
        }
        Properties ep = new Properties();
        ep.put("group.id", empty);
        try (KafkaConsumer<byte[], byte[]> c = consumer(ep)) {
            c.subscribe(List.of(topic));
            drain(c, 1, 60000);
            c.commitSync();
        }
        BgMember m = new BgMember(stable, new Properties());
        try (Admin a = admin()) {
            long deadline = System.currentTimeMillis() + 60000;
            while (m.received().isEmpty() && System.currentTimeMillis() < deadline) Thread.sleep(200);
            Set<String> emptyNow = Set.of();
            for (int i = 0; i < 40; i++) {
                emptyNow = a.listGroups(new ListGroupsOptions().inGroupStates(Set.of(GroupState.EMPTY))).all().get()
                        .stream().map(GroupListing::groupId).collect(Collectors.toSet());
                if (emptyNow.contains(empty)) break;
                Thread.sleep(250);
            }
            Set<String> stableNow = a.listGroups(new ListGroupsOptions().inGroupStates(Set.of(GroupState.STABLE))).all().get()
                    .stream().map(GroupListing::groupId).collect(Collectors.toSet());
            Collection<GroupListing> all = a.listGroups().all().get();
            GroupListing one = all.stream().filter(x -> x.groupId().equals(stable)).findFirst().get();
            return "stable_filter=[stable=" + stableNow.contains(stable) + ",empty=" + stableNow.contains(empty)
                    + "] empty_filter=[stable=" + emptyNow.contains(stable) + ",empty=" + emptyNow.contains(empty)
                    + "] type=" + one.type().orElse(null) + " state=" + one.groupState().orElse(null);
        } finally {
            m.close();
        }
    }

    static String txnTimeout() throws Exception {
        Properties pp = new Properties();
        pp.put("transactional.id", "java-timeout-" + topic);
        pp.put("transaction.timeout.ms", "5000");
        Properties committed = new Properties();
        committed.put("isolation.level", "read_committed");
        try (KafkaProducer<byte[], byte[]> t = producer(pp)) {
            t.initTransactions();
            t.beginTransaction();
            send(t, topic, 0, "abandoned");
            try (KafkaConsumer<byte[], byte[]> c = consumer(committed)) {
                c.assign(List.of(tp(0)));
                long held = c.endOffsets(List.of(tp(0))).get(tp(0));
                long end = held;
                long deadline = System.currentTimeMillis() + 90000;
                while (end == held && System.currentTimeMillis() < deadline) {
                    Thread.sleep(500);
                    end = c.endOffsets(List.of(tp(0))).get(tp(0));
                }
                String commit = attempt(t::commitTransaction);
                return "held_at=" + held + " released_at=" + end + " commit_after_expiry=" + commit
                        + " read_committed=[" + state(committed) + "]";
            }
        }
    }

    static String compactedNullKey() throws Exception {
        String t = topic + "-c";
        try (Admin a = admin()) {
            createTopic(a, t, 1, Map.of("cleanup.policy", "compact"));
            try (KafkaProducer<byte[], byte[]> p = producer()) {
                String nullKey = "none";
                try {
                    send(p, new ProducerRecord<>(t, 0, null, b("v")));
                } catch (Exception e) {
                    nullKey = err(e);
                }
                long keyed = send(p, new ProducerRecord<>(t, 0, b("k"), b("v"))).offset();
                long tomb = send(p, new ProducerRecord<>(t, 0, b("k"), (byte[]) null)).offset();
                return "null_key=" + nullKey + " keyed_offset=" + keyed + " tombstone_offset=" + tomb;
            }
        }
    }

    static String metadata() throws Exception {
        try (Admin a = admin(); KafkaConsumer<byte[], byte[]> c = consumer(new Properties())) {
            int partitions = c.partitionsFor(topic).size();
            DescribeClusterResult dc = a.describeCluster();
            return "partitions=" + partitions + " brokers=" + dc.nodes().get().size()
                    + " controller=" + (dc.controller().get() != null)
                    + " listed=" + a.listTopics().names().get().contains(topic)
                    + " all_topics=" + c.listTopics().containsKey(topic);
        }
    }

    static String unknownTopic() throws Exception {
        String missing = topic + "-missing";
        Properties cp = new Properties();
        cp.put("default.api.timeout.ms", "10000");
        try (KafkaConsumer<byte[], byte[]> c = consumer(cp); Admin a = admin()) {
            String partitions;
            try {
                partitions = String.valueOf(c.partitionsFor(missing).size());
            } catch (Exception e) {
                partitions = err(e);
            }
            return "partitions=" + partitions + " describe=" + attempt(
                    () -> a.describeTopics(List.of(missing)).allTopicNames().get());
        }
    }
}
