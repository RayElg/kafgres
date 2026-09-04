# Compressed batches from real clients

Captured off the wire, not compressed by the test that reads them.

That distinction is the whole reason these files exist. Compressing a payload in a test and
decompressing it again proves that one library round-trips with itself; it cannot prove the
broker can read what a client actually sends. Producing the same 200 records through
librdkafka and through the Java client is what showed the two **disagree about snappy**:

    librdkafka  ba 9c 01 …              a bare snappy block, varint uncompressed length
    Java        82 53 4e 41 50 50 59 00  \x82SNAPPY\0 xerial framing

Same codec number on the wire, two framings, decided by which client produced the batch. A
decompressor handling one silently fails for half the client population, and the failure
surfaces as compaction quietly not working on those topics rather than as an error.

## How they were made

    kcat -b … -t codecs -p N -P -z <codec> -X linger.ms=500 -X batch.num.messages=1000
    kafka-console-producer.sh --compression-codec snappy --batch-size 1000000

then read back out of `kafgres_log` and base64-decoded.

Note `-z` alone does not compress: librdkafka skips compression for a single small message,
so an earlier check that "all four codecs round-trip" was really testing four uncompressed
batches. The batching flags are what make the fixture a fixture.

| File | Producer | Framing |
|---|---|---|
| `librdkafka_gzip.batch` | kcat | standard gzip (`1f 8b`) |
| `librdkafka_snappy_raw.batch` | kcat | bare snappy block |
| `librdkafka_lz4.batch` | kcat | LZ4 frame (`04 22 4d 18`) |
| `librdkafka_zstd.batch` | kcat | zstd frame (`28 b5 2f fd`) |
| `java_snappy_xerial.batch` | `kafka-console-producer.sh` | xerial-framed snappy |

Each holds 200 records with null keys. Null is deliberate: it is what these producers send
by default, and a decoder that invents a key would pass a value-only check.
