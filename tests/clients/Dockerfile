# Client tooling for integration and conformance tests.
#
# Built separately from the broker image so a broker rebuild does not re-pull client
# packages, and so the same image serves the whole client matrix.
#
# `kcat` is the binary; `kafkacat` is a transitional package name. Debian bookworm
# carries kcat 1.7.1 against librdkafka 2.x, and unlike edenhill/kcat on Docker Hub it
# is available for arm64.
# Sarama is in the matrix because it shares no code with librdkafka or the Java client,
# so it catches assumptions those two happen to make together — and it negotiates
# different API versions, which is where over-advertising shows up first.
FROM golang:1.22-bookworm AS sarama
WORKDIR /src
COPY sarama/ ./
RUN go mod tidy && CGO_ENABLED=0 go build -o /out/sarama-conformance .

# franz-go uses the highest version of every API both sides advertise.
FROM golang:1.22-bookworm AS franz
WORKDIR /src
COPY franz/ ./
RUN go mod tidy && CGO_ENABLED=0 go build -o /out/franz-conformance .

# Compiled against the reference broker's jars, so client and broker are one Kafka release.
FROM eclipse-temurin:17-jdk-jammy AS java
COPY --from=apache/kafka:4.3.1 /opt/kafka/libs /opt/kafka/libs
COPY java/Scenarios.java /src/Scenarios.java
RUN mkdir /out && javac -Xlint:none -cp "/opt/kafka/libs/*" -d /out /src/Scenarios.java

FROM debian:bookworm-slim

RUN apt-get update -qq \
 && apt-get install -y -qq --no-install-recommends \
      kcat \
      ca-certificates \
      python3 \
      python3-pip \
      openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/*

# kafka-python is a fourth implementation, and different again: pure Python, its own version negotiation, and common in data engineering.
# `kafka-python-ng` is the maintained fork; the original has not shipped since 2020 and
# does not know about anything past Kafka 2.6.
# kafka-python needs lz4 and zstandard for those codecs.
RUN pip3 install --break-system-packages --no-cache-dir "kafka-python-ng==2.2.3" "confluent-kafka==2.15.1" \
      lz4 zstandard

COPY --from=sarama /out/sarama-conformance /usr/local/bin/sarama-conformance
COPY python/scenarios.py /usr/local/bin/kafka-python-conformance
COPY librdkafka/scenarios.py /usr/local/bin/librdkafka-conformance
COPY --from=franz /out/franz-conformance /usr/local/bin/franz-conformance
COPY --from=java /opt/kafka/libs /opt/kafka/libs
COPY --from=java /out /opt/kafka/conformance
RUN printf '#!/bin/sh\nexec java -cp "/opt/kafka/conformance:/opt/kafka/libs/*" Scenarios "$@"\n' \
      > /usr/local/bin/java-conformance \
 && chmod +x /usr/local/bin/kafka-python-conformance /usr/local/bin/librdkafka-conformance \
      /usr/local/bin/java-conformance

ENTRYPOINT []
