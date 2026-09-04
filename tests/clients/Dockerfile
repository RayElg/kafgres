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

FROM debian:bookworm-slim

RUN apt-get update -qq \
 && apt-get install -y -qq --no-install-recommends \
      kcat \
      ca-certificates \
      python3 \
      python3-pip \
 && rm -rf /var/lib/apt/lists/*

# kafka-python is a fourth implementation, and different again: pure Python, its own version negotiation, and common in data engineering.
# `kafka-python-ng` is the maintained fork; the original has not shipped since 2020 and
# does not know about anything past Kafka 2.6.
RUN pip3 install --break-system-packages --no-cache-dir "kafka-python-ng==2.2.3"

COPY --from=sarama /out/sarama-conformance /usr/local/bin/sarama-conformance
COPY python/scenarios.py /usr/local/bin/kafka-python-conformance
RUN chmod +x /usr/local/bin/kafka-python-conformance

ENTRYPOINT []
