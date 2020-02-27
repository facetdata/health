## Kafka consumer probes

Health monitors a set of kafka consumers configured via rainer at http://disco/prod/v1/redirect/health:prod/config/kafka-consumer.
With kafka 7 the threshold measures a number of bytes, whereas with kafka 8 it measures a number of messages. Either
way, if a consumer is farther behind than the configured threshold, health will send an alert mail with the subject
"High latency for consumer".
