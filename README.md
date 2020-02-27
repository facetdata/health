# Health
# <img alt="Health" src="https://cloud.githubusercontent.com/assets/1214075/3402294/3c6f1b88-fd5a-11e3-8548-a4371fae9c44.png" />

Health is a service that is responsible for various monitoring and alerting related activities. 
Many of its functions are configurable at runtime via [rainer](https://github.com/metamx/rainer) 
(the easiest way to use it is probably pyrainer).

Health consists of two type of components:
1. HQ (Headquarter)
2. Wagon (Agents)

## HQ 
HQ is responsible for sending alert notifications (emails and pagers) and monitoring health agents.

## Wagon
Wagon is a container for multiple agents. The list of active agents is configurable via application properties.
Agents are responsible for monitoring different services: Druid, Kafka, Mesos, Realtime processing pipelines.
```properties
health.druid.enable = true
health.kafka.cluster.enable = true
health.mesos.enable = false
```

## Alert emails

Health sends an email to predefined address for any properly formatted message sent to `alerts` (configurable) 
topic in Kafka. These can be posted to Intake endpoint. For format details see `com.metamx.emitter.service.AlertEvent`.

## Pagers

Pagers are notification of [PagerDuty](https://www.pagerduty.com/). PagerDuty is a service that opens an incident 
for any registered service (Druid, Kafka, Intake, etc) by service key and notifies in-call person by 
email, SMS, phone call, Slack, etc.

There are two types of PagerDuty request based on the incident related action:
1. Open an incident (triggers notifications)
2. Resolve an incident

Health notifies PagerDuty for any properly formatted message sent to the `pagers` (configurable) Kafka topic.
These can be posted to Intake endpoint. For format details see `com.metamx.health.agent.events.PagerTriggerData`
and `com.metamx.health.agent.events.PagerResolveData`.

## Health agents

### HTTP probes

See it [here](contrib/ping/README.md).

### Druid probes

See it [here](contrib/druid/README.md).

### Kafka cluster probes

See it [here](contrib/kafka-cluster/README.md).

### Kafka consumer probes

See it [here](contrib/kafka-consumer/README.md).

### Mesos probes

See it [here](contrib/mesos/README.md).

### Pipes probes

See it [here](contrib/pipes/README.md).

### Realtime processing probes

See it [here](contrib/realtime/README.md).

### Stateful checks

See it [here](contrib/stateful-check/README.md)

## Event processors

Event processors are part of Health HQ. The processors consume events from Kafka cluster.
+ Alerts
+ Pagers
+ Heartbeats
+ Statuses

### Alert Event Processor
Consumes events from `alerts` Kafka topic and notifies by email for alerts having 
predefined severity.

### Heartbeat Event Processor
Consumes events (generated by agents) from `heartbeats` Kafka topic. For those agents that haven't sent
heartbeats in predefined time interval alerts and pagers are generated.

When adding a new probe, be sure to update the list of known probes at 
```bash
rainer http://hq.health.com/config/heartbeat edit config
```

### Pager Event Processor
Consumes events from `pagers` Kafka topic. Depending on the event time the processor either raises
or resolves a pager.

### Status Event Processor
Consumes events from `statuses` Kafka topic and saves them into database.  

## HQ Rainer configs

### HQ Notify Inventory Rainer Config
```bash
rainer http://hq.health.com/config/notify-inventory edit config
```
```yaml
config:
  mailer:
    notifyFormat: "%s" # Subject
    smtpHost: smtp.example.com
    smtpPort: 2525
    smtpUser: user@example.com
    smtpPass: secret-password
    smtpSSLEnabled: false
    smtpTimeoutMs: 30000
    smtpTo: user@example.com
    smtpFrom: user@example.com
    smtpDebug: false
    quiet: false
  mailBuffer:
    backoffPeriods: PT3S PT5M PT1H
    maxBodySize: 100000 # Default 100000
    maxMails: 100 # Default 100
    mailCutoff: 10 # Default # 10
  pagerDuty:
    postUrl: https://events.pagerduty.com/create_event.json
    quiet: false
    mailOnly: false
```

### HQ Heartbeats Rainer Config
```bash
rainer http://hq.health.com/config/heartbeat edit config
```
```yaml
config:
  # Some agents (like kafka consumer offsets) can take a lot of time to complete one loop
  period: PT90M
  agents:
    - druid cluster
    - kafka cluster
    - kafka consumer offsets
    - mesos cluster
    - http pings
    - pipes backlog
    - realtime
```

