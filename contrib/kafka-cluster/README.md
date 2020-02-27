# Kafka cluster probes

The agent monitors multiple aspects of Kafka clusters:
- The cluster integrity (missing brokers, incorrect broker ids) 
- Offline partitions
- Under-replicated partitions
- Unclean leader election

## Configuration URL
The agent is configurable via rainer at `<wagon.health.domain>/config/kafka-cluster`.

## HTTP probe configuration

```yaml
  zkConnect: # Zookeeper URL
  zkTimeout: # Zookeeper session timeout (optional, the default is 30000 millis)
  brokersCount: # The number of Kafka brokers
  pagerKey: # PagerDuty service key (optional, it empty then no pagers are triggered)
  jmxPort: # JMX port to connect to to get the cluster's metrics (optional)
  brokerStartId: # The starting number of broker ids (optional, the default is 1)
  alert: # Properties related to triggering alerts and pagers (optional) 
    UnderReplicatedPartitionsPercentage: # The percentage of under-replicated partitions
    UnderReplicatedPeriod: # The period while a particular partition is under-replicated
    OfflinePartitions: # If true then alerts and pagers related to offline partitions are triggered
    UncleanLeaderElection: # If true then alerts and pagers related to unclean leader election incidents are triggered
```

By default only metrics are sent. Alert and pager are triggered if corresponding properties
are specified