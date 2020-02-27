# Druid probes

Health monitors segment load status and maxTime of all druid datasources. It emits these values as metrics and 
also emits alerts in at least two cases:

- "Data not loading": Load status above zero for too long
- "Stale data": maxTime too old by more than a per-datasource threshold

## Configuration URL
The agent is configurable via rainer at `<wagon.health.domain>/config/druid`.

## HTTP probe configuration

```yaml
  zkConnect: # Zookeeper URL
  discoPath: # ZK service discovery path
  broker: # Druid broker name
  coordinator: # Druid coordinator name
  alert: # Alert threshold config (optional)
    threshold: # Threshold period
    pagerKey: # PagerDuty service key (optional), if empty then only an alert is sent
```

By default only metrics are sent. Alert and pager are triggered if corresponding properties
are specified