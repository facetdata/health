# Mesos probes

## Configurations

These are the details about the configuration options available in this module

## Environments

Each environment can have a unique  set of configurations outlined below.

## Env Configurations

These configuration options are available:

|Option|Description|Default|
|------|-----------|-------|
|`zkMaterPath`|The zookeeper path the Mesos Masters use|_NONE_(required)|
|`zkEnsemble`|The fallback zookeeper connection string|The default curator zk connection string for health|
|`zkSessionTimeout`|The zookeeper session timeout in ms|`60000`|
|`zkConnectionTimeout`|The zookeeper connection timeout in ms|`1000`|
|`zkMaxRetries`|The zookeeper exponential backoff maximum retry limit |`10`|
|`zkBaseSleep`|The zookeeper exponential backoff base sleep in ms|`1000`|
|`exhibitorURI`|The URI for the exhibitor list in the form of `http://some.host:8080/exhibitor/v1/cluster/list`. If not set will use `zkEnsemble` instead|NONE|
|`exhibitorMaxRetries`|The exhibitor exponential backoff retry limit|`10`|
|`exhibitorBaseSleep`|The exhibitor exponential backoff base sleep in ms|`100`|
|`exhibitorPollingMs`|The exhibitor polling period, controls how long until the *first* polling, as well as how long between pollings in ms|`1000`|
