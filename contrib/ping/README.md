# HTTP ping

HTTP ping agent monitors a set of HTTP services configured via rainer. 
It checks each service once a minute (configurable), and emits the status codes and latencies of these pings as metrics. 
The default is to GET each uri, and sent an alert if it fails to return 200 for more than 3 (configurable) minutes in a row. 
The behavior can be customized.

The alert mail subjects have the service name followed by "Service failure".

## Configuration URL
The agent is configurable via rainer at `<wagon.health.domain>/config/http-ping`.

## HTTP probe configuration

```yaml
  http/probe/name: # HTTP probe name
    uri: # Resource identifier (http(s) and disco are supported)
    discoConfig: # Zookeeper configuration (optional)
      zkConnect: # ZK connection string 
      discoPath: # ZK discovery path
    postData: # Post string data or JS function (optional, GET request is sent if omitted)
    pagerKey: # Pagerduty service key to assign pagers to (optional, no pagers are triggered if omitted)
    predicate: # JS predicate to process the response (optional, the default checks if response status is 200)
    pingPeriod: # The period of checks (optional, the default is PT1M)
    failCount: # How many pings should fail in a row before reporting a failure (optional, the default is 3)
    resolveCount: # How many pings should succeed in a row before resolving a failure (optional, the default is 2)
    ignoreFailedFetch: # Ignore the failures due to timeout (optional, the default is false),
```

## POST Data

### String

The simplest way to specify POST data is to use a string.

```yaml
serviceName:
  uri: http://example.com
  postData: 'hello world'
```

Or:

```yaml
serviceName:
  uri: http://example.com
  postData:
    type: string
    value: 'hello world'
```

### JavaScript

You can also specify POST data through a JavaScript function. This is useful if you need to include randomness or
something related to the current timestamp in your POST data.

```yaml
serviceName:
  uri: http://example.com
  postData:
    type: js
    function: 'function() { return new Date().toISOString(); }'
```

## HTTP Predicates

### JavaScript

The JavaScript predicate is a function that gets an object on which you can call body() and statusCode() and
inspect those things. For example, this predicate will expect HTTP 200 with a JSON payload that contains the key "hey" with
value "what":

```yaml
serviceName:
  uri:       http://example.com/
  predicate: 'function(response) { try { return JSON.parse(response.body()).hey == "what" && response.statusCode() == 200; } catch(e) { return false; } }'
```

This is an equivalent way of writing the same thing:

```yaml
serviceName:
  uri: http://example.com
  predicate:
    type: js
    function: 'function(response) { try { return JSON.parse(response.body()).hey == "what" && response.statusCode() == 200; } catch(e) { return false; } }'
```

Some bonus functionality is available that is not part of the standard scripting engines:

- `DateTime.parse(string)` (`parse(long)`) can be called to parse ISO8601 dates using Joda Time. It returns the number of milliseconds
  since the epoch, or throws an exception if the string is not a valid datetime.
- `PingResult.create(boolean, object)` can be called to instantiate a predicate result with bool status and a map that
  would be added to an alert and a pager.
- `ExtJs.load(string)` load an external JS lib from a file.
- `Log` with `trace`, `debug`, `info`, `warn`, `error` and `wtf` methods to write messages directly to the health 
  wagon's log file.
  
```yaml
router/prod:
  zkConnect: 'zk.example.com'
  discoPath: '/prod'
  uri:       'disco://druid:prod:router/druid/v2/'
  postData:
    type: js
    function: |
      function() {
        query_id = "prod_router_" + new Date().toISOString()
        query = { queryType: "timeBoundary", bound: "maxTime", dataSource: "vungle_adserver", context: { populateCache: false, useCache: false, queryId: query_id, metaData: { email: "health", dash: "health", page: "health", type: "health"}}}
        Log.error( "query: " + JSON.stringify(query))
        return JSON.stringify(query)
      }
  predicate:
    type: js
    function: |
      function(response) {
        try {
          retval = response.statusCode() == 200 && JSON.parse(response.body())[0].result > "2000"
          return PingResult.create(retval, { "dash": "<link to the dash>" });
        } catch(e) { }
        return false;
      }
```

### Gmail RSS

The Gmail RSS predicate expects to be pointed at a [Gmail inbox feed](https://developers.google.com/gmail/gmail_inbox_feed)
and is designed to look for recent emails matching a particular sender and subject. For example, this predicate will
check that an email from alerts@example.com with subject containing "HealthCheck" has arrived sometime
in the past two hours:

```yaml
serviceName:
  uri: 'https://user%40example.com:password@mail.google.com/mail/feed/atom'
  predicate:
    type:          gmailRss
    senderAddress: alerts@example.com
    titleRegex:    '.*HealthCheck.*'
    threshold:     PT2H
```