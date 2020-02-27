# Stateful Checks Agent

Stateful checks are designed for the cases when checks should be automatically generated
or deleted. Stateful check logic should be fully described in a JS-based function returning 
an array of all the sub-checks done internally. So it's mandatory to catch any exception
during the check and fail a specific sub-check if needed. 

For the sake of running fully synchronous check, a `HttpClient` is introduced into the JS engine.
Example:
```javascript
var response = HttpClient.get("http://localhost:9079/get");
response.content();
response.status();
response.reason();
```
```javascript
var response = HttpClient.post("http://localhost:9079/post", {"msg":"post"});
```
`HttpClient` has two methods: `get` and `post`, and returns the result synchronously.

## Configuration URL
The agent is configurable via rainer at `<wagon.health.domain>/config/stateful-check`.

## Stateful Check Configuration
```yaml
checks:
  stateful/check/name: # Stateful check name
    check: # JS-based function returning CheckResults
    checkPeriod: # The period of checks (optional, the default is PT1M)
    resolveCount: # How many pings should succeed in a row before resolving a failure (optional, the default is 2)
    failCount: # How many pings should fail in a row before reporting a failure (optional, the default is 3)
    pagerKey: # Pagerduty service key to assign pagers to (optional, no pagers are triggered if omitted)
```
