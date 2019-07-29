### Lexus on jsonl Streams

This project is a JavaScript implementation of the [Lexus query langauge](https://github.com/appcelerator/lexus) on streams of json documents. The stream can be anything that can pipe to `stdout`.

### Test Data Set

Consider the following example file of newline separated list of JSON documents [jsonl](http://jsonlines.org/) representing a simplified log from a web server API.

```
$ cat > events.json << DONE
> {"id": "123", "ts": 1519799205709, "event": "api.access", "platform": "linux", "url": "/api/v1/users/login", "millis": 1000}
> {"id": "124", "ts": 1519799210760, "event": "api.access", "platform": "linux", "url": "/api/v1/users/logout", "millis": 800}
> {"id": "125", "ts": 1519799215430, "event": "api.access", "platform": "windows", "url": "/api/v1/users/login", "millis": 259}
> {"id": "126", "ts": 1519799221999, "event": "api.access", "platform": "windows", "url": "/api/v1/users/logout", "millis": 199}
> {"id": "127", "ts": 1519799226407, "event": "api.access", "platform": "linux", "url": "/api/v1/users/login", "millis": 750}
> {"id": "128", "ts": 1519799231479, "event": "api.access", "platform": "linux", "url": "/api/v1/users/logout", "millis": 323}
> {"id": "129", "ts": 1519799239583, "event": "api.access", "platform": "windows", "url": "/api/v1/users/login", "millis": 1100}
> {"id": "130", "ts": 1519799244271, "event": "api.access", "platform": "windows", "url": "/api/v1/users/logout", "millis": 340}
> DONE
$ wc -l events.json
       8 events.json
```
We'll use this as an example data stream to show expected results with some simple Lexus queries.
For more details on the Lexus syntax see [this document](https://github.com/appcelerator/lexus/blob/master/docs/getting-started.md).
The stdout of the lexus-jsonl.js script is redirected to `stdout` to show that this tool can be used as part of a command line pipeline. In each example below, this stdout is redirected to `/dev/null`.
The output results of the Lexus query are sent to `stderr`.
Note that the Lexus query itself is passed as the first argument to the script.

### Count

A simple count method should bring back the same result as the `wc -l` above:

```
$ cat events.json | node lexus-jsonl.js '[{"version": "0.2", "operation": {"method": "count"}}]' > /dev/null
[{"version":"0.2","result":8}]
```

### Group By

You can use the `groups` field to specify a field to have the results grouped-by:

```
$ cat events.json | node lexus-jsonl.js '[{"version": "0.2", "operation": {"method": "count"}, "groups": ["platform"]}]' > /dev/null
[{"version":"0.3","result":{"linux":4,"windows":4}}]
```

### Group By 2 Dimensions

Each `platform` and `url` combination has 2 documents:

```
$ cat events.json | node lexus-jsonl.js '[{"version": "0.2", "operation": {"method": "count"}, "groups": ["platform", "url"]}]' > /dev/null
[{"version":"0.2","result":{"linux":{"/api/v1/users/login":2,"/api/v1/users/logout":2},"windows":{"/api/v1/users/login":2,"/api/v1/users/logout":2}}}]
```

We'll use the same groups in each of the subsequent examples.

### Sum

```
$ cat events.json | node lexus-jsonl.js '[{"version": "0.2", "operation": {"method": "sum", "field": "millis"}, "groups": ["platform", "url"]}]' > /dev/null
[{"version":"0.2","result":{"linux":{"/api/v1/users/login":1750,"/api/v1/users/logout":1123},"windows":{"/api/v1/users/login":1359,"/api/v1/users/logout":539}}}]
```

### Avg

```
$ cat events.json | node lexus-jsonl.js '[{"version": "0.2", "operation": {"method": "avg", "field": "millis"}, "groups": ["platform", "url"]}]' > /dev/null
[{"version":"0.2","result":{"linux":{"/api/v1/users/login":875,"/api/v1/users/logout":561.5},"windows":{"/api/v1/users/login":679.5,"/api/v1/users/logout":269.5}}}]
```

### Min

```
$ cat events.json | node lexus-jsonl.js '[{"version": "0.2", "operation": {"method": "min", "field": "millis"}, "groups": ["platform", "url"]}]' > /dev/null
[{"version":"0.2","result":{"linux":{"/api/v1/users/login":750,"/api/v1/users/logout":323},"windows":{"/api/v1/users/login":259,"/api/v1/users/logout":199}}}]

```

### Max

```
$ cat events.json | node lexus-jsonl.js '[{"version": "0.2", "operation": {"method": "max", "field": "millis"}, "groups": ["platform", "url"]}]' > /dev/null
[{"version":"0.2","result":{"linux":{"/api/v1/users/login":1000,"/api/v1/users/logout":800},"windows":{"/api/v1/users/login":1100,"/api/v1/users/logout":340}}}]
```

### Run Tests

To run all the unit tests for lexus-jsonl simply do:

```
npm test
```
