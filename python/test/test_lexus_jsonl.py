import json
from lexus_jsonl import LexusEventStream

events = [
  {"id": "123", "time": 1522096142033, "event": "ti.start", "temp": 44, "platform": "android", "deploytype": "production", "data": {"comp": "acme", "level": "gold"}},
  {"id": "124", "time": 1522096142034, "event": "ti.end", "temp": 45, "platform": "android", "deploytype": "production", "data": {"comp": "acme", "level": "silver"}},
  {"id": "125", "time": 1522096142035, "event": "ti.start", "temp": 46, "platform": "ios", "deploytype": "production", "data": {"comp": "newco", "level": "gold"}},
  {"id": "126", "time": 1522096142036, "event": "ti.end", "temp": 47, "platform": "ios", "deploytype": "production", "data": {"comp": "newco", "level": "silver"}},
  {"id": "127", "time": 1522096142037, "event": "ti.start", "temp": 46, "platform": "android", "deploytype": "development", "data": {"comp": "newco", "level": "silver"}},
  {"id": "128", "time": 1522096142038, "event": "ti.end", "temp": 45, "platform": "android", "deploytype": "development", "data": {"comp": "acme", "level": "silver"}},
  {"id": "129", "time": 1522096142039, "event": "ti.start", "temp": 44, "platform": "ios", "deploytype": "development", "data": {"comp": "acme", "level": "silver"}},
  {"id": "130", "time": 1522188550240, "event": "ti.end", "temp": 43, "platform": "ios", "deploytype": "development", "foobar": "baz", "data": {"comp": "acme", "level": "silver"}}
]
queries = [
  [{"version": "0.3", "operation": {"method": "count"}}, {"version": "0.3", "operation": {"method": "count"}, "groups": ["platform"]}],
  [{"version": "0.3", "operation": {"method": "count"}, "groups": ["platform"]}],
  [{"version": "0.3", "operation": {"method": "count"}, "groups": ["platform", "deploytype"]}],
  [{"version": "0.3", "operation": {"method": "count"}, "filters": {"$match": {"event": "ti.start"}}, "groups": ["platform", "deploytype"]}],
  [{"version": "0.3", "operation": {"method": "count"}, "filters": {"$range": {"time": {"gte": 1522096142034, "lte": 1522096142036}}}}],
  [{"version": "0.3", "operation": {"method": "count"}, "filters": {"$range": {"time": {"gt": 1522096142034, "lte": 1522096142036}}}}],
  [{"version": "0.3", "operation": {"method": "count"}, "filters": {"$prefix": {"event": "ti.en"}}}],
  [{"version": "0.3", "operation": {"method": "count"}, "filters": {"$not": [{"$match": {"id": "127"}}]}}],
  [{"version": "0.3", "operation": {"method": "count"}, "filters": {"$or": [{"$match": {"id": "127"}}, {"$match": {"id": "129"}}]}}],
  [{"version": "0.3", "operation": {"method": "count"}, "filters": {"$and": [{"$match": {"id": "127"}}, {"$match": {"time": 1522096142037}}]}}],
  [{"version": "0.3", "operation": {"method": "count"}, "filters": {"$suffix": {"event": "end"}}}],
  [{"version": "0.3", "operation": {"method": "count"}, "groups": ["event"]}],
  [{"version": "0.3", "operation": {"method": "unique", "field": "event"}}],
  [{"version": "0.3", "operation": {"method": "find", "params": {"limit": 2, "include": ["id", "time", "event"]}}}],
  [{"version": "0.3", "operation": {"method": "find", "params": {"limit": 2, "include": ["id", "time", "event", "data.comp"]}}}],
  # [{"version": "0.3", "operation": {"method": "find", "params": {"limit": 2, "offset": 1, "include": ["id", "time", "event"]}}}],
  [{"version": "0.3", "operation": {"method": "count"}, "filters": {"$exists": {"foobar": True}}}],
  [{"version": "0.3", "operation": {"method": "count"}}],
  [{"version": "0.3", "operation": {"method": "find", "params": {"limit": 2, "include": ["id", "time", "event"]}}, "groups": ["event"]}],
  [{"version": "0.3", "operation": {"method": "sum", "field": "temp"}}],
  [{"version": "0.3", "operation": {"method": "avg", "field": "temp"}}],
  [{"version": "0.3", "operation": {"method": "min", "field": "temp"}}],
  [{"version": "0.3", "operation": {"method": "max", "field": "temp"}}],
  [{"version": "0.3", "operation": {"method": "count"}, "groups": ["event"]}],
  [{"version": "0.3", "operation": {"method": "count"}, "groups": ["data.comp"]}],
  [{"version": "0.3", "operation": {"method": "count"}, "groups": ["data.comp", "data.level"]}],
  [{"version": "0.3", "operation": {"method": "count"}, "groups": [{"type": "date", "field": "time", "params": {"interval": "day"}}]}],
  [{"version": "0.3", "operation": {"method": "count"}, "groups": [{"type": "numeric", "field": "temp", "params": {"interval": 3}}]}]
]
expected = [
  [{"version":"0.3", "result":8}, {"version":"0.3", "result":{"android":4, "ios":4}}],
  [{"version":"0.3", "result":{"android":4, "ios":4}}],
  [{"version":"0.3", "result":{"android":{"production":2, "development":2}, "ios":{"production":2, "development":2}}}],
  [{"version":"0.3", "result":{"android":{"production":1, "development":1}, "ios":{"production":1, "development":1}}}],
  [{"version":"0.3", "result":3}],
  [{"version":"0.3", "result":2}],
  [{"version":"0.3", "result":4}],
  [{"version":"0.3", "result":7}],
  [{"version":"0.3", "result":2}],
  [{"version":"0.3", "result":1}],
  [{"version":"0.3", "result":4}],
  [{"version":"0.3", "result": {"ti.start": 4, "ti.end": 4}}],
  [{"version":"0.3", "result":2}],
  [{"version":"0.3", "result": [{"id":"123", "time":1522096142033, "event":"ti.start"}, {"id":"124", "time":1522096142034, "event":"ti.end"}]}],
  [{"version":"0.3", "result": [{"id":"123", "time":1522096142033, "event":"ti.start", "data.comp": "acme"}, {"id":"124", "time":1522096142034, "event":"ti.end", "data.comp": "acme"}]}],
  # [{"version":"0.3", "result": [{"id":"124", "time":1522096142033, "event":"ti.start"}, {"id":"125", "time":1522096142034, "event":"ti.end"}]}],
  [{"version":"0.3", "result":1}],
  [{"version":"0.3", "result":8}],
  [{"version":"0.3", "result": {"ti.start":[{"id":"123", "time":1522096142033, "event":"ti.start"}, {"id":"125", "time":1522096142035, "event":"ti.start"}], "ti.end":[{"id":"124", "time":1522096142034, "event":"ti.end"}, {"id":"126", "time":1522096142036, "event":"ti.end"}]}}],
  [{"version":"0.3", "result":360.0}],
  [{"version":"0.3", "result":45.0}],
  [{"version":"0.3", "result":43.0}],
  [{"version":"0.3", "result":47.0}],
  [{"version":"0.3", "result": {"ti.start": 4, "ti.end": 4}}],
  [{"version":"0.3", "result": {"acme": 5, "newco": 3}}],
  [{"version":"0.3", "result": {"acme": {"gold": 1, "silver": 4}, "newco": {"gold": 1, "silver": 2}}}],
  [{"version":"0.3", "result": {"2018-03-26T00:00:00.000Z": 7, "2018-03-27T00:00:00.000Z": 1}}],
  [{"version":"0.3", "result": {42.0: 1, 45.0: 6, 48.0: 1}}]
]


def deep_equals(a, b):
  return json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)


def test_lexus_test_suite():
  assert deep_equals({"one": 1, "foo": "bar"}, {"foo": "bar", "one": 1})
  for item in zip(queries, expected):
      q = item[0]
      e = item[1]

      lexusEventStream = LexusEventStream(json.dumps(q))
      for i in events:
        lexusEventStream.process_event(i)
      lexusEventStream.finalize()
      # print(json.dumps(lexusEventStream.lexus_result))
      assert deep_equals(e, lexusEventStream.lexus_result)

