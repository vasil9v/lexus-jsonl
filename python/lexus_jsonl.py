import sys
import json
from date_bucket import date_bucket_of
from dots_get import dots_get


LEXUS_VERSION = "0.3"
state = {
  "lines_read": None,
  "per_lines": None
}


def to_std_err(s):
  print(s, file=sys.stderr)


def inc(d, k, v):
  d[k] = d.get(k, 0) + v
  return d[k]


class LexusEventStream:

  def __init__(self, query):
    self.lexus_result = []
    self.lexus_query = json.loads(query)
    if type(self.lexus_query) != list:
      self.lexus_query = [self.lexus_query]

    for i in range(len(self.lexus_query)):
      self.lexus_query[i]["groups"] = self.lexus_query[i].get("groups", [])
      self.lexus_query[i]["operation"]["params"] = self.lexus_query[i]["operation"].get("params", {})
      if self.lexus_query[i]["operation"]["method"] == "find":
        self.lexus_query[i]["operation"]["params"]["limit"] = self.lexus_query[i]["operation"]["params"].get("limit", 10)
      if self.lexus_query[i]["operation"]["method"] == "unique":
        self.lexus_query[i]["groups"].append(self.lexus_query[i]["operation"]["field"])

      # normalize each groups item into extended format
      for g in range(len(self.lexus_query[i]["groups"])):
        if type(self.lexus_query[i]["groups"][g]) != dict:
          self.lexus_query[i]["groups"][g] = {
            "type": "string",
            "field": self.lexus_query[i]["groups"][g]
          }
      self.lexus_result.append({"version": LEXUS_VERSION})

  def filter(self, json, filter):
    """
    Walk through each filter operation.
    Return true if the document should be included in the query.
    Since the filter rejects documents that should not be included,
    then the first operation which is false should short circuit and
    return false immediately.
    """
    for op in filter:
      # TODO "$geo"
      if op == "$range":
        for field in filter[op]:
          value = float(dots_get(json, field))
          if "gte" in filter[op][field] and value < float(filter[op][field]["gte"]):
            return False
          if "lte" in filter[op][field] and value > float(filter[op][field]["lte"]):
            return False
          if "gt" in filter[op][field] and value <= float(filter[op][field]["gt"]):
            return False
          if "lt" in filter[op][field] and value >= float(filter[op][field]["lt"]):
            return False
      elif op == "$exists":
        for field in filter[op]:
          if ((field in json) != filter[op][field]):  # todo dots_get(json, field)
            return False
      elif op == "$match":
        for field in filter[op]:
          if (dots_get(json, field) != filter[op][field]):
            return False
        # TODO if array
      elif op == "$prefix":
        for field in filter[op]:
          if (not(dots_get(json, field)) or not dots_get(json, field).startswith(filter[op][field])):
            return False
        # TODO if array
      elif op == "$suffix":
        for field in filter[op]:
          if (not(dots_get(json, field)) or not dots_get(json, field).endswith(filter[op][field])):
            return False
        # TODO if array
      elif op == "$not":
        for sub in filter[op]:
          if (self.filter(json, sub)):
            return False
      elif op == "$or":
        atLeastOne = False
        for sub in filter[op]:
          if (self.filter(json, sub)):
            atLeastOne = True
        if not atLeastOne:
          return False
      elif op == "$and":
        for sub in filter[op]:
          if not self.filter(json, sub):
            return False
      else:
        to_std_err("filter not supported: {}".format(op))
    return True

  def finalize(self):
    for i in range(len(self.lexus_result)):
      self.lexus_result[i] = self.finalizeResult(self.lexus_query[i], self.lexus_result[i])

  def finalizeResult(self, query, jsonobj):
    if type(jsonobj) != dict:
      return jsonobj

    if "_lexusAvg" in jsonobj:
      return jsonobj["_lexusAvg"]["v"] / jsonobj["_lexusAvg"]["n"]

    if "_lexusUnique" in jsonobj:
      return len(jsonobj["_lexusUnique"])

    for i in jsonobj:
      jsonobj[i] = self.finalizeResult(query, jsonobj[i])

    return jsonobj

  def projection(self, params, jsonobj):
    if "include" not in params and "exclude" not in params:
      return jsonobj

    jsonobj2 = {}
    for field in params.get("exclude", []):
      del jsonobj[field]  # todo dots_get(jsonobj, field)

    if params.get("include"):
      for field in params["include"]:
        jsonobj2[field] = dots_get(jsonobj, field)
    else:
      jsonobj2 = jsonobj

    return jsonobj2

  def date_bucket_of(self, s, interval):
    return date_bucket_of(s, interval)

  def process_event(self, jsonobj):
    if not self.lexus_query:
      return

    for i in range(len(self.lexus_query)):
      if self.filter(jsonobj, self.lexus_query[i].get("filters", {})):
        leaf = self.lexus_result[i]
        key = "result"
        for j in range(len(self.lexus_query[i]["groups"])):
          if key not in leaf:
            leaf[key] = {}
          leaf = leaf[key]
          key = dots_get(jsonobj, self.lexus_query[i]["groups"][j]["field"])
          if (self.lexus_query[i]["groups"][j]["type"] == "date"):
            key = self.date_bucket_of(key, self.lexus_query[i]["groups"][j]["params"]["interval"])
          if (self.lexus_query[i]["groups"][j]["type"] == "numeric"):
            interval = float(self.lexus_query[i]["groups"][j]["params"]["interval"])
            key = round(float(key) / interval) * interval

        mth = self.lexus_query[i]["operation"]["method"]
        value = None
        if "field" in self.lexus_query[i]["operation"]:
          value = dots_get(jsonobj, self.lexus_query[i]["operation"]["field"])
        if mth == "min":
          if value and ((key not in leaf) or float(value) < leaf[key]):
            leaf[key] = float(value)
        elif mth == "max":
          if value and ((key not in leaf) or float(value) > leaf[key]):
            leaf[key] = float(value)
        elif mth == "sum":
          if value:
            leaf[key] = leaf.get(key, 0)
            leaf[key] += float(value)
        elif mth == "avg":
          if value:
            leaf[key] = leaf.get(key, {"_lexusAvg": {"n": 0, "v": 0}})
            leaf[key]["_lexusAvg"]["n"] += 1
            leaf[key]["_lexusAvg"]["v"] += float(value)
        elif mth == "count":
          inc(leaf, key, 1)
        elif mth == "unique":
          if "_lexusUnique" not in leaf:
            leaf["_lexusUnique"] = {}
          inc(leaf["_lexusUnique"], key, 1)
        elif mth == "find":
          leaf[key] = leaf.get(key, [])
          if (len(leaf[key]) < self.lexus_query[i]["operation"]["params"]["limit"]):
            leaf[key].append(self.projection(self.lexus_query[i]["operation"]["params"], jsonobj))
          """
          TODO
          "limit": {
            "type": "number",
            "description": "A limit on the number of returned documents"
          "offset": {
            "type": "number",
            "description": "A number of documents to skip before finding documents to return"
          "sort": {
            "type": "object",
            "additionalProperties": {
              "type": "number",
              "description": "A sort index to apply to the designated field",
              "enum": [ -1, 1 ]
          """
        else:
          to_std_err("method not supported: " + self.lexus_query[i]["operation"]["method"])


def do_start():
  lexusEventStream = LexusEventStream(sys.argv[1])
  state["lines_read"] = 0
  return lexusEventStream


def do_final(lexusEventStream):
  lexusEventStream.finalize()
  to_std_err(json.dumps(lexusEventStream.lexus_result))


if __name__ == "__main__":
  if len(sys.argv) > 2:
    state["per_lines"] = int(sys.argv[2])

  lexusEventStream = do_start()

  for line in sys.stdin:
    lexusEventStream.process_event(json.loads(line.strip()))
    state["lines_read"] += 1
    if state["per_lines"] and (state["lines_read"] > state["per_lines"]):
      do_final(lexusEventStream)
      lexusEventStream = do_start()
    print(line)

  do_final(lexusEventStream)
