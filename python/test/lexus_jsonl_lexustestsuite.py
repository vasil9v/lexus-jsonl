import os
import json
from pathlib import Path
from lexus_jsonl import LexusEventStream


def numbers_agnostic_deep_equals(a, b):
  ta = type(a)
  tb = type(b)
  if ta == int: ta = float
  if tb == int: tb = float
  if ta != tb:
    return False
  if type(a) in {str, bool}:
    return a == b
  if type(a) in {int, float}:
    return float(a) == float(b)
  if type(a) == list:
    if len(a) != len(b):
      return False
    for i in range(len(a)):
      if not numbers_agnostic_deep_equals(a[i], b[i]):
        return False
    return True
  visited = {}
  for i in a:
    visited[i] = 1
    if i not in b:
      return False
    if not numbers_agnostic_deep_equals(a[i], b[i]):
      return False
  for i in b:
    if i not in visited:
      return False
    del visited[i]
  return len(visited) == 0


def load_test_suite(lexus_suite_path):
  items = []
  querypath = Path(lexus_suite_path) / "queries"
  files = os.listdir(querypath)
  for i in files:
    items.append(json.loads(open(querypath / i).read().strip()))
  return items


def load_test_records(lexus_suite_path):
  items = []
  recordspath = Path(lexus_suite_path) / "records"
  files = os.listdir(recordspath)
  for i in files:
    lines = open(recordspath / i).read().strip().split("\n")
    for line in lines:
        items.append(json.loads(line))
  return items


def test_suite():
  items = load_test_suite("../node_modules/lexus/suite")  # FIXME
  records = load_test_records("../node_modules/lexus/suite")  # FIXME
  for i in items:
    lexusEventStream = LexusEventStream(json.dumps(i["query"][0]))
    for r in records:
      lexusEventStream.process_event(r)
    lexusEventStream.finalize()
    i["result"][0]["version"] = '0.3' # FIXME
    assert numbers_agnostic_deep_equals(i["result"][0], lexusEventStream.lexus_result[0]), "\n{}\n{}\n".format(i["result"][0], lexusEventStream.lexus_result[0])
