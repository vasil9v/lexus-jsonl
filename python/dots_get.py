def test_dots_get():
  r = dots_get({ "test": { "test": "example" } }, 'test.test')
  assert r == "example", r
  r = dots_keys('this["is"].my[1].example')
  assert r == [ 'this', 'is', 'my', 1, 'example' ], r
  r = dots_keys('this.is.my[1].example')
  assert r == [ 'this', 'is', 'my', 1, 'example' ], r


def unquote(s):
  if len(s) > 1:
    if s[0] in {"'", '"'}:
      if s[0] == s[-1]:
        return s[1:-1]
  return s


def extract_array_indexes(l):
  r = []
  for i in l:
    if "[" in i:
      idx = i.find("[")
      edx = i.find("]", idx)
      r.append(i[0:idx])
      value = i[idx+1:edx]
      if value.isnumeric():
        value = int(value)
      else:
        value = unquote(value)
      r.append(value)
    else:
      r.append(i)
  return r


def dots_keys(field):
  segments = field.split(".")
  segments = extract_array_indexes(segments)
  return segments


def dots_get(obj, field):
  segments = dots_keys(field)
  while obj and len(segments) > 0:
    obj = obj.get(segments[0])
    segments = segments[1:]
  return obj
