from datetime import datetime


def date_bucket_of(s, interval):
  DATE_STRING_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
  tasks = [
    lambda t: t.replace(month = 1),
    lambda t: t.replace(day = 1),
    lambda t: t.replace(hour = 0),
    lambda t: t.replace(minute = 0),
    lambda t: t.replace(second = 0),
    lambda t: t.replace(microsecond = 0),
  ]
  interval_to_pointer = {
    "year": 0,
    "years": 0,
    "month": 1,
    "months": 1,
    "day": 2,
    "days": 2,
    "hour": 3,
    "hours": 3,
    "minute": 4,
    "minutes": 4,
    "second": 5,
    "seconds": 5,
  }
  ptr = interval_to_pointer.get(interval, len(tasks))
  date_bucket = None
  if type(s) == int:
      date_bucket = datetime.fromtimestamp(int(s / 1000))
  else:
      date_bucket = datetime.strptime(s, DATE_STRING_FORMAT)
  while ptr < len(tasks):
    f = tasks[ptr]
    date_bucket = f(date_bucket)
    ptr += 1
  
  strdate = date_bucket.strftime(DATE_STRING_FORMAT)
  return strdate.replace("000Z", "Z")


def test_date_bucket_of():
  s = "2025-04-10T18:48:49.578000Z"
  assert date_bucket_of(s, "year") == "2025-01-01T00:00:00.000Z"
  assert date_bucket_of(s, "month") == "2025-04-01T00:00:00.000Z"
  assert date_bucket_of(s, "day") == "2025-04-10T00:00:00.000Z"
  assert date_bucket_of(s, "hour") == "2025-04-10T18:00:00.000Z"
  assert date_bucket_of(s, "minute") == "2025-04-10T18:48:00.000Z"
  assert date_bucket_of(s, "second") == "2025-04-10T18:48:49.000Z"
