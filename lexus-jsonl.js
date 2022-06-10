const dots = require("dot-notes");
const LEXUS_VERSION = "0.3";
let linesRead;
let perlines = null;

function inc(d, k, v) {
  d[k] = d[k] || 0;
  d[k] += v;
  return d[k];
};

let lexusEventStream = {
  init: function (query) {
    this.lexusQuery = [];
    this.lexusResult = [];
    // console.log("query: " + query);
    this.lexusQuery = JSON.parse(query);
    if (!Array.isArray(this.lexusQuery)) {
      this.lexusQuery = [this.lexusQuery];
    }
    for (let i = 0; i < this.lexusQuery.length; i += 1) {
      this.lexusQuery[i].groups = this.lexusQuery[i].groups || [];
      this.lexusQuery[i].operation.params = this.lexusQuery[i].operation.params || {};
      if (this.lexusQuery[i].operation.method === "find") {
        this.lexusQuery[i].operation.params.limit = this.lexusQuery[i].operation.params.limit || 10;
      }
      if (this.lexusQuery[i].operation.method === "unique") {
        this.lexusQuery[i].groups.push(this.lexusQuery[i].operation.field);
      }

      // normalize each groups item into extended format
      for (let g = 0; g < this.lexusQuery[i].groups.length; g += 1) {
        if (typeof this.lexusQuery[i].groups[g] !== "object") {
          this.lexusQuery[i].groups[g] = {
            "type": "string",
            "field": this.lexusQuery[i].groups[g]
          };
        }
      }
      this.lexusResult.push({"version": LEXUS_VERSION});
    }
  },

  /*
   * Walk through each filter operation.
   * Return true if the document should be included in the query.
   * Since the filter rejects documents that should not be included,
   * then the first operation which is false should short circuit and
   * return false immediately.
   */
  filter: function (json, filter) {
    for (let op in filter) {
      switch (op) {
        // TODO "$geo"
        case "$range":
          for (let field in filter[op]) {
            if (Number(dots.get(json, field)) < Number(filter[op][field].gte) || Number(dots.get(json, field)) > Number(filter[op][field].lte)) {
              return false;
            }
            if (Number(dots.get(json, field)) <= Number(filter[op][field].gt) || Number(dots.get(json, field)) >= Number(filter[op][field].lt)) {
              return false;
            }
          }
          break;
        case "$exists":
          for (let field in filter[op]) {
            if ((field in json) !== filter[op][field]) { // todo dots.get(json, field)
              return false;
            }
          }
          break;
        case "$match":
          for (let field in filter[op]) {
            if (dots.get(json, field) !== filter[op][field]) {
              return false;
            }
          }
          // TODO if array
          break;
        case "$prefix":
          for (let field in filter[op]) {
            if (!(dots.get(json, field)) || !dots.get(json, field).startsWith(filter[op][field])) {
              return false;
            }
          }
          // TODO if array
          break;
        case "$suffix":
          for (let field in filter[op]) {
            if (!(dots.get(json, field)) || !dots.get(json, field).endsWith(filter[op][field])) {
              return false;
            }
          }
          // TODO if array
          break;
        case "$not":
          for (let sub of filter[op]) {
            if (this.filter(json, sub)) {
              return false;
            }
          }
          break;
        case "$or":
          let atLeastOne = false;
          for (let sub of filter[op]) {
            if (this.filter(json, sub)) {
              atLeastOne = true;
            }
          }
          if (!atLeastOne) {
            return false;
          }
          break;
        case "$and":
          for (let sub of filter[op]) {
            if (!this.filter(json, sub)) {
              return false;
            }
          }
          break;
        default:
          console.error("filter not supported: " + op);
          break;
      }
    }
    return true;
  },
  sortedDict: function (d) {
    let arr = [];
    for (let i in d) {
      arr.push([d[i], i]);
    }
    arr.sort((a, b) => {
      return (b[0] - a[0]);
    });
    return arr;
  },
  dictKeys: function (arr) {
    let keys = [];
    for (let item of arr) {
      keys.push(item[1]);
    }
    return keys;
  },
  dictObj: function (arr) {
    let obj = {};
    for (let item of arr) {
      obj[item[1]] = item[0];
    }
    return obj;
  },
  finalize: function () {
    for (let i in this.lexusResult) {
      this.lexusResult[i] = this.finalizeResult(this.lexusQuery[i], this.lexusResult[i]);
    }
  },
  finalizeResult: function (query, json) {
    if (typeof json !== "object") {
      return json;
    }
    if ("_lexusAvg" in json) {
      return json._lexusAvg.v / json._lexusAvg.n;
    }
    if ("_lexusCard" in json) {
      return Object.keys(json._lexusCard).length;
    }
    for (let i in json) {
      json[i] = this.finalizeResult(query, json[i]);
    }
    return json;
  },
  projection: function (params, json) {
    if (!(params.include || params.exclude)) {
      return json;
    }
    let json2 = {};
    if (params.exclude) {
      for (let field of params.exclude) {
        delete json[field]; // todo dots.get(json, field)
      }
    }
    if (params.include) {
      for (let field of params.include) {
        json2[field] = dots.get(json, field);
      }
    }
    return json2;
  },
  dateBucketOf: function (s, interval) {
    let d = new Date(s);
    let d2 = new Date(s);
    switch (interval) {
      case "year":
      case "years":
        d2.setMonth(0);
      case "month":
      case "months":
        d2.setDate(1);
      case "day":
      case "days":
        d2.setHours(0);
      case "hour":
      case "hours":
        d2.setMinutes(0);
      case "minute":
      case "minutes":
        d2.setSeconds(0);
      case "second":
      case "seconds":
        d2.setMilliseconds(0);
        return d2.toISOString();
        break;
      default:
        console.error("time interval not supported: " + interval);
        process.exit();
        break;
    }
    return null;
  },
  processEvent: function (json) {
    if (!this.lexusQuery) {
      return;
    }
    for (let i = 0; i < this.lexusQuery.length; i += 1) {
      if (this.filter(json, this.lexusQuery[i].filters || {})) {
        let leaf = this.lexusResult[i];
        let key = "result";
        for (let j = 0; j < this.lexusQuery[i].groups.length; j += 1) {
          if (!(key in leaf)) {
            leaf[key] = {};
          }
          leaf = leaf[key];
          key = dots.get(json, this.lexusQuery[i].groups[j].field);
          if (this.lexusQuery[i].groups[j].type === "date") {
            key = this.dateBucketOf(key, this.lexusQuery[i].groups[j].params.interval);
          }
          if (this.lexusQuery[i].groups[j].type === "numeric") {
            let interval = Number(this.lexusQuery[i].groups[j].params.interval);
            key = Math.round(Number(key) / interval) * interval;
          }
        }
        switch (this.lexusQuery[i].operation.method) {
          case "min":
            if (!leaf[key] || Number(dots.get(json, this.lexusQuery[i].operation.field)) < leaf[key]) {
              leaf[key] = Number(dots.get(json, this.lexusQuery[i].operation.field));
            }
            break;
          case "max":
            if (!leaf[key] || Number(dots.get(json, this.lexusQuery[i].operation.field)) > leaf[key]) {
              leaf[key] = Number(dots.get(json, this.lexusQuery[i].operation.field));
            }
            break;
          case "sum":
            leaf[key] = leaf[key] || 0;
            leaf[key] += Number(dots.get(json, this.lexusQuery[i].operation.field));
            break;
          case "avg":
            leaf[key] = leaf[key] || {"_lexusAvg": {"n": 0, "v": 0}};
            leaf[key]._lexusAvg.n += 1;
            leaf[key]._lexusAvg.v += Number(dots.get(json, this.lexusQuery[i].operation.field));
            break;
          case "count":
            inc(leaf, key, 1);
            break;
          case "unique":
            if (!leaf._lexusCard) {
              leaf._lexusCard = {};
            }
            inc(leaf._lexusCard, key, 1);
            break;
          case "find":
            leaf[key] = leaf[key] || [];
            if (leaf[key].length < this.lexusQuery[i].operation.params.limit) {
              leaf[key].push(this.projection(this.lexusQuery[i].operation.params, json));
            }
            break;

            /*
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
            */
          default:
            console.error("method not supported: " + this.lexusQuery[i].operation.method);
        }
      }
    }
  }
};

if (require.main === module) {
  const readline = require("readline");

  let rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: false
  });

  function doStart() {
    lexusEventStream.init(process.argv[2]);
    linesRead = 0;
  }

  function doFinal() {
    lexusEventStream.finalize();
    console.error(JSON.stringify(lexusEventStream.lexusResult));
  }

  if (process.argv.length > 3) {
    perlines = process.argv[3];
  }

  doStart();

  rl.on("line", function (line) {
    lexusEventStream.processEvent(JSON.parse(line));
    linesRead += 1;
    if (perlines && (linesRead > Number(perlines))) {
      doFinal();
      doStart();
    }
    console.log(line);
  });

  rl.on("close", doFinal);
}
else {
  module.exports = lexusEventStream;
}
