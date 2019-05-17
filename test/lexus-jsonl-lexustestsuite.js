const should = require('should');
const lexusJsonl = require('../lexus-jsonl');
const lexusTestsuite = require('../node_modules/lexus/suite'); // FIXME

suite('lexus-jsonl compliance with Lexus Testsuite', () => {
  test('all', (done) => {
    lexusTestsuite.init().then(function () {
      for (let i in lexusTestsuite.queries) {
        let q = lexusTestsuite.queries[i].query[0];
        lexusJsonl.init(JSON.stringify(q));
        for (let i of lexusTestsuite.records) {
          lexusJsonl.processEvent(i);
        }
        lexusJsonl.finalize();
        lexusTestsuite.queries[i].result[0].version = '0.3'; // FIXME
        should(lexusTestsuite.queries[i].result[0]).deepEqual(lexusJsonl.lexusResult[0]);
      }
      done();
    }).catch(function (e) {
      console.log(e);
    });
  });
});
