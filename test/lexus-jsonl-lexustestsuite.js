const should = require('should');
const lexusJsonl = require('../lexus-jsonl');
const lexusTestsuite = require('../node_modules/lexus/testsuite'); // FIXME

suite('lexus-jsonl compliance with Lexus Testsuite', () => {
  test('all', (done) => {
    for (let i in lexusTestsuite.queries) {
      let q = lexusTestsuite.queries[i].query[0];
      lexusJsonl.init(JSON.stringify(q));
      for (let i of lexusTestsuite.records) {
        lexusJsonl.processEvent(i);
      }
      lexusJsonl.finalize();
      // console.log(JSON.stringify(lexusJsonl.lexusResult));
      should(lexusTestsuite.queries[i].result[0]).deepEqual(lexusJsonl.lexusResult);
    }
    done();
  });
});
