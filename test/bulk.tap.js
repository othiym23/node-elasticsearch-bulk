'use strict';

var spawn               = require('child_process').spawn
  , test                = require('tap').test
  , carrier             = require('carrier')
  , longjohn            = require('longjohn')
  , ElasticSearchClient = require('../index')
  ;

/*
 * constants
 */
var INDEX_NAME = 'your_index_name'
  , OBJECT_NAME = 'your_object_name'
  ;

test("the bulk command", function (t) {
  // shutdown shuts down the nodes, so need to run the cluster ourselves
  var server = spawn('elasticsearch', ['-f'], {stdio : 'pipe'});
  server.on('close', function () {
    console.log('# elasticsearch server shut down');
  });

  this.tearDown(function () {
    server.kill();
  });

  carrier.carry(server.stdout, function (line) {
    if (line.match('started')) {
      process.nextTick(function () {
        var client = new ElasticSearchClient({
          host : 'localhost',
          port : 9200
        });

        t.test("simple bulk command", function (t) {
          t.plan(1);
          var bulk = client.bulk(function (b) {
            b.index(INDEX_NAME, OBJECT_NAME, {name : 'name', id : '1111'});
            b.index(INDEX_NAME, OBJECT_NAME, {name : 'another', id : '2222'});
            b.deleteDocument(INDEX_NAME, OBJECT_NAME, '2222');
          });

          bulk.on('data', function (data) {
            var returned = JSON.parse(data);
            console.dir(returned.items);
            t.equals(returned.items.length, 3, "succeeded");
            t.end();
          });

          bulk.exec();
        });

        t.end();
      });
    }
  });
});
