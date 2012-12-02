'use strict';

var querystring         = require('querystring')
  , ElasticSearchClient = require('elasticsearchclient')
  ;

var Operation = function (operation, command, document) {
  this.operation = operation;
  this.command   = command;
  this.document  = document;
};

Operation.prototype.marshal = function () {
  var command = {};
  command[this.operation] = this.command;
  var marshalled = JSON.stringify(command) + '\n';

  if (this.document) marshalled += JSON.stringify(this.document) + '\n';

  return marshalled;
};


var CORE_OPERATIONS = [
  'multiget',
  'multisearch',
  'percolate',
  'percolator',
  'search'
];

var BulkAPI = function (queue) {
  this.queue = queue;
};

BulkAPI.prototype.count = function (indexName, typeName, query) {
  var command = {
    _index : indexName,
    _type  : typeName,
    _query : query
  };

  this.queue.push(new Operation('count', command));
};

BulkAPI.prototype.deleteByQuery = function (indexName, typeName, queryObj) {
  var command = {
    _index : indexName,
    _type  : typeName,
    _query : queryObj
  };

  this.queue.push(new Operation('delete', command));
};

BulkAPI.prototype.deleteDocument = function (indexName, typeName, documentId) {
  var command = {
    _index : indexName,
    _type  : typeName,
    _id    : documentId
  };

  this.queue.push(new Operation('delete', command));
};

BulkAPI.prototype.get = function (indexName, typeName, documentId) {
  var command = {
    _index : indexName,
    _type  : typeName,
    _id    : documentId
  };

  this.queue.push(new Operation('get', command));
};

BulkAPI.prototype.index = function (indexName, typeName, document) {
  var command = {
    _index : indexName,
    _type  : typeName
  };

  if (document.id) {
    command._id = document.id;
    delete document.id;
  }

  this.queue.push(new Operation('index', command, document));
};

BulkAPI.prototype.moreLikeThis = function (indexName, typeName, documentId) {
  var command = {
    _index : indexName,
    _type  : typeName,
    _id    : documentId
  };

  this.queue.push(new Operation('mlt', command));
};

BulkAPI.prototype.multiget = function (indexName, typeName, documentArray) {
  var command = {
    _index : indexName,
    _type  : typeName
  };

  this.queue.push(new Operation('mget', command, documentArray));
};


// there is a bulk operation already defined on the prototype, but it's pretty raw
ElasticSearchClient.prototype.bulk = function (callback, options) {
  var path          = '/_bulk'
    , qs            = ''
    , queue         = []
    , commandBuffer = ''
    ;

  if (options) qs = querystring.stringify(options);
  if (qs.length > 0) path += "?" + qs;

  callback(new BulkAPI(queue));
  queue.forEach(function (command) { commandBuffer += command.marshal(); });

  return this.createCall({path   : path,
                          method : 'POST',
                          data   : commandBuffer},
                         this.clientOptions);
};

module.exports = ElasticSearchClient;
