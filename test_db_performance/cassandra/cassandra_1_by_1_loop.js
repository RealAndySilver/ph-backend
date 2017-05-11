"use strict";
const cassandra = require('cassandra-driver');
const async = require('async');
const assert = require('assert');

const client = new cassandra.Client({ contactPoints: ['127.0.0.1'] });
var num = 111;
var size = 100000;
var start, finish;
/**
 * Example using async library for avoiding nested callbacks
 * See https://github.com/caolan/async
 * Alternately you can use the Promise-based API.
 */
//const id = cassandra.types.Uuid.random();

async.series([
  function connect(next) {
    client.connect(next);
  },
  function createKeyspace(next) {
    var query = "CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' }";
    client.execute(query, next);
  },
  function createTable(next) {
    var query = 'CREATE TABLE IF NOT EXISTS examples.basic (id uuid, txt text, val int, date timestamp , PRIMARY KEY(id))';
    client.execute(query, next);
  },
  function insert(next) {
    start = new Date();
    console.log('executing my stuff');
    insertionLoop(client, function () {
      finish = new Date();
      console.log('Operation took ' + (finish.getTime() - start.getTime()) + " ms");
      //next();
    });
  },
  function close(next){
    client.shutdown();
  }
], function (err) {
  if (err) {
    console.error('There was an error', err.message, err.stack);
  }
  console.log('Shutting down');
  client.shutdown();
});

var insertionLoop = function (client, callback) {
  var query = 'INSERT INTO examples.basic (id, txt, val, date) VALUES (?, ?, ?, ?)';
  for (var num = 1; num <= size; num++) {
    client.execute(query, [cassandra.types.Uuid.random(), 'sensor_1', Math.random(), Date.now()], { prepare: true }, function (err) {
      assert.ifError(err);
    });
  }
  callback();
}