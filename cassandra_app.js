"use strict";
const cassandra = require('cassandra-driver');
const assert = require('assert');

const client = new cassandra.Client({ contactPoints: ['127.0.0.1']});
const id = cassandra.types.Uuid.random();
var num = 111;
var size = 100000;
var start, finish;

/**
 * Example using Promise.
 * See basic-execute-flow.js for an example using callback-based execution.
 */
client.connect()
  .then(function (){
    var query = "CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' }";
    client.execute(query);
    var query = "CREATE TABLE IF NOT EXISTS examples.basic (id uuid, txt text, val int, date timestamp , PRIMARY KEY(id))";
    client.execute(query);
    var query = 'INSERT INTO examples.basic (id, txt, val, date) VALUES (?, ?, ?, ?)';
    start = new Date();
    console.log("executing my stuff");
    for (var num = 1; num <= size; num++) {
      client.execute(query, [cassandra.types.Uuid.random(), 'Hello!', num, new Date()], { prepare: true },  function(err){
        assert.ifError(err);
    });
    }
    finish = new Date();
    console.log("Operation took " + (finish.getTime() - start.getTime()) + " ms");
  })
  .catch(function (err) {
    console.error('There was an error when connecting', err);
    return client.shutdown();
  });