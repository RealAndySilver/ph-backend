"use strict";
const cassandra = require('cassandra-driver');
const async = require('async');
const assert = require('assert');

const client = new cassandra.Client({ contactPoints: ['127.0.0.1'] });
var size = 100000;
var max_array_size = 50000;
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
        var query = "CREATE TABLE IF NOT EXISTS examples.basic (id uuid, txt text, val int, date timestamp , PRIMARY KEY(id))";
        client.execute(query, next);
    },
    function insert(next) {
        var query = 'INSERT INTO examples.basic (id, txt, val, date) VALUES (?, ?, ?, ?)';
        start = new Date();
        console.log("executing my stuff");
        var queries = [];
        for (var num = 1; num <= size; num++) {
            queries.push({ "query": query, "params": [cassandra.types.Uuid.random(), 'Hello!', num, new Date()] });
        }
        console.log("rows to insert: ",queries.length)

        //console.log(chunks.length);
        if (size > 50000) {
            var chunks = split(queries, max_array_size);
            console.log("chunk size: ", chunks.length);
            for (var i = 0; i < chunks.length; i++) {
                client.batch(chunks[i], { prepare: true }, function (err) {
                    assert.ifError(err);
                });
            }
        } else {
            client.batch(queries, { prepare: true }, function (err) {
                assert.ifError(err);
            });
        }

        finish = new Date();
        console.log("Operation took " + (finish.getTime() - start.getTime()) + " ms");
    }
], function (err) {
    if (err) {
        console.error('There was an error', err.message, err.stack);
    }
    console.log('Shutting down');
    client.shutdown();
});

function split(array, groupsize) {
    var sets = [], chunks, i = 0;
    chunks = array.length / groupsize;

    while (i < chunks) {
        //console.log(i);
        sets[i] = array.splice(0, groupsize);
        i++;
    }

    return sets;
};