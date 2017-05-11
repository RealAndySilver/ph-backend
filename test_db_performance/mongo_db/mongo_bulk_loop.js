var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var express = require('express');

var url = 'mongodb://localhost:27017/test';
var app = express();


var start, finish;

var size = 100000;

var insertDocument = function (bulk, num, callback) {
  bulk.insert({
    "txt": 'sensor_1',
    "val": Math.random(),
    "date": Date.now()
  }, function (err, result) {
    assert.equal(err, null);
    callback();
  });
};


MongoClient.connect(url, function (err, db) {
  start = new Date();
  console.log("executing my stuff");
  assert.equal(null, err);
  var bulk = db.collection('basic').initializeUnorderedBulkOp();
  for (var num = 0; num < size; num++) {
    insertDocument(bulk, num);
  }
  bulk.execute(function (){
    finish = new Date();
    console.log("bulk Operation took " + (finish.getTime() - start.getTime()) + " ms");
    db.close();
  });
  

});
