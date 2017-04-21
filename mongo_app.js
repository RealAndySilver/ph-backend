var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
//var ObjectId = require('mongodb').ObjectID;
var express = require('express');

var url = 'mongodb://localhost:27017/test';
var app = express();


var start, finish;

var size = 100000;

var insertDocument = function (db, num, callback) {
  db.collection('basic').insertOne({
    "txt": 'Hello!',
    "val": num,
    "date": new Date()
  }, function (err, result) {
    assert.equal(err, null);
    callback();
  });
};

var insertDocument = function (bulk, num, callback) {
  bulk.insert({
    "txt": 'Hello!',
    "val": num,
    "date": new Date()
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
    insertDocument(bulk, num);//, function(){
      //db.close();
    //});
  }
  bulk.execute();
  finish = new Date();
  console.log("Operation took " + (finish.getTime() - start.getTime()) + " ms");

});
