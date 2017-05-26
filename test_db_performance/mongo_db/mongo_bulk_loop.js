var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var express = require('express');

var url = 'mongodb://localhost:27017/test';
var app = express();


var start, finish;

var size = 100000;

var crypto = require('crypto');
var format = require('biguint-format');

var bulkInsertDocument = function (bulk, array) {
    bulk.insert(array);
};


MongoClient.connect(url, function (err, db) {

  assert.equal(null, err);
  var array = [];
  for (var num = 0; num < size; num++) {
    array.push({
      //"txt": 'random_positive',
      //"txt": 'random_positive',
      //"txt": 'n_32bit_integer',
      //"txt": 'n_64bit_integer',
      "txt": 'n_base64_string',
      //"val": Math.random(),
      //"val": Math.random()*-1,
      "val": new Buffer(Math.random() * 50).toString('base64'),
      //"val": format(crypto.randomBytes(8)),
      "date": new Date(),
      "var": "PV"
    })
  }
  
  start = new Date();
  console.log("executing my stuff");
  var bulk = db.collection('basic').initializeUnorderedBulkOp();
  for (let value of array) {
    bulkInsertDocument(bulk, value);
    //bulkUpdateDocument(bulk, tempArray);//[num])
  }
  

  bulk.execute(function () {
    finish = new Date();
    console.log("bulk Operation took " + (finish.getTime() - start.getTime()) + " ms");
    db.close();
  });


});
