var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var express = require('express');

var url = 'mongodb://localhost:27017/test';
var app = express();


var start, finish;

var size = 100000;



MongoClient.connect(url, function (err, db) {
    
    assert.equal(null, err);
    var array = [];
    for (var num = 0; num < size; num++) {
        array.push({
            "txt": 'Hello!',
            "val": Math.random(),
            "date": new Date()
        });
    }
    start = new Date();
    console.log("executing my stuff");
    db.collection('basic').insertMany(array, function (err, result) {
        //assert.equal(err, null);
        //console.log("result ", err || result);
        finish = new Date();
        console.log("bulk Operation took " + (finish.getTime() - start.getTime()) + " ms");
        db.close();
    });
    /*var bulk = db.collection('basic').initializeUnorderedBulkOp();
    for (var num = 0; num < size; num++) {
      insertDocument(bulk, num);
    }
    bulk.execute(function (){
      finish = new Date();
      console.log("bulk Operation took " + (finish.getTime() - start.getTime()) + " ms");
    });*/


});
