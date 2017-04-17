var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var ObjectId = require('mongodb').ObjectID;
var url = 'mongodb://localhost:27017/test';

var start, finish;

var size = 100000;

var insertDocument = function(db, num, callback) {
   db.collection('basic').insertOne( {
      "txt" : 'Hello!',
      "val" : num,
      "date": new Date()
   }, function(err, result) {
    assert.equal(err, null);
    //console.log("Inserted a document into the restaurants collection.");
    callback();
  });
};


MongoClient.connect(url, function(err, db) {
  start = new Date();  
  console.log("executing my stuff");
  assert.equal(null, err);
  for (var num =0; num<size ;num++) {
    insertDocument(db, num);
  }
  db.close();
  finish = new Date();
console.log("Operation took " + (finish.getTime() - start.getTime()) + " ms");  
  
});
