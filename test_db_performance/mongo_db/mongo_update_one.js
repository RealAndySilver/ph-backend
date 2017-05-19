var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var express = require('express');

var url = 'mongodb://localhost:27017/test';
var app = express();


var start, finish;

var size = 100000;

var insertDocument = function (db) {
    db.collection('basic').insertOne({
        "tag": 'sensor_1',
        "creation_time": new Date(),
        "data_array": []
    });
};

var insertionLoop = function (db, array, callback) {

    db.collection('basic').updateOne({
        //upsert: true,
        "tag": 'sensor_1'
    }, {
            $push: {
                "data_array": array
            }
        }, function (err, result) {
            //console.log("result ", err || result);
        });
    callback();
}



MongoClient.connect(url, function (err, db) {
    insertDocument(db);
    var array = [];
    for (var num = 0; num < size; num++) {
        array.push({
            "txt": 'Hello!',
            "val": Math.random()*size,
            "date": new Date()
        });
    }
    start = new Date();
    console.log("executing my stuff");
    array.sort(function (a, b) {
        return a.val - b.val;
    });
    insertionLoop(db, array, function (err, result) {
        console.log("result ", err || result);
        finish = new Date();
        console.log("Operation took " + (finish.getTime() - start.getTime()) + " ms");
        db.close();
    });

});
