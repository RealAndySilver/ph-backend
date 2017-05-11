var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var express = require('express');

var url = 'mongodb://localhost:27017/test';
var app = express();


var start, finish;

var size = 10;

var insertDocument = function (db) {
    db.collection('basic').insertOne({
        "tag": 'sensor_1',
        "creation_time": new Date(),
        "data_array": []
    });
};

var insertionLoop = function (db, array, callback) {

    db.collection('basic').updateOne({
        "tag": 'sensor_1'
    }, {
            $push: {
                "data_array": array.data_array
            }
        }, {
            upsert: true
        }, function (err, result) {
            //console.log("result ", err || result);
        });
    callback();
}


MongoClient.connect(url, function (err, db) {
    insertDocument(db);
    var array = [];
    var array2 = []
    array.push({
        "tag": 'sensor_1',
        "creation_time": new Date(),
        "data_array": []
    })
    for (var num = 0; num < size; num++) {
        array2.push({
            "txt": 'Hello!',
            "val": num,
            "date": new Date()
        });

    }
    array.data_array = array2;
    console.log("array", array);
    start = new Date();
    console.log("executing my stuff");
    insertionLoop(db, array, function (err, result) {
        console.log("result ", err || result);
        finish = new Date();
        console.log("Operation took " + (finish.getTime() - start.getTime()) + " ms");
    });

});
