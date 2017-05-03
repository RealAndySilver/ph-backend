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

var theArray = [];

var insertionLoop = function (db, array, callback) {
    for (let item of array) {
        //theArray.push(item.dataarray)
        db.collection('basic').updateOne(
            {
                "tag": item.tag
            }, {
                $push: {
                    "data_array": item.dataarray
                }
            }, {
                upsert: true
            }, function (err, result) {
                //console.log("result ", err || result);
            });
    }
    callback();
}


MongoClient.connect(url, function (err, db) {
    //insertDocument(db);
    var array = [];

    var n = 1;

    for (var num = 0; num < size; num++) {
        array.push({
            "tag": 'sensor_' + n,
            "creation_time": new Date(),
            "dataarray": [{
                "txt": 'Hello!',
                "val": num,
                "date": new Date()
            }]
        });
        n++;
        if (n > 10) {
            n = 1;
        }
    }

    //array.dataarray = array2;

    start = new Date();
    console.log("executing my stuff");

    insertionLoop(db, array, function (err, result) {
        console.log("result ", err || result);
        finish = new Date();
        console.log("Operation took " + (finish.getTime() - start.getTime()) + " ms");
    });

    //

});
