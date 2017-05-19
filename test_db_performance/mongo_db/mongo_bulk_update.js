var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var express = require('express');

var url = 'mongodb://localhost:27017/test';
var app = express();


var start, finish;

var _ = require('underscore');

var size = 100000;

var insertDocument = function (db) {
    db.collection('basic').insertOne({
        "tag": 'sensor_1',
        "creation_time": new Date(),
        "data_array": []
    });
};

var theArray = [];

var insertionLoop = function (bulk, grouped) {
    for (let item of grouped) {
        //console.log(item);
        //theArray.push(item.dataarray)
        //bulk.collection('basic').updateOne(
        bulk.find({
            "tag": item.tag
        }).upsert().update({
            $push: {
                "data_array": item.data
            }
        }, function (err, result) {
            //console.log("result ", err || result);
        });
    }
}


MongoClient.connect(url, function (err, db) {
    //insertDocument(db);
    var array = [];

    var n = 1;

    for (var num = 0; num < size; num++) {
        array.push({
            "tag": 'sensor_' + n,
            "val": Math.random(),
            "date": new Date()
        });
        n++;
        if (n > 1000) {
            n = 1;
        }
    }

    //var groups = _.omit("tag", _.groupBy(array, "tag"));

    var grouped = _.chain(array).groupBy("tag").map(function (data, tag) {
        // Optionally remove product_id from each record
        var dataArray = _.map(data, function (it) {
            return _.omit(it, "tag");
        });
        return {
            tag: tag,
            data: dataArray
        };
    }).value();
    //console.log("groups", grouped);
    start = new Date();
    console.log("executing my stuff");
    var bulk = db.collection('basic').initializeUnorderedBulkOp();
    insertionLoop(bulk, grouped);
    bulk.execute(function () {
        //console.log("result ", err || result);
        finish = new Date();
        console.log("Operation took " + (finish.getTime() - start.getTime()) + " ms");
        db.close()
    });

    //

});
