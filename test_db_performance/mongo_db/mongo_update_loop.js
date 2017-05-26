var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var express = require('express');

var url = 'mongodb://localhost:27017/test';
var app = express();


var start, finish;

var _ = require('underscore');

var size = 100000;

var theArray = [];

var crypto = require('crypto');
var format = require('biguint-format');

var insertionLoop = function (db, grouped, callback) {
    for (let item of grouped) {
        db.collection('basic').updateOne(
            {
                "tag": item.tag
            }, {
                $push: {
                    "data_array": item.data
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
            //"val": Math.random()*-1,
            //"val": new Buffer(Math.random() * 50).toString('base64'),
            "val": format(crypto.randomBytes(4)),
            "date": new Date(),
            "var":"PV"
        });
        n++;
        if (n > 1000) {
            n = 1;
        }
    }

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
    start = new Date();
    console.log("executing my stuff");

    insertionLoop(db, grouped, function (err, result) {
        //console.log("result ", err || result);
        finish = new Date();
        console.log("Operation took " + (finish.getTime() - start.getTime()) + " ms");
        db.close()
    });

    //

});
