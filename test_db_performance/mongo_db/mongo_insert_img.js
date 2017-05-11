var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var express = require('express');
var fs = require('fs');

var url = 'mongodb://localhost:27017/test';
var app = express();


var start, finish;
var file = '/Users/parzifal/Pictures/display_pic.jpg';
var blob;

var size = 1000;

var insertDocument = function (bulk, num, callback) {

    bulk.insert({
        "txt": 'sensor_1',
        "val": Math.random(),
        "date": Date.now(),
        "path": file
        //"blob": blob
    }, function (err, result) {
        assert.equal(err, null);
        callback();
    });
};


MongoClient.connect(url, function (err, db) {
    bin = fs.readFileSync(file);
    start = new Date();
    console.log("executing my stuff");
    assert.equal(null, err);
    var bulk = db.collection('basic').initializeUnorderedBulkOp();
    var fileArray = [];
    for (var num = 0; num < size; num++) {
        fileArray.push({
            "new_path": "image_" + num + ".jpg",
            "bin_file": bin
        })
        insertDocument(bulk, num);
    }
    insertFiles(fileArray);

    bulk.execute(function () {
        finish = new Date();
        console.log("bulk Operation took " + (finish.getTime() - start.getTime()) + " ms");
        db.close();
    });


});

function insertFiles(fileArray) {
    for (let value of fileArray) {
        fs.writeFileSync("/Users/parzifal/Pictures/uploaded_images/" + value.new_path, value.bin_file);
    }
}
