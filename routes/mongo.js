var assert = require('assert');
var MongoClient = require('mongodb').MongoClient;
var url = 'mongodb://localhost:27017/test';
var start, finish;
var size = 100000;

//Connect to mongo
MongoClient.connect(url, (err, database) => {
    if (err) return console.log(err)
    db = database
})

var router = function (app) {

    /* GET home page. */
    app.get('/mongo-api', function (req, res, next) {
        res.send("Mongo DB");
    });

    // mongo api insert n rows
    app.post('/mongo-api/data', function (req, res) {
        start = new Date();
        console.log("executing my stuff");
        for (var num = 0; num < size; num++) {
            insertDocument(db, num, function () {
                db.close();
            });
        }
        finish = new Date();
        var time = (finish.getTime() - start.getTime()) + " ms";
        console.log("Operation took " + time);
        return res.send({ "Number of rows inserted": size, "Time spent": time });
    });

}

// insert a row
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

module.exports = router;
