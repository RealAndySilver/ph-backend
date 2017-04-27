var assert = require('assert');
var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');
var url = 'mongodb://localhost:27017/test';
var start, finish;
var globalArray = [];
var tempArray = [];
var flag = true;

var file = './public/average.csv';

//app.use(express.bodyParser({limit: ‘50mb’}));


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
        var size = 1;
        if (!req.body.txt || !req.body.val || !req.body.date) {
            res.status(400).json({ message: 'Please fill out all fields' });
        }
        start = new Date();
        console.log("executing my stuff");
        insertDocument(db, req, function () {
            db.close();
        });
        finish = new Date();
        var time = (finish.getTime() - start.getTime()) + " ms";
        console.log("Operation took " + time);
        res.send({ "Number of rows inserted": size, "Time spent": time });
    });

    // mongo api insert n rows
    app.post('/mongo-api/big-data', function (req, res) {

        var bigdata = req.body.bigdata;
        //if (typeof bigdata == Array) {
        if (bigdata.length != 0) {
            for (var i = 0; i < bigdata.length; i++) {
                globalArray.push(bigdata[i]);
            }
        }
        console.log("globalArray ", globalArray[0].txt);
        res.send("ok");

    });


}

setInterval(function () {
    if (flag) {
        flag = false;
        tempArray = [];
        //console.log("globalArray ",globalArray[0]);
        tempArray = globalArray.slice();
        globalArray = [];

        //console.log("tempArray ", tempArray[0]);
        if (tempArray.length != 0) {
            //console.log("tempArray1: ", tempArray);
            start = new Date();
            //console.log("executing my stuff");
            var bulk = db.collection('basic').initializeUnorderedBulkOp();
            for (var num = 0; num < tempArray.length; num++) {
                //  console.log("tempArray2: ", tempArray[num]);
                bulkInsertDocument(bulk, tempArray[num]);
            }
            bulk.execute(function () {
                flag = true;
                console.log("success!!");
                finish = new Date();
                var time = (finish.getTime() - start.getTime());
                try {
                    fs.appendFileSync(file, 'date,' + new Date() + ',time,' + time + '\n');
                } catch (e) {

                }
                console.log("Operation took " + time);
            });

        } else {
            flag = true;
        }

    }
}, 1000);


// insert a row
var bulkInsertDocument = function (bulk, req) {
    bulk.insert({
        "txt": req.txt,
        "val": req.val,
        "date": req.date
    });
};

var insertDocument = function (db, req, callback) {
    db.collection('basic').insertOne({
        "txt": req.body.txt,
        "val": req.body.val,
        "date": req.body.date
    }, function (err, result) {
        assert.equal(err, null);
        callback();
    });
};

module.exports = router;
