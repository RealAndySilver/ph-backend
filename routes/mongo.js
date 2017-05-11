var assert = require('assert');
var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');
var url = 'mongodb://localhost:27017/test';
var start, finish;
var globalArray = [];
var tempArray = [];
var flag = true;
var mkdirp = require('mkdirp');
var getDirName = require('path').dirname;


var file = './public/mongo_average.csv';

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

    // mongo api insert a row
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
    app.post('/mongo-api/big-data----', function (req, res) {
        res.send("ok");
        var bigdata = req.body.bigdata;
        //if (typeof bigdata == Array) {
        if (bigdata.length != 0) {
            for (var i = 0; i < bigdata.length; i++) {
                globalArray.push(bigdata[i]);
            }
        }
    });

    // mongo api insert n rows (with images & videos)
    app.post('/mongo-api/big-data', function (req, res) {
        res.send("ok");
        var bigdata = req.body.bigdata;
        var binArray = [];
        //if (typeof bigdata == Array) {
        if (bigdata.length != 0) {
            //for (let value of bigdata) {
            for (var i = 0; i < bigdata.length; i++) {
                var value = bigdata[i];
                if (value.file) {
                    var dir = "/Users/parzifal/data/AP/" + value.txt;
                    var file_path = dir + "/" + i + "_" + value.date + ".png";
                    //console.log("bin: "+value.file.bin);
                    var b = new Buffer(value.file.bin.data);
                    writeFile(file_path, b);
                    globalArray.push(
                        {
                            txt: value.txt,
                            path: file_path,
                            val: value.val,
                            date: value.date
                        }
                    );
                } else {
                    globalArray.push(value);
                }
            }
        }
    });

    //mongo api update 
    app.post('/mongo-api/big-data-update', function (req, res) {
        res.send("ok");
        var bigdata = req.body.bigdata;
        //console.log("big-data: " + JSON.stringify(bigdata));
        //if (typeof bigdata == Array) {
        var array = [];
        /*array.push({
            "tag": 'sensor_1',
            "creation_time": new Date(),
            "data_array":[]
        })*/
        if (bigdata.length != 0) {
            for (var i = 0; i < bigdata.length; i++) {
                globalArray.push(bigdata[i]);
            }
        }
        //array[0].data_array = array2;
        //globalArray = array;
    })


}

function writeFile(path, contents, cb) {
  mkdirp(getDirName(path), function (err) {
    if (err) return cb(err);

    fs.writeFileSync(path, contents, cb);
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
            console.log("executing my stuff");
            var row_size = tempArray.length;
            console.log("rows to insert: ", row_size)
            var bulk = db.collection('basic').initializeUnorderedBulkOp();
            for (let value of tempArray) {
                bulkInsertDocument(bulk, value);
                //bulkUpdateDocument(bulk, tempArray);//[num])
            }
            bulk.execute(function () {
                flag = true;
                console.log("success!!");
                finish = new Date();
                var time = (finish.getTime() - start.getTime());
                console.log("Operation took " + time);
                try {
                    fs.appendFileSync(file, 'rows inserted,' + row_size + ',date,' + new Date() + ',time,' + time + '\n');
                } catch (e) {

                }

            });

        } else {
            flag = true;
        }

    }
}, 1000);


// insert a row
var bulkInsertDocument = function (bulk, array) {
    bulk.insert(array);
};

// insert a row
var bulkUpdateDocument = function (bulk, array) {
    bulk.find({ "tag": array[0].txt }).upsert().updateOne({
        $set: {
            "creation_time": new Date()
        },
        $push: {
            "data_array": array
        }
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
