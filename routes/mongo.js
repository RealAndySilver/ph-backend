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
var _ = require('underscore');
var multer = require('multer');
var upload = multer();


var file = './public/mongo_average.csv';

/*var MongoClient = require('mongodb').MongoClient
  , Server = require('mongodb').Server;

var mongoClient = new MongoClient(new Server('localhost', 27017));
mongoClient.open(function(err, mongoClient) {
  var db1 = mongoClient.db("mydb");

  mongoClient.close();
});*/

//Connect to mongo
MongoClient.connect(url, (err, database) => {
    if (err) return console.log(err)
    db = database
});

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
        var time = ((finish.getTime() - start.getTime()) / 1000) + " s";
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

    // upload file
    app.post('/upload', upload.single('my_file'), function (req, res) {

        console.log("uploading bitch");

        var file = req.file;
        console.log("file: ", file);

        res.send("ok");
    });

    // mongo api insert n rows (with images & videos)
    app.post('/mongo-api/big-data', upload.array("upload_files"), function (req, res) {
        res.send("ok");
        var files = req.files;
        var bigdata = req.body;

        //if (typeof bigdata == Array) {  
        if (files) {
            //for (let value of bigdata) {
            for (var i = 0; i < files.length; i++) {

                if (files.length > 1) {
                    var vr = bigdata.var[i];
                    var tag = bigdata.tag[i];
                    var file_size = bigdata.file_size[i];
                    var date = new Date(bigdata.date[i]);
                } else {
                    var vr = bigdata.var;
                    var tag = bigdata.tag;
                    var file_size = bigdata.file_size;
                    var date = new Date(bigdata.date);
                }
                var dir = "/data/" + vr + "/" + tag + "/" + file_size;
                var file_path = dir + "/" + files[i].originalname;

                start = new Date();
                //var b = new Buffer(value.file.bin.data);
                writeFile(file_path, files[i].buffer);
                finish = new Date();
                var time = ((finish.getTime() - start.getTime()) / 1000) + " s";
                console.log("Upload operation took " + time);

                globalArray.push(
                    {
                        tag: tag,
                        val: file_path,
                        date: date,
                        var: vr
                    }
                );
            }
        } else {
            bigdata = req.body.bigdata;
            for (var i = 0; i < bigdata.length; i++) {
                globalArray.push(bigdata[i]);
            }
        }
    });

    //mongo api insert by update 
    app.post('/mongo-api/big-data-update', function (req, res) {
        res.send("ok");
        bigdata = req.body.bigdata;
        if (bigdata.length != 0) {
            for (var i = 0; i < bigdata.length; i++) {
                globalArray.push(bigdata[i]);
            }
        }
        processArray();
    })


}

function writeFile(path, contents, cb) {
    mkdirp(getDirName(path), function (err) {
        if (err) return console.log(err);
        fs.writeFileSync(path, contents);
    });
}

function processArray() {
    if (flag) {
        flag = false;
        tempArray = [];
        tempArray = globalArray.slice();
        globalArray = [];
        if (tempArray.length != 0) {
            var grouped = _.chain(tempArray).groupBy("txt").map(function (data, tag) {
                // Optionally remove product_id from each record
                var dataArray = _.map(data, function (it) {
                    return _.omit(it, "txt");
                });
                return {
                    tag: tag,
                    data: dataArray
                };
            }).value();

            start = new Date();
            var row_size = tempArray.length;
            console.log("rows to insert: ", row_size)
            /*MongoClient.connect(url, (err, database) => {
                if (err) return console.log(err)
                db = database
            });*/
            insertionLoop(db, grouped, function (err, result) {
                //console.log("result ", err || result);
                flag = true;
                console.log("success!!");
                finish = new Date();
                var time = (finish.getTime() - start.getTime());
                console.log("Operation took " + time);
                try {
                    fs.appendFileSync(file, 'rows inserted,' + row_size + ',date,' + new Date() + ',time,' + time + '\n');
                } catch (e) {

                }
                /*db.close();*/
            });

        } else {
            flag = true;
        }

    }
}

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
                //db.close();
                //console.log("result ", err || result);
            });
    }
    callback();
}

var bulkUpdateLoop = function (bulk, grouped) {
    for (let item of grouped) {
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

setInterval(function () {
    if (flag) {
        flag = false;
        tempArray = [];
        //console.log("globalArray ",globalArray[0]);
        tempArray = globalArray.slice();
        globalArray = [];
        if (tempArray.length != 0) {
            //console.log("tempArray1: ", tempArray);
            start = new Date();
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
                var time = ((finish.getTime() - start.getTime()) / 1000) + " s";
                console.log("Insert operation took " + time);
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
