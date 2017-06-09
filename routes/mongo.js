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


//Connect to mongo
MongoClient.connect(url, (err, database) => {
    if (err) return console.log(err)
    db = database
});
var log = '';
var router = function (app, io) {

    // mongo api insert n rows (with images & videos)
    app.post('/mongo-api/big-data', upload.array("upload_files"), function (req, res) {
        res.send("ok");
        var files = req.files;
        var bigdata = req.body;
        var dir = '';
        var vr;
        var tag;
        var file_size;
        var date;
        var file_path = '';

        if (files) {
            for (var i = 0; i < files.length; i++) {
                if (files.length > 1) {
                    vr = bigdata.var[i];
                    tag = bigdata.tag[i];
                    file_size = bigdata.file_size[i];
                    date = new Date(bigdata.date[i]);
                } else {
                    vr = bigdata.var;
                    tag = bigdata.tag;
                    file_size = bigdata.file_size;
                    date = new Date(bigdata.date);
                }
                dir = "./data/tags/" + tag + "/" + vr + "/" + file_size;
                file_path = dir + "/" + files[i].originalname;

                start = new Date();
                writeFile(file_path, files[i].buffer, function (err) {
                    if (err) {
                        console.log('Err:', err);
                    }
                    finish = new Date();
                    var time = ((finish.getTime() - start.getTime()) / 1000) + " s";
                    log = vr + " Upload '+  +'operation took " + time;
                    console.log(vr + " Upload '+  +'operation took " + time);
                });
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
    });
    io.on('connection', function (socket) {
        console.log("connected");
        socket.on('counting requests', function () {
            console.log('counting ', log);
            socket.emit('counting requests', {
                numRequests: log
            });
        });
    });
}

//create file in the path
function writeFile(path, contents, callback) {
    mkdirp(getDirName(path), function (err) {
        if (err) return console.log(err);
        fs.writeFile(path, contents, function (err) {
            callback(err);
        });
    });
}

// Bulk Insert
var bulkInsertDocument = function (bulk, array) {
    bulk.insert(array);
};

//Upsert
var upsertLoop = function (db, grouped, callback) {
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
                callback(err, result);
                //console.log("result ", err || result);
            });
    }
}

//Bulk Update 
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

// Main process executed every second
setInterval(function () {
    if (flag) {
        flag = false;
        tempArray = [];
        tempArray = globalArray.slice();
        globalArray = [];
        if (tempArray.length != 0) {
            start = new Date();
            var row_size = tempArray.length;
            console.log("rows to insert: ", row_size)
            var bulk = db.collection('basic').initializeUnorderedBulkOp();
            for (let value of tempArray) {
                bulkInsertDocument(bulk, value);
            }
            bulk.execute(function () {
                flag = true;
                console.log("success!!");
                finish = new Date();
                var time = ((finish.getTime() - start.getTime()) / 1000);
                console.log("Insert operation took " + time);
                log = "Insert operation took " + time;
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

//alternative process for upsert version
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
            console.log("rows to insert: ", row_size);
            upsertLoop(db, grouped, function (err, result) {
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
}

module.exports = router;
