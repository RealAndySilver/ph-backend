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

        var file = req.file;
        console.log("file: ", file);

        res.send("ok");
    });

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
        
        //if (typeof bigdata == Array) {  
        if (files) {
            //for (let value of bigdata) {
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
                //var b = new Buffer(value.file.bin.data);
                writeFile(file_path, files[i].buffer, function (err) {
	                if(err){
		                console.log('Err:', err);
	                }
                    finish = new Date();
                    var time = ((finish.getTime() - start.getTime()) / 1000) + " s";
                    log = vr + "Upload '+  +'operation took " + time;
                    console.log(vr + "Upload '+  +'operation took " + time);
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
    var i=0;
    io.on('connection', function (socket) {
	    console.log("connected");
	    socket.on('counting requests', function () {
		    console.log('counting ',log);
	        socket.emit('counting requests', {
	            numRequests: log
	        });
	    });
	});
}

function writeFile(path, contents, callback) {
    mkdirp(getDirName(path), function (err) {
        if (err) return console.log(err);
        fs.writeFile(path, contents, function(err){
            callback(err);
        });
        
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
            console.log("rows to insert: ", row_size);
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
