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
var socket;
var db;
//Connect to mongo
MongoClient.connect(url, (err, database) => {
    if (err) return console.log(err)
    db = database;
    db.collection('basic').createIndex({tag:1}, function(){
	    db.collection('basic').ensureIndex({tag:1}, {unique:true}, function(err, indexName) {
	    	//console.log('Indexed', indexName);
	    });
    });
    db.collection('basic').createIndex({date:1}, function(){
	    db.collection('basic').ensureIndex({date:1}, {unique:false}, function(err, indexName) {
	    	//console.log('Indexed', indexName);
	    });
    });
    
    db.collection('past').createIndex({tag:1}, function(){
	    db.collection('past').ensureIndex({tag:1}, {unique:true}, function(err, indexName) {
	    	//console.log('Indexed', indexName);
	    });
    });
    db.collection('past').createIndex({date:1}, function(){
	    db.collection('past').ensureIndex({date:1}, {unique:false}, function(err, indexName) {
	    	//console.log('Indexed', indexName);
	    });
    });
    
});
var log = {
	upload : {
		
	},
	insert : {
		
	}
};
var emit = function(){
	if(socket){
		socket.emit('getServerInfo', {
	        log: log
	    });
    }
};
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
                    log.upload.message = vr + "Upload '+  +'operation took " + time;
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

    io.on('connection', function (socket_m) {
	    console.log("connected");
	    socket = socket_m;
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
/*
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
                log.insert.final_time = "Insert operation took " + time;
                log.insert.rows_inserted = tempArray.length;
				log.insert.insertion_time = time + 's';
				log.insert.insertion_date = new Date();
				emit();
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
*/
var giant = {};
setInterval(function () {
    if (flag) {
        flag = false;
        tempArray = [];
        tempArray = globalArray.slice();
        globalArray = [];
        if (tempArray.length != 0) {
            var start = new Date();
            var row_size = tempArray.length;
            //console.log("rows to insert: ", row_size)
            //var bulk = db.collection('basic').initializeUnorderedBulkOp();
            //for (let value of tempArray) {
	        //for (let i = 0; i<tempArray.length;i++) {
		    tempArray.forEach(function(item){
	            let date = {};
	            date.date = new Date(item.date);
	            date.year = date.date.getYear();
                date.month = date.date.getMonth()+1;
				date.day = date.date.getDate();
				date.hours = date.date.getHours();
				date.minutes = date.date.getMinutes();
				date.seconds = date.date.getSeconds();
				let set={};
				if(!giant[item.tag]){
					giant[item.tag] = {};
					giant[item.tag].data = {};
				}

				/*if(!giant[item.tag]['data'+'.'+date.month+'.'+date.day+'.'+date.hours+'.'+date.minutes]){
					giant[item.tag]['data'+'.'+date.month+'.'+date.day+'.'+date.hours+'.'+date.minutes] = {};
					giant[item.tag]['data'+'.'+date.month+'.'+date.day+'.'+date.hours+'.'+date.minutes][date.seconds] = {};
				}
				giant[item.tag]['data'+'.'+date.month+'.'+date.day+'.'+date.hours+'.'+date.minutes][date.seconds] = item.val
*///{/* d:date.date.getTime(), */ v:item.val};
/*
				if(!giant[item.tag].data['data'+'.'+date.minutes]){
					giant[item.tag].data['data'+'.'+date.minutes] = {};
					giant[item.tag].data['data'+'.'+date.minutes][date.seconds] = {};
				}
*/
				giant[item.tag].date = new Date('2017', date.month, date.day , date.hours );
				//giant[item.tag].data['data'+'.'+date.minutes+'.'+date.seconds] = item.val;
				giant[item.tag].data['data'+'.'+date.minutes+'.'+date.seconds] = item.val;
				//set['data'+'.'+date.month+'.'+date.day+'.'+date.hours+'.'+date.minutes+'.'+date.seconds]={date:date.date, val:tempArray[i].val};
				//set.date = date;
/*
		        bulk.find({
		            "tag": tempArray[i].tag + '_2017' + date.month + date.day + date.hours
		        })
		        .upsert()
		        .update(
		        {
			    	//$inc: {qty:1},
		            $set: set,
		        });
*/
            });
            //bulk.execute(function(err,res){
	            //console.log(Object.keys(giant['Positive_PV_Sender0']));
	            flag = true;
			    var finish = new Date();
			    arr = [];
			    var time = ((finish.getTime() - start.getTime()) / 1000);
			    //console.log('Success. Upsert operation took ',time,'s');
			    //log.insert.final_time = "Insert operation took " + time;
                //log.insert.rows_inserted = tempArray.length;
				//log.insert.insertion_time = time + 's';
				//log.insert.insertion_date = new Date();
				//emit();
				try {
                    //fs.appendFileSync(file, 'rows upserted,' + row_size + ',date,' + new Date() + ',time,' + time + '\n');
                } catch (e) {

                }
		    //});
        } 
        else {
            flag = true;
        }

    }
}, 1000);
function clone(obj) {
    if (null == obj || "object" != typeof obj) return obj;
    var copy = obj.constructor();
    for (var attr in obj) {
        if (obj.hasOwnProperty(attr)) copy[attr] = obj[attr];
    }
    return copy;
}

/*
setInterval(function () {
    if (flag) {
        flag = false;
        tempArray = [];
        tempArray = globalArray.slice();
        globalArray = [];
        if (tempArray.length != 0) {
            var start = new Date();
            var row_size = tempArray.length;
            console.log("rows to insert: ", row_size)
            var bulk = db.collection('basic').initializeUnorderedBulkOp();
            //for (let value of tempArray) {
	        //for (let i = 0; i<tempArray.length;i++) {
		    tempArray.forEach(function(item){
	            let date = {};
	            date.date = new Date(item.date);
	            date.year = date.date.getYear();
                date.month = date.date.getMonth()+1;
				date.day = date.date.getDate();
				date.hours = date.date.getHours();
				date.minutes = date.date.getMinutes();
				date.seconds = date.date.getSeconds();
				let set={};
				set[date.hours] = item.val;
		        bulk.find({
		           "tag": item.tag,
				   "date": new Date('2017', date.month, date.day , date.hours )
		        })
		        .upsert()
		        .update(
		        {
			    	//$inc: {qty:1},
		            $push: set,
		        });
		        
            });
            bulk.execute(function(err,res){
	            //console.log(Object.keys(giant['Positive_PV_Sender0']));
	            flag = true;
			    var finish = new Date();
			    arr = [];
			    var time = ((finish.getTime() - start.getTime()) / 1000);
			    console.log('Success. Upsert operation took ',time,'s');
			    //log.insert.final_time = "Insert operation took " + time;
                //log.insert.rows_inserted = tempArray.length;
				//log.insert.insertion_time = time + 's';
				//log.insert.insertion_date = new Date();
				//emit();
				
		    });
        } 
        else {
            flag = true;
        }

    }
}, 1000);
*/
/*
setInterval(function(){
	var date = new Date();
	let seconds = date.getSeconds();
	var keys = null;
	if(seconds == 0){
		keys = Object.keys(giant);
		for (let i = 0; i<tempArray.length;i++) {
			set['data'+'.'+date.month+'.'+date.day+'.'+date.hours+'.'+date.minutes+'.'+date.seconds]={date:date.date, val:tempArray[i].val};
			//set.date = date;
	        bulk.find({
	            "tag": tempArray[i].tag + '_2017' + date.month + date.day + date.hours
	        })
	        .upsert()
	        .update(
	        {
		    	//$inc: {qty:1},
	            $set: set,
	        });
        }
		bulk.execute(function(err,res){
            console.log(Object.keys(giant));
            flag = true;
		    var finish = new Date();
		    arr = [];
		    var time = ((finish.getTime() - start.getTime()) / 1000);
		    console.log('Success. Upsert operation took ',time,'s');
		    //log.insert.final_time = "Insert operation took " + time;
            //log.insert.rows_inserted = tempArray.length;
			//log.insert.insertion_time = time + 's';
			//log.insert.insertion_date = new Date();
			//emit();
			try {
                //fs.appendFileSync(file, 'rows upserted,' + row_size + ',date,' + new Date() + ',time,' + time + '\n');
            } catch (e) {

            }
	    });
	}
},1000);
*/

function updateTest(){
	var bulk = db.collection('basic').initializeUnorderedBulkOp();
	var local_giant = clone(giant);//JSON.parse(JSON.stringify(giant));
	giant = {};
	var keys = Object.keys(local_giant);
	var start = null;
	var finish = null;
	start = new Date();
	console.log('Started upsert with ',keys.length);
	
	//for (let item of arr) {
	for(let i = 0; i< keys.length; i++){
		//set['value.'+hours+minutes+seconds+i]={date:new Date(), val:Math.random()};
		//console.log(local_giant[keys[i]])
        bulk.find({
            "tag": keys[i],
            "date": local_giant[keys[i]].date,
        })
        .upsert()
        .update(
        {
	    	//date : local_giant[keys[i]].date,
            $set: local_giant[keys[i]].data
        });
    }
    bulk.execute(function(err,res){
	    finish = new Date();
	    console.log('Finished in ',(finish.getTime() - start.getTime())/1000+'s');
    });
}
setInterval(function(){
	updateTest();
}, 60000);


function move(){
	var bulkInsert = db.collection('past').initializeUnorderedBulkOp()
	var bulkRemove = db.collection('basic').initializeUnorderedBulkOp()
	var x = 10000
	var counter = 0
	var date = new Date();
	date.setMonth(date.getMonth()+1);
	date.setHours(date.getHours()-1);
    date.setMinutes(0);
    date.setSeconds(0);
    date.setMilliseconds(0);
    console.log('Moving files from basic to past using date',date);
	db.collection('basic').find({date:{$lte: date}}).forEach(function(doc){
		bulkInsert.insert(doc);
		bulkRemove.find({_id:doc._id}).removeOne();
		counter ++;
		if( counter % x == 0){
			bulkInsert.execute()
			bulkRemove.execute()
			bulkInsert = db.collection('past').initializeUnorderedBulkOp()
			bulkRemove = db.collection('basic').initializeUnorderedBulkOp()
		}
	});
}
setTimeout(function(){
	move();
}, 1000);
setInterval(function(){
	move();
}, 60000*60);
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
