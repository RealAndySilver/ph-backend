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
var multer = require('multer');
var upload = multer();

var file = './public/mongo_average.csv';
var socket;
var global_db;
//Create Indexes
MongoClient.connect(url, (err, db) => {
    if (err) return console.log(err)
    global_db = db;
    db.collection('basic').createIndex({tag:1}, {unique:false}, function(){
	    db.collection('basic').ensureIndex({tag:1}, {unique:false}, function(err, indexName) {
	    	//console.log('Indexed', indexName);
	    });
    });
    db.collection('basic').createIndex({date:1}, {unique:false}, function(){
	    db.collection('basic').ensureIndex({date:1}, {unique:false}, function(err, indexName) {
	    	//console.log('Indexed', indexName);
	    });
    });
    db.collection('basic').createIndex({var:1}, {unique:false}, function(){
	    db.collection('basic').ensureIndex({var:1}, {unique:false}, function(err, indexName) {
	    	//console.log('Indexed', indexName);
	    });
    });
    
    db.collection('past').createIndex({tag:1}, {unique:false}, function(){
	    db.collection('past').ensureIndex({tag:1}, {unique:false}, function(err, indexName) {
	    	//console.log('Indexed', indexName);
	    });
    });
    db.collection('past').createIndex({date:1}, {unique:false}, function(){
	    db.collection('past').ensureIndex({date:1}, {unique:false}, function(err, indexName) {
	    	//console.log('Indexed', indexName);
	    });
    });
    db.collection('past').createIndex({var:1}, {unique:false}, function(){
	    db.collection('past').ensureIndex({var:1}, {unique:false}, function(err, indexName) {
	    	//console.log('Indexed', indexName);
	    });
    });
    
    db.collection('daily').createIndex({tag:1}, {unique:false}, function(){
	    db.collection('daily').ensureIndex({tag:1}, {unique:false}, function(err, indexName) {
	    	//console.log('Indexed', indexName);
	    });
    });
    db.collection('daily').createIndex({date:1}, {unique:false}, function(){
	    db.collection('daily').ensureIndex({date:1}, {unique:false}, function(err, indexName) {
	    	//console.log('Indexed', indexName);
	    });
    });
    db.collection('daily').createIndex({var:1}, {unique:false}, function(){
	    db.collection('daily').ensureIndex({var:1}, {unique:false}, function(err, indexName) {
	    	//console.log('Indexed', indexName);
	    });
    });
    
    db.collection('tags').createIndex({tag:1}, {unique:false}, function(){
	    db.collection('tags').ensureIndex({tag:1}, {unique:true}, function(err, indexName) {
	    	//console.log('Indexed', indexName);
	    });
    });
    db.collection('tags').createIndex({date:1}, { expireAfterSeconds: 3600*24 }, function(){
	    db.collection('tags').ensureIndex({date:1}, {unique:false}, function(err, indexName) {
	    	//console.log('Indexed', indexName);
	    });
    });
});

var log = { upload : {}, insert : {} };

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
    
    app.get('/mongo-api/get/:tag_list/:start_date/:end_date/:variable?', function (req, res) {
	    //MongoClient.connect(url, (err, db) => {
		    let db = global_db;
		    let start = new Date(req.params.start_date);
		    let variable = req.params.variable || 'PV';
		    start.setSeconds(0);
		    start.setMilliseconds(0);
		    //start = start.toISOString();
		    let end = new Date(req.params.end_date);
		    end.setSeconds(0);
		    end.setMilliseconds(0);
		    //end = end.toISOString();
	        db.collection("past").find({ 
			    tag: {
			        $in:req.params.tag_list.split(','),
			    }, 
			    var : variable,
			    date: {
			        $gte: start,
			        $lte: end
			    }
			}, {_id:0}).toArray(function(err, arr){
				db.collection("basic").find({ 
				    tag: {
				        $in:req.params.tag_list.split(',')
				    }, 
				    var : variable,
				    date: {
				        $gte: start,
				        $lte: end
				    }
				}, {_id:0}).toArray(function(err2, arr2){
					let data;
					let current = [];
					if(err || arr.length < 1){
						arr = [];
					}
					if(err2 || arr2.length < 1){
						arr2 = [];
					}
					
					req.params.tag_list.split(',').forEach(function(item){
						let keys;
						let final_obj = {
							data : {}
						};
						if(!current[item.tag]){
							current[item.tag] = [];
						}
						if(giant[item]){
							keys = Object.keys(giant[item].data);
							giant[item].tag = item;
							keys.forEach(function(key){
								let split_key = key.split(".");
								final_obj.date = giant[item].date;
								final_obj.tag = giant[item].tag;
								final_obj.var = giant[item].var;
								if(!final_obj.data[split_key[1]]){
									final_obj.data[split_key[1]] = {};
								}
								final_obj.data[split_key[1]][split_key[2]] = giant[item].data[key];
							});
							current.push(final_obj);
						}
					});
					res.json({data:arr.concat(arr2), current:current});
				});
			});
		//});
    });
    
    app.get('/mongo-api/getAllFromTagList/:tag_list/:start_date/:end_date', function (req, res) {
	    //MongoClient.connect(url, (err, db) => {
		    let db = global_db;
		    let start = new Date(req.params.start_date);
		    start.setSeconds(0);
		    start.setMilliseconds(0);
		    //start = start.toISOString();
		    let end = new Date(req.params.end_date);
		    end.setSeconds(0);
		    end.setMilliseconds(0);
		    //end = end.toISOString();
	        db.collection("past").find({ 
			    tag: {
			        $in:req.params.tag_list.split(','),
			    }, 
			    date: {
			        $gte: start,
			        $lte: end
			    }
			}, {_id:0}).toArray(function(err, arr){
				db.collection("basic").find({ 
				    tag: {
				        $in:req.params.tag_list.split(',')
				    }, 
				    date: {
				        $gte: start,
				        $lte: end
				    }
				}, {_id:0}).toArray(function(err2, arr2){
					let data;
					let current = [];
					if(err || arr.length < 1){
						arr = [];
					}
					if(err2 || arr2.length < 1){
						arr2 = [];
					}
					
					req.params.tag_list.split(',').forEach(function(item){
						let keys;
						let final_obj = {
							data : {}
						};
						if(!current[item.tag]){
							current[item.tag] = [];
						}
						if(giant[item]){
							keys = Object.keys(giant[item].data);
							giant[item].tag = item;
							keys.forEach(function(key){
								let split_key = key.split(".");
								final_obj.date = giant[item].date;
								final_obj.tag = giant[item].tag;
								final_obj.var = giant[item].var;
								if(!final_obj.data[split_key[1]]){
									final_obj.data[split_key[1]] = {};
								}
								final_obj.data[split_key[1]][split_key[2]] = giant[item].data[key];
							});
							current.push(final_obj);
						}
					});
					res.json({data:arr.concat(arr2), current:current});
				});
			});
		//});
    });
    
    app.get('/mongo-api/getTagsLike/:tags_like', function (req, res) {
	    let db = global_db;
	    if(req.params.tags_like && req.params.tags_like.length){
		    //MongoClient.connect(url, (err, db) => {
		        db.collection("tags").find({ 
				    tag: { '$regex' : req.params.tags_like, '$options' : 'i' }
				}, {tag:1, _id:0})
				.limit(30)
				.toArray(function(err, arr){
						res.send({data:arr});
				});
			//});
		}
		else{
			res.send({status:false, message:'No tag name received'});
		}
    });

    io.on('connection', function (socket_m) {
	    console.log("connected");
	    socket = socket_m;
	});
}

var giant = {};
var tags_set = {};
setInterval(function () {
    if (flag) {
        flag = false;
        tempArray = [];
        tempArray = globalArray.slice();
        globalArray = [];
        if (tempArray.length != 0) {
		    tempArray.forEach(function(item){
	            let date = {};
	            date.date = new Date(item.date);
	            date.year = date.date.getFullYear();
                date.month = date.date.getMonth();
				date.day = date.date.getDate();
				date.hours = date.date.getHours();
				date.minutes = date.date.getMinutes();
				date.seconds = date.date.getSeconds();
				let set={};
				if(!giant[item.tag]){
					giant[item.tag] = {};
					giant[item.tag].data = {};
				}
				tags_set[item.tag] = date.date;
				giant[item.tag].date = new Date('2017', date.month, date.day , date.hours );
				giant[item.tag].var = item.var;
				giant[item.tag].data['data'+'.'+date.minutes+'.'+date.seconds] = item.val;

            });
	        flag = true;	
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
function updateData(callback){
	MongoClient.connect(url, (err, db) => {
		var bulk = db.collection('basic').initializeUnorderedBulkOp();
		var local_giant = clone(giant);//JSON.parse(JSON.stringify(giant));
		giant = {};
		var keys = Object.keys(local_giant);
		var start = null;
		var finish = null;
		var bulk_available = false;
		start = new Date();
		console.log('Started upsert with ',keys.length, 'at second', start.getSeconds());		
		for(let i = 0; i< keys.length; i++){
			//set['value.'+hours+minutes+seconds+i]={date:new Date(), val:Math.random()};
			//console.log(local_giant[keys[i]])
	        bulk.find({
	            "tag": keys[i],
	            "date": local_giant[keys[i]].date,
	            "var": local_giant[keys[i]].var,
	        })
	        .upsert()
	        .update(
	        {
	            $set: local_giant[keys[i]].data
	        });
	        bulk_available = true;
	    }
	    if(bulk_available){
		    bulk.execute(function(err,res){
			    finish = new Date();
			    var time = ((finish.getTime() - start.getTime()) / 1000);
			    console.log('Finished in ',(finish.getTime() - start.getTime())/1000+'s');
			    log.insert.final_time = "Upsert operation took " + time;
                log.insert.rows_inserted = keys.length;
				log.insert.insertion_time = time + 's';
				log.insert.insertion_date = new Date();
				emit();
				callback(null,1);
				db.close();
				try {
                    //fs.appendFileSync(file, 'rows upserted,' + row_size + ',date,' + new Date() + ',time,' + time + '\n');
                } catch (e) {

                }
		    });
	    }
	    else{
		    console.log('No bulk operation available.');
		    callback(null,1);
	    }
    });
}
*/

function updateData(callback){
	var obj = {
		tag : "Positive_PV_Sender",
		var : "PV",
		date: "2017-07-08T00:00:00.000Z",
		data : {}
	}
	var data = {};
	for(var hour = 0; hour < 24; hour++){
		for(var i = 0; i<60;i++){
		    if(!data[hour]){
		        data[hour] = {};
		    }
		    for(var j = 0; j<60; j++){
		        if(!data[hour][i]){
		            data[hour][i] = {};
		        }
		        data[hour][i][j] = 1.234;
		    }
		}
	}
	obj.data = data;

	MongoClient.connect(url, (err, db) => {
		var bulk = db.collection('daily').initializeUnorderedBulkOp();
		var local_giant = clone(giant);//JSON.parse(JSON.stringify(giant));
		giant = {};
		var keys = Object.keys(local_giant);
		var start = null;
		var finish = null;
		var bulk_available = false;
		start = new Date();
		console.log('Started upsert with ',keys.length, 'at second', start.getSeconds());		
		for(let i = 0; i< keys.length; i++){
			//set['value.'+hours+minutes+seconds+i]={date:new Date(), val:Math.random()};
			//console.log(local_giant[keys[i]].data)
	        bulk.find({
	            "tag": keys[i],
	            "date": "2017-07-08T00:00:00.000Z",
	            "var": "PV",
	        })
	        .upsert()
	        .update(
	        {
	            $set: {"data.0.0":data["0"]["0"]}
	        });
/*
			let local_obj = clone(obj);
			local_obj.tag = local_obj.tag+i;
			bulk.insert(local_obj);
*/
	        bulk_available = true;
	        //console.log(keys[i],i)
	    }
	    if(bulk_available){
		    bulk.execute(function(err,res){
			    finish = new Date();
			    var time = ((finish.getTime() - start.getTime()) / 1000);
			    console.log('Finished in ',(finish.getTime() - start.getTime())/1000+'s');
			    log.insert.final_time = "Upsert operation took " + time;
                log.insert.rows_inserted = keys.length;
				log.insert.insertion_time = time + 's';
				log.insert.insertion_date = new Date();
				emit();
				callback(null,1);
				db.close();
				try {
                    //fs.appendFileSync(file, 'rows upserted,' + row_size + ',date,' + new Date() + ',time,' + time + '\n');
                } catch (e) {

                }
		    });
	    }
	    else{
		    console.log('No bulk operation available.');
		    callback(null,1);
	    }
    });
}

function updateTags(callback){
	MongoClient.connect(url, (err, db) => {
		var bulk = db.collection('tags').initializeUnorderedBulkOp();
		var local_tags_set = clone(tags_set);
		tags_set = {};
		var keys = Object.keys(local_tags_set);
		var start = null;
		var finish = null;
		var bulk_available = false;
		start = new Date();
		console.log('Started tags upsert with ',keys.length, 'at second', start.getSeconds());		
		for(let i = 0; i< keys.length; i++){
	        bulk.find({
	            "tag": keys[i],
	        })
	        .upsert()
	        .update(
	        {
	            $set: {"date": local_tags_set[keys[i]]}
	        });
	        bulk_available = true;
	    }
	    if(bulk_available){
		    bulk.execute(function(err,res){
			    finish = new Date();
			    console.log('Finished tags in ',(finish.getTime() - start.getTime())/1000+'s');
			    db.close();
			    callback(null,1);
		    });
	    }
	    else{
		    console.log('No bulk operation available.');
		    callback(null,1);
	    }
    });
}

var time_to_update = true;
var last_updated_minute = 0;
var first_time_to_update = true;
var busy = false;
setInterval(function(){
	var date = new Date();
	if(!busy){
		if(date.getSeconds()==0 || first_time_to_update || last_updated_minute != date.getMinutes()){
			busy = true;
			updateData(function(err, res){
				updateTags(function(err2, res2){
					time_to_update = false;
					first_time_to_update = false;
					last_updated_minute = date.getMinutes();
					busy = false;
				});
			});
		}
		else{
			time_to_update = true;
		}
	}
	else{
		//console.log('busy')
	}
}, 1000);


function move(){
	MongoClient.connect(url, (err, db) => {
		if(err) {
			console.log('error, moving again',err);
			move();
			return;
		}
		var date = new Date();
		var bulkInsert = db.collection('past').initializeUnorderedBulkOp()
		var bulkRemove = db.collection('basic').initializeUnorderedBulkOp()
		var x = 1000
		var counter = 0
		
		var start = null;
		var finish = null;
		start = new Date();
		
		date.setMonth(date.getMonth());
		date.setHours(date.getHours()-1);
	    date.setMinutes(0);
	    date.setSeconds(0);
	    date.setMilliseconds(0);
	    console.log('Moving files from basic to past using date',date);
	    db.collection('basic').find({date:{$lte: date}}).count(function(err,count){
			db.collection('basic').find({date:{$lte: date}}).forEach(function(doc){
			    var time;
				bulkInsert.insert(doc);
				bulkRemove.find({_id:doc._id}).removeOne();
				counter ++;
				if( counter % x == 0 || counter == count){
					bulkInsert.execute()
					bulkRemove.execute()
					bulkInsert = db.collection('past').initializeUnorderedBulkOp()
					bulkRemove = db.collection('basic').initializeUnorderedBulkOp()
				}
				if(counter == count){
					finish = new Date();
				    time = ((finish.getTime() - start.getTime()) / 1000);
				    console.log('Finished moving data to "past" collection in ',(finish.getTime() - start.getTime())/1000+'s');
				}
			});
	    });
		
	});
}

var time_to_move_data = true;
var first_time_moving_data = true;
setInterval(function(){
	var date = new Date();
	if(date.getMinutes()==0 || first_time_moving_data){
		if(!time_to_move_data){
			return;
		}
		move();
		time_to_move_data = false;
		first_time_moving_data = false;
	}
	else{
		time_to_move_data = true;
	}
}, 1000);

//create file in the path
function writeFile(path, contents, callback) {
    mkdirp(getDirName(path), function (err) {
        if (err) return console.log(err);
        fs.writeFile(path, contents, function (err) {
            callback(err);
        });
    });
}



module.exports = router;
