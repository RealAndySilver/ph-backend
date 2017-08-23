var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');
var path_1 = require('path');
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

var giant = {};
var giant_copy = {};
var tags_set = {};

var time_to_update = true;
var last_updated_minute = 0;
var first_time_to_update = true;
var busy = false;


//Create Indexes
MongoClient.connect(url, (err, db) => {
    if (err) return console.log(err)
    
/*
    db.collection('realtime').createIndex({date:1}, { expireAfterSeconds: 90 }, function(){
	    db.collection('realtime').ensureIndex({date:1}, {unique:false}, function(err, indexName) {
	    	//console.log('Indexed', indexName);
	    });
    });
*/

    db.collection('current').createIndex({date:1}, {unique: false, expireAfterSeconds: (60*60*2)+60*15 /*2 hours and 15 minutes*/}, function(){
	    db.collection('current').ensureIndex({date:1}, {unique:false}, function(err, indexName) {
	    	//console.log('Indexed', indexName);
	    });
    });

    db.collection('past').createIndex({date:1}, {unique:false}, function(){
	    db.collection('past').ensureIndex({date:1}, {unique:false}, function(err, indexName) {
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

var router = function (app, io, worker) {

    // mongo api insert n rows (with images & videos)
/*
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
            for (let i = 0; i < bigdata.length; i++) {
                globalArray.push(bigdata[i]);
            }
        }
    }); 
*/   
	app.post('/mongo-api/big-data', upload.array("upload_files"), function (req, res) {
        var files = req.files;
        var bigdata = req.body;
        var dir = '';
        var initial = '';
        var vr;
        var tag;
        var file_size;
        var date;
        var file_name = '';

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
                initial = path_1.join(__dirname+'/../data');
                dir = "/tags/" + tag + "/" + vr + "/" + file_size;
                file_name = "/" + files[i].originalname;
                start = new Date();
                writeFile(initial+dir+file_name, files[i].buffer, function (err) {
                    if (err) {
                        console.log('Err:', err);
                    }
                });
                addToGiant({tag: tag, val: './data'+dir+file_name , date: date, var: vr});
                res.send("ok");
            }
        } else {
            bigdata = req.body.bigdata;
            for (let i = 0; i < bigdata.length; i++) {
                addToGiant(bigdata[i]);
            }
			res.send("ok");
        }
    }); 
/*
	app.post('/mongo-api/big-data', upload.array("upload_files"), function (req, res) {
        var files = req.files;
        var bigdata = req.body;
        var dir = '';
        var vr;
        var tag;
        var file_size;
        var date;
        var file_path = '';

		MongoClient.connect(url, (err, db) => {
			var date = new Date();
			var bulkInsert = db.collection('realtime').initializeUnorderedBulkOp()			
			var start = null;
			var finish = null;
			start = new Date();
		    var time;
		    
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
	            let item;
	            let current_date;
	            console.log('Inserting '+bigdata.length+' realtime files',date);
	            for (var i = 0; i < bigdata.length; i++) {
		            item = bigdata[i];
		            current_date = new Date(item.date);
	                bulkInsert.insert({_id:item.tag+':'+item.var+':'+current_date.getMinutes()+'.'+current_date.getSeconds()+':'+item.val, date:current_date});
	                addToGiant(bigdata[i]);
	            }
				res.send("ok");
	        }
			
			
			bulkInsert.execute(function(err,res){
			    finish = new Date();
			    time = ((finish.getTime() - start.getTime()) / 1000);
			    console.log('Finished realtime in ',(finish.getTime() - start.getTime())/1000+'s');
				db.close();
		    });
		});
        
    });
*/

	app.get('/mongo-api/getCurrent/:tag_list/:variable?', function (req, res) {
		let data;
		let current = [];
		
		req.params.tag_list.split(',').forEach(function(item){
			let keys;
			let final_obj = {
				data : {}
			};
			if(!current[item.tag]){
				current[item.tag] = [];
			}
			if(giant_copy[item]){
				keys = Object.keys(giant_copy[item].data);
				giant_copy[item].tag = item;
				keys.forEach(function(key){
					let split_key = key.split(".");
					final_obj.date = giant_copy[item].date;
					final_obj.tag = giant_copy[item].tag;
					final_obj.var = giant_copy[item].var;
					if(!final_obj.data[split_key[1]]){
						final_obj.data[split_key[1]] = {};
					}
					final_obj.data[split_key[1]][split_key[2]] = giant_copy[item].data[key];
				});
				current.push(final_obj);
			}
		});
		res.json({status: true, current:current});
    });
	
    io.on('connection', function (socket_m) {
	    console.log("connected");
	    socket = socket_m;
	});
}


/*
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
				date.year_to_hours = new Date(date.year, date.month, date.day , date.hours );

				if(!giant[date.year_to_hours]){
					giant[date.year_to_hours] = {};
				}
				if(!giant[date.year_to_hours][item.tag]){
					giant[date.year_to_hours][item.tag] = {};
					giant[date.year_to_hours][item.tag].data = {};
				}
				
				tags_set[item.tag] = date.date;
				giant[date.year_to_hours][item.tag].data.date = date.year_to_hours ;
				giant[date.year_to_hours][item.tag].data.var = item.var;
				giant[date.year_to_hours][item.tag].data.tag = item.tag;
				giant[date.year_to_hours][item.tag].data['data'+'.'+date.minutes+'.'+date.seconds] = item.val;

				if(!giant_copy[item.tag]){
					giant_copy[item.tag] = {};
					giant_copy[item.tag].data = {};
				}
				giant_copy[item.tag].date = date.year_to_hours;
				giant_copy[item.tag].var = item.var;
				giant_copy[item.tag].data['data'+'.'+date.minutes+'.'+date.seconds] = item.val;
				
            });
	        flag = true;
        } 
        else {
            flag = true;
        }
    }
}, 1000);
*/

function addToGiant(item){
/*
	let date = {};
    date.date = new Date(item.date);
    date.year = date.date.getFullYear();
    date.month = date.date.getMonth();
	date.day = date.date.getDate();
	date.hours = date.date.getHours();
	date.minutes = date.date.getMinutes();
	date.seconds = date.date.getSeconds();
	date.year_to_hours = new Date(date.year, date.month, date.day , date.hours );

	if(!giant[date.year_to_hours]){
		giant[date.year_to_hours] = {};
	}
	if(!giant[date.year_to_hours][item.tag]){
		giant[date.year_to_hours][item.tag] = {};
		giant[date.year_to_hours][item.tag].data = {};
	}
	
	tags_set[item.tag] = date.date;
	giant[date.year_to_hours][item.tag].data.date = date.year_to_hours ;
	giant[date.year_to_hours][item.tag].data.var = item.var;
	giant[date.year_to_hours][item.tag].data.tag = item.tag;
	giant[date.year_to_hours][item.tag].data['data'+'.'+date.minutes+'.'+date.seconds] = item.val;
*/
	let date = new Date(item.date);
	let minutes = date.getMinutes();
	let seconds = date.getSeconds();
	date.setMinutes(0);
	date.setSeconds(0);
	date.setMilliseconds(0);

	if(!giant[date]){
		giant[date] = {};
	}
	if(!giant[date][item.tag]){
		giant[date][item.tag] = {};
		giant[date][item.tag].data = {};
	}
	
	tags_set[item.tag] = date.date;
	//giant[date][item.tag].data._id = item.tag+':'+item.var+':'+date.getFullYear()+''+date.getMonth()+''+date.getDate()+''+date.getHours() ;
	giant[date][item.tag].data.date = date ;
	giant[date][item.tag].data.var = item.var;
	//giant[date][item.tag].data.tag = item.tag;
	giant[date][item.tag].data['data'+'.'+minutes+'.'+seconds] = item.val;

/*
	if(!giant_copy[item.tag]){
		giant_copy[item.tag] = {};
		giant_copy[item.tag].data = {};
	}
	giant_copy[item.tag].date = date.year_to_hours;
	giant_copy[item.tag].var = item.var;
	giant_copy[item.tag].data['data'+'.'+date.minutes+'.'+date.seconds] = item.val;
*/
}

function clone(obj) {
    if (null == obj || "object" != typeof obj) return obj;
    var copy = obj.constructor();
    for (var attr in obj) {
        if (obj.hasOwnProperty(attr)) copy[attr] = obj[attr];
    }
    return copy;
}
function updateData(callback){
	MongoClient.connect(url, (err, db) => {
		if(err){
			updateData(callback);
			return;
		}
		var bulk = db.collection('current').initializeUnorderedBulkOp();
		var local_giant = clone(giant);//JSON.parse(JSON.stringify(giant));
		giant = {};
		var keys = Object.keys(local_giant);
		var start = null;
		var finish = null;
		var bulk_available = false;
		let key, sub_key, sub_keys, dt;
		start = new Date();
		console.log('Started upsert with ',keys.length, ' dates, at second', start.getSeconds());		
		
		for(let i = 0; i< keys.length; i++){
			let sub_keys = Object.keys(local_giant[keys[i]]);
			console.log('Starting '+sub_keys.length+' upserts with date',keys[i], ' at second', start.getSeconds());
			for(let j = 0; j< sub_keys.length; j++){
				let set_object = local_giant[keys[i]][sub_keys[j]].data;
				let dt = set_object.date;
				set_object.tag = sub_keys[j];
		        bulk.find({
		            "_id": sub_keys[j]+':'+set_object.var+':'+dt.getFullYear()+''+dt.getMonth()+''+dt.getDate()+''+dt.getHours(),
		            //"tag": sub_keys[j],
		            //"date": local_giant[keys[i]][sub_keys[j]].data.date,
		            //"var": local_giant[keys[i]][sub_keys[j]].var,
		        })
		        .upsert()
		        .update(
		        {
		            $set: set_object
		        });
		        bulk_available = true;
	        }
	    }
/*
		for(let i = 0; i< keys.length; i++){
			key = keys[i];
			sub_keys = Object.keys(local_giant[key]);
			console.log('Starting '+sub_keys.length+' upserts with date',key, ' at second', start.getSeconds());
			for(let j = 0; j< sub_keys.length; j++){
				sub_key = sub_keys[j];
				dt = local_giant[key][sub_key].data.date;
				
		        bulk.find({
		            "_id": sub_key+':'+local_giant[key][sub_key].data.var+':'+dt.getFullYear()+''+dt.getMonth()+''+dt.getDate()+''+dt.getHours(),
		            //"tag": sub_keys[j],
		            //"date": local_giant[key][sub_key].data.date,
		            //"var": local_giant[keys[i]][sub_keys[j]].var,
		        })
		        .upsert()
		        .update(
		        {
		            $set: local_giant[key][sub_key].data
		        });
		        bulk_available = true;
	        }
	    }
*/
	    
	    if(bulk_available){
		    bulk.execute(function(err,res){
			    finish = new Date();
			    var time = ((finish.getTime() - start.getTime()) / 1000);
			    console.log('Finished in ',(finish.getTime() - start.getTime())/1000+'s');
			    giant_copy = {};
			    
/*
			    log.insert.final_time = "Upsert operation took " + time;
                log.insert.rows_inserted = keys.length;
				log.insert.insertion_time = time + 's';
				log.insert.insertion_date = new Date();
				emit();
*/
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
		if(err){
			updateTags(callback);
			return;
		}
		var bulk = db.collection('tags').initializeUnorderedBulkOp();
		var local_tags_set = clone(tags_set);
		tags_set = {};
		var keys = Object.keys(local_tags_set);
		var start = null;
		var finish = null;
		var bulk_available = false;
		let key;
		start = new Date();
		console.log('Started tags upsert with ',keys.length, 'at second', start.getSeconds());		
		for(let i = 0; i< keys.length; i++){
			key = keys[i];
	        bulk.find({
		        "_id": key,
	        })
	        .upsert()
	        .update(
	        {
	            $set: {"date": local_tags_set[key]}
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

if(process.env.type=="single_thread"){
	setInterval(function(){
		updateData(function(err, res){
				updateTags(function(err2, res2){
	
			});
	
		});
	}, 10000);
}
else{
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



module.exports = router;