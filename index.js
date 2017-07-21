
var cluster = require('cluster');
var path = require('path');
var bodyParser = require("body-parser");
const port = process.env.PORT || 3000;

if(process.env.type=="single_thread"){
	console.log('Running in single thread mode.');
	const express = require('express');
	const app = express();
	const server = require('http').Server(app);
	const io = require('socket.io')(server);
	app.use(bodyParser.json({limit: '50mb'}));
	var writer = require("./writer.js")(app,io);
	server.listen(port, () => console.log('listening on port ' + port));
}
else{
	// Code to run if we're in the master process
	
	if (cluster.isMaster) {
	
	    // Count the machine's CPUs
	    var cpuCount = require('os').cpus().length;
		console.log('Running clustering mode with',cpuCount,'cores.');
	    // Create a worker for each CPU
	    for (var i = 0; i < cpuCount; i += 1) {
	        cluster.fork();
	    }
	
	    // Listen for dying workers
	    cluster.on('exit', function (worker) {	
	        console.log('Worker %d died ', worker.id);
	        cluster.fork();
	    });
	
	// Code to run if we're in a worker process
	} else {
		const express = require('express');
		const app = express();
		const server = require('http').Server(app);
		const io = require('socket.io')(server);
		app.use(bodyParser.json({limit: '50mb'}));	
		var writer = require("./writer.js")(app, io, cluster.worker);
		server.listen(port, () => console.log('listening on port ' + port));
	    console.log('Worker %d running!', cluster.worker.id);
	}
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////
//******************************* IMPORTANT ******* IMPORTANT *******************************************//
///////////////////////////////////////////////////////////////////////////////////////////////////////////
/////// Garbage collector, server need to be started with the command ' node --expose-gc ./bin/www '///////
/////// This command has been added to the package.json scripts and can be started as ' npm start ' ///////
///////////// This manual garbage collection helps to reduce the footprint significantly //////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
//******************************* IMPORTANT ******* IMPORTANT *******************************************//
///////////////////////////////////////////////////////////////////////////////////////////////////////////
setInterval(function(){
  //Garbage collection every 5 seconds.
  global.gc();
}, 1000*5);
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////// End of Garbage Collection ///////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
