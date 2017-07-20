const express = require('express');
var path = require('path');
var bodyParser = require("body-parser");
const app = express();
const server = require('http').Server(app);
const io = require('socket.io')(server);
var port = 3000;
var writer_route = "./writer.js";
if(process.env.type == "balanced"){
	port = process.env.PORT || 3100;
	writer_route = "./writer_for_balancer.js"
}

var allowCrossDomain = function(req, res, next) {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,UNLOCK,PURGE');
    res.header(	'Access-Control-Allow-Headers', 
    			'Content-Type , content-type, Authorization, Content-Length, X-Requested-With, type, token,Cache-Control,If-Modified-Since,if-modified-since, pragma');
    if ('OPTIONS' == req.method) {
      res.sendStatus(200);
    }
    else{
	  next();  
    }
}

app.use(allowCrossDomain);

app.use(bodyParser.json({limit: '50mb'}));
app.use(bodyParser.urlencoded({limit: '50mb', extended: true }));
app.use(express.static(__dirname + '/public'));
app.use('/data', express.static(__dirname + '/data'));


var writer = require(writer_route)(app,io);

server.listen(port, () => console.log('listening on port ' + port));

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
