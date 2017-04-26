var express = require("express");
var bodyParser = require("body-parser");
var app = express();

 
app.use(bodyParser.json({limit: '50mb'}));
app.use(bodyParser.urlencoded({limit: '50mb', extended: true }));

var mongo = require("./routes/mongo.js")(app);
var cassandra = require("./routes/cassandra.js")(app);


module.exports = app;
