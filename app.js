var express = require("express");
var bodyParser = require("body-parser");
var app = express();

 
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser({limit: '50mb'})); 

var mongo = require("./routes/mongo.js")(app);
var cassandra = require("./routes/cassandra.js")(app);


module.exports = app;
