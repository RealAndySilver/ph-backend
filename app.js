var express = require("express");
var bodyParser = require("body-parser");
var app = express();

 
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
 
var mongo = require("./routes/mongo.js")(app);


module.exports = app;
