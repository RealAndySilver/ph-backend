const express = require('express');
var bodyParser = require("body-parser");
const app = express();
const server = require('http').Server(app);
const io = require('socket.io')(server);
const port = process.env.PORT || 3000;

app.use(bodyParser.json({limit: '50mb'}));
app.use(bodyParser.urlencoded({limit: '50mb', extended: true }));
app.use(express.static(__dirname + '/public'));
var mongo = require("./routes/mongo.js")(app,io);
var cassandra = require("./routes/cassandra.js")(app);

server.listen(port, () => console.log('listening on port ' + port));
