const express = require('express');
var bodyParser = require("body-parser");
const app = express();
const server = require('http').Server(app);
const io = require('socket.io')(server);
const port = process.env.PORT || 3000;

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));



// collect request info
var requests = [];
var requestTrimThreshold = 5000;
var requestTrimSize = 4000;
app.use(express.static(__dirname + '/public'));
app.use(function (req, res, next) {
    requests.push(Date.now());
    console.log("request");

    // now keep requests array from growing forever
    if (requests.length > requestTrimThreshold) {
        requests = requests.slice(0, requests.length - requestTrimSize);
    }
    next();
});


app.get("/test", function (req, res) {
    var cnt = requests.length;
    res.json({ requestsLastMinute: cnt });
});

var mongo = require("./routes/mongo.js")(app);
var cassandra = require("./routes/cassandra.js")(app);

io.on('connection', function (socket) {
    console.log("connected");
    socket.on('counting requests', function () {
        console.log("counting...");
        socket.emit('counting requests', {
            numRequests: getNumRequests()
        });
    });
});

function getNumRequests() {
    var now = Date.now();
    var aMinuteAgo = now - (1000);
    var cnt = 0;
    // since recent requests are at the end of the array, search the array
    // from back to front
    for (var i = requests.length - 1; i >= 0; i--) {
        if (requests[i] >= aMinuteAgo) {
            ++cnt;
        } else {
            break;
        }
    }
    return cnt;
}

server.listen(port, () => console.log('listening on port ' + port));
