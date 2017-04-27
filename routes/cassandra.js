var assert = require('assert');
const cassandra = require('cassandra-driver');
const client = new cassandra.Client({ contactPoints: ['127.0.0.1'] });
const id = cassandra.types.Uuid.random();

var start, finish;
var max_array_size = 50000;


var router = function (app) {

    /* GET home page. */
    app.get('/cassandra-api', function (req, res, next) {
        res.send("Cassandra");
    });

    // cassandra api insert n rows
    app.post('/cassandra-api/data', function (req, res) {
        client.connect()
            .then(function () {
                var size = 1;
                if (!req.body.txt || !req.body.val || !req.body.date) {
                    return res.status(400).json({ message: 'Please fill out all fields' });
                }
                var query = 'INSERT INTO examples.basic (id, txt, val, date) VALUES (?, ?, ?, ?)';
                start = new Date();
                console.log("executing my stuff");
                client.execute(query, [cassandra.types.Uuid.random(), req.body.txt, req.body.val, req.body.date], { prepare: true }, function (err) {
                    assert.ifError(err);
                });

                finish = new Date();
                var time = (finish.getTime() - start.getTime()) + " ms";
                console.log("Operation took " + time);
                return res.send({ "Number of rows inserted": size, "Time spent": time });
            })
            .catch(function (err) {
                console.error('There was an error when connecting', err);
                return client.shutdown();
            });
    });

    app.post('/cassandra-api/big-data', function (req, res) {
        client.connect()
            .then(function () {

                if (!req.body.bigdata) {
                    return res.status(400).json({ message: 'no data to insert' });
                }
                var bigdata = req.body.bigdata;
                //if (typeof bigdata == Array) {
                if (bigdata.length != 0) {
                    /*for (var i = 0; i < bigdata.length; i++) {
                        globalArray.push(bigdata[i]);
                    }*/
                    var query = 'INSERT INTO examples.basic (id, txt, val, date) VALUES (?, ?, ?, ?)';
                    var queries = [];
                    for (var num = 1; num <= bigdata.length; num++) {
                        queries.push({ "query": query, "params": [cassandra.types.Uuid.random(), bigdata.txt, bigdata.val, bigdata.date] });
                    }

                    start = new Date();
                    console.log("executing my stuff");
                    console.log("rows to insert: ", queries.length)
                    var chunks = split(queries, max_array_size);
                    console.log('chunk size: ', chunks.length);
                    insertChunks(client, chunks, function () {
                        finish = new Date();
                        console.log("Operation took " + (finish.getTime() - start.getTime()) + " ms");
                    });
                }
                res.send("ok");


            })
            .catch(function (err) {
                console.error('There was an error when connecting', err);
                return client.shutdown();
            });
    })
}

var insertChunks = function (client, chunks, callback) {
    for (var i = 0; i < chunks.length; i++) {
        client.batch(chunks[i], { prepare: true }, function (err) {
            assert.ifError(err);
        });
    }
    callback();
}

function split(array, groupsize) {
    var sets = [], chunks, i = 0;
    chunks = array.length / groupsize;

    while (i < chunks) {
        //console.log(i);
        sets[i] = array.splice(0, groupsize);
        i++;
    }

    return sets;
};

module.exports = router;
