var assert = require('assert');
const cassandra = require('cassandra-driver');
const client = new cassandra.Client({ contactPoints: ['127.0.0.1'] });
const id = cassandra.types.Uuid.random();
var fs = require('fs');

var start, finish;
var max_array_size = 10000;
var queries = [];
var tempQueries = [];
var flag = true;

var file = './public/cassa_average.csv';

var last_val = 0;


var router = function (app) {

    /* GET home page. */
    app.get('/cassandra-api', function (req, res, next) {
        res.send("Cassandra");
    });

    // cassandra api insert a row
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

    //cassandra api insert n rows
    app.post('/cassandra-api/big-data', function (req, res) {
        client.connect()
            .then(function () {
                if (!req.body.bigdata) {
                    return res.status(400).json({ message: 'no data to insert' });
                }
                var bigdata = req.body.bigdata;
                //if (typeof bigdata == Array) {
                if (bigdata.length != 0) {
                    var query = 'INSERT INTO examples.basic (id, txt, val, date) VALUES (?, ?, ?, ?)';
                    for (var num = 0; num < bigdata.length; num++) {
                        queries.push({ "query": query, "params": [cassandra.types.Uuid.random(), bigdata[num].txt, bigdata[num].val, bigdata[num].date] });
                    }
                    //last_val = bigdata[num - 1].val;
                    res.send("ok");
                }
            })
            .catch(function (err) {
                console.error('There was an error when connecting', err);
                return client.shutdown();
            });
    });

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

function batchInsert(client, tempQueries, callback) {
    client.batch(tempQueries, { prepare: true }, function (err) {
        assert.ifError(err);
    });
    callback();
}

setInterval(function () {
    if (flag) {
        flag = false;
        tempQueries = [];
        //console.log("globalArray ",globalArray[0]);
        tempQueries = queries.slice();
        queries = [];

        //console.log("tempArray ", tempArray[0]);
        if (tempQueries.length != 0) {
            //console.log("tempArray1: ", tempArray);
            start = new Date();
            console.log("executing my stuff");
            var row_size = tempQueries.length;
            console.log("rows to insert: ", row_size)
            var chunks = split(tempQueries, max_array_size);
            insertChunks(client, chunks, function () {
            //batchInsert(client, tempQueries, function () {
                //var query = 'SELECT id, date, txt, val FROM examples.basic WHERE val = ? ALLOW FILTERING';
                var query = 'SELECT count(*) FROM examples.basic ';
                //console.log("last_val", last_val);
                /*client.execute(query, { prepare: true }, function (err, result) {
                    //console.log("result ", err || result);
                    var row = result.first();
                    console.log('Obtained row: ', row);
                });*/
                flag = true;
                console.log("success!!");
                finish = new Date();
                var time = (finish.getTime() - start.getTime());
                try {
                    fs.appendFileSync(file, 'rows inserted,' + row_size + ',date,' + new Date() + ',time,' + time + '\n');
                } catch (e) {

                }
                console.log("Operation took " + time);
            });
        } else {
            flag = true;
        }

    }
}, 1000);

module.exports = router;
