var assert = require('assert');
const cassandra = require('cassandra-driver');
const client = new cassandra.Client({ contactPoints: ['127.0.0.1'] });
const id = cassandra.types.Uuid.random();
var size = 100000;
var start, finish;


var router = function (app) {

    /* GET home page. */
    app.get('/cassandra-api', function (req, res, next) {
        res.send("Cassandra");
    });

    // cassandra api insert n rows
    app.post('/cassandra-api/data', function (req, res) {
        client.connect()
            .then(function () {
                var query = 'INSERT INTO examples.basic (id, txt, val, date) VALUES (?, ?, ?, ?)';
                start = new Date();
                console.log("executing my stuff");
                for (var num = 1; num <= size; num++) {
                    client.execute(query, [cassandra.types.Uuid.random(), 'Hello!', num, new Date()], { prepare: true }, function (err) {
                        //console.log("WDF");
                        assert.ifError(err);
                    });
                }
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
}
module.exports = router;
