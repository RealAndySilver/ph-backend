'use strict';

(function () {

    var socket = io();
    var $insertion = {
	    counter : $('.insertion_counter'), // div counter
		time : $('.insertion_time'),
		date : $('.insertion_date')
	};

    socket.on('getServerInfo', function (data) {
        setData(data)
    });

    function setData(data) {
	    console.log('Data',data);
        $insertion.counter.val(data.log.insert.rows_inserted);
        $insertion.time.val(data.log.insert.insertion_time);
        $insertion.date.val(new Date(data.log.insert.insertion_date));
    }

})();