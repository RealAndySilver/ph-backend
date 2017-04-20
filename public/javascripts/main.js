'use strict';

(function () {

    var socket = io();
    var $counter = $('.counter'); // div counter

    var countRequests = function() {
        socket.emit('counting requests');
    };
    setInterval(countRequests, 1000); // call every second

    socket.on('counting requests', function (data) {
        onCountingRequests(data)
    });

    function onCountingRequests(data) {
        $counter.val(data.numRequests);
    }

})();