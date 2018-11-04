'use strict';

exports.redisKeepalive = function(client) {
    const timer = setInterval(() => client.ping(), 1000 * 10);
    client.on('end', () => clearInterval(timer));
};
