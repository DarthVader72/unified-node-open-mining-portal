'use strict';

const WORKER_REGEX = /^([a-km-zA-HJ-NP-Z1-9]{34})(.[a-zA-Z0-9]+)?$/;

module.exports = exports = {
    redisKeepalive : function(client) {
        const timer = setInterval(() => client.ping(), 1000 * 10);
        client.on('end', () => clearInterval(timer));
    },
    redisPostConnect : function(client, {db, password = null }) {
        if (password && password !== '') {
            client.auth(password);
        }

        client.select(db);

        this.redisKeepalive(client);
    },
    isValidWorker : function(name) {
        return name.match(WORKER_REGEX) != null;
    },
    worker2address : function(name) {
        return name.replace(WORKER_REGEX, '$1');
    },
};

Object.freeze(exports);


