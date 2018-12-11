'use strict';

var mysql = require('mysql');
var cluster = require('cluster');
module.exports = function(logger, poolConfig){

    const mposConfig = poolConfig.mposMode;
    const coin = poolConfig.coin.name;

    const connection = mysql.createPool({
        host: mposConfig.host,
        port: mposConfig.port,
        user: mposConfig.user,
        password: mposConfig.password,
        database: mposConfig.database,
        connectionLimit:  mposConfig.connectionLimit,
    });
    
    const {
        anonymousMode,
        queueShares,
    } = mposConfig;
    
    const CHUNK_MAX = 100;
    const CHUNK_INTERVAL = 1e3;
    const all_queues = [];
    let curr_queue = [];

    const logIdentify = 'MySQL';
    const logComponent = coin;

    const ensureWorkerFormat = (workerName) => {
        if (workerName.split('.').length == 1) {
            return `${workerName}.default`;
        }
        
        return workerName;
    };

    this.handleAuth = function(workerName, password, authCallback){

        if (poolConfig.validateWorkerUsername !== true && mposConfig.autoCreateWorker !== true){
            authCallback(true);
            return;
        }

        connection.query(
            'SELECT password FROM pool_worker WHERE username = LOWER(?)',
            [workerName.toLowerCase()],
            function(err, result){
                if (err){
                    logger.error(logIdentify, logComponent, 'Database error when authenticating worker: ' +
                        JSON.stringify(err));
                    authCallback(false);
                }
                else if (!result[0]){
                    if(mposConfig.autoCreateWorker){
                        var account = workerName.split('.')[0];
                        connection.query(
                            'SELECT id,username FROM accounts WHERE username = LOWER(?)',
                            [account.toLowerCase()],
                            function(err, result){
                                if (err){
                                    logger.error(logIdentify, logComponent, 'Database error when authenticating account: ' +
                                        JSON.stringify(err));
                                    authCallback(false);
                                }else if(!result[0]){
                                    authCallback(false);
                                }else{
                                    connection.query(
                                        "INSERT INTO `pool_worker` (`account_id`, `username`, `password`) VALUES (?, ?, ?);",
                                        [result[0].id,workerName.toLowerCase(),password],
                                        function(err, result){
                                            if (err){
                                                logger.error(logIdentify, logComponent, 'Database error when insert worker: ' +
                                                    JSON.stringify(err));
                                                authCallback(false);
                                            }else {
                                                authCallback(true);
                                            }
                                        })
                                }
                            }
                        );
                    }
                    else{
                        authCallback(false);
                    }
                }
                else if (mposConfig.checkPassword &&  result[0].password !== password)
                    authCallback(false);
                else
                    authCallback(true);
            }
        );

    };

    this.handleShare = function(isValidShare, isValidBlock, shareData){
        var dbData = [
            shareData.ip,
            ensureWorkerFormat(shareData.worker),
            isValidShare ? 'Y' : 'N',
            isValidBlock ? 'Y' : 'N',
            shareData.difficulty * (poolConfig.coin.mposDiffMultiplier || 1),
            typeof(shareData.error) === 'undefined' ? null : shareData.error,
            shareData.blockHash ? shareData.blockHash : (shareData.blockHashInvalid ? shareData.blockHashInvalid : ''),
            shareData.height || 0,
        ];
        
        if (queueShares) {
            const d = new Date();
            dbData.unshift(d.toISOString().split('.')[0]);

            curr_queue.push(dbData);

            if (curr_queue.length >= CHUNK_MAX) {
                all_queues.push(curr_queue);
                curr_queue = [];
            }
        } else {
            connection.query(
                'INSERT INTO `shares` SET time = NOW(), rem_host = ?, username = ?, our_result = ?, upstream_result = ?, difficulty = ?, reason = ?, solution = ?, height = ?',
                dbData,
                function(err, result) {
                    if (err)
                        logger.error(logIdentify, logComponent, 'Insert error when adding share: ' + JSON.stringify(err));
                    else
                        logger.debug(logIdentify, logComponent, 'Share inserted');
                }
            );
        }
    };

    this.handleDifficultyUpdate = function(workerName, diff){
        if (!workerName) {
            return;
        }

        if (queueShares || anonymousMode) {
            // MPOS does not really use it, but disable only for the new modes
            return;
        }

        workerName = ensureWorkerFormat(workerName);

        connection.query(
            'UPDATE `pool_worker` SET `difficulty` = ' + diff + ' WHERE `username` = ?',
            [workerName],
            function(err, result){
                if (err)
                    logger.error(logIdentify, logComponent, 'Error when updating worker diff: ' +
                        JSON.stringify(err));
                else if (result.affectedRows === 0){
                    connection.query('INSERT INTO `pool_worker` SET ?', {username: workerName, difficulty: diff});
                }
                else {
                    logger.debug(logIdentify, logComponent, 'Updated difficulty successfully');
                }
            }
        );
    };

    if (queueShares) {
        let in_progress = false;
        let force_next_time = false;
        
        const process_queue = () => {
            //logger.debug(logIdentify, logComponent,
            //             `all_queues = ${JSON.stringify(all_queues)} curr_queue = ${JSON.stringify(curr_queue)}`);
            //logger.debug(logIdentify, logComponent,
            //             `ShareQueue pending ${all_queues.length} current length ${curr_queue.length}`);

            if (in_progress) {
                return;
            }

            if (all_queues.length == 0) {
                if (!force_next_time) {
                    force_next_time = true;
                    return;
                } else if (curr_queue.length == 0) {
                    force_next_time = false;
                    return;
                }

                all_queues.push(curr_queue);
                curr_queue = [];
            }

            force_next_time = false;
            in_progress = true;
            
            const process_item = () => {
                if (all_queues.length == 0) {
                    in_progress = false;
                    return;
                }

                const item = all_queues[0];
                const q = 'INSERT INTO `shares`(time, rem_host, username, our_result, upstream_result, difficulty, reason, solution, height) VALUES ' +
                    item.map(r => {
                        r = r.map(v => mysql.escape(v)).join(',');
                        return`(${r})`;
                    }).join(',');
                    
                //logger.debug(logIdentify, logComponent, `ShareQuery: ${q}`);

                connection.query(
                    q,
                    (err, result) => {
                        if (err) {
                            logger.error(logIdentify, logComponent,
                                         `Insert error when adding shares, retrying: ${JSON.stringify(err)}`);
                        } else {
                            logger.debug(logIdentify, logComponent,
                                         `Shares inserted: ${item.length}`);
                            all_queues.shift();
                        }

                        process_item();
                    }
                );
            };

            process_item();
        };
        
        setInterval(process_queue, CHUNK_INTERVAL);
    }
};
