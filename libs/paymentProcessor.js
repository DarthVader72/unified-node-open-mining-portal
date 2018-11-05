'use strict';

var fs = require('fs');

var redis = require('redis');
var async = require('async');
const Redlock = require('redlock');

var Stratum = require('@energicryptocurrency/merged-pool');
var util = require('@energicryptocurrency/merged-pool/lib/util.js');
const utils = require('./utils');


module.exports = function(logger){

    var poolConfigs = JSON.parse(process.env.pools);

    var enabledPools = [];
    
    process.on('message', function(message) {
        switch(message.type){
            case 'reloadpool':
                if (message.coin) {
                    var messageCoin = message.coin.toLowerCase();
                    var poolTarget = Object.keys(poolConfigs).filter(function(p){
                        return p.toLowerCase() === messageCoin;
                    })[0];
                    poolConfigs  = JSON.parse(message.pools);
                    if (addPoolIfEnabled(messageCoin))
                        setupPools([messageCoin]);
                }
                break;
        }
    });

    var addPoolIfEnabled = function(c) {
        var poolOptions = poolConfigs[c];
        if (poolOptions.paymentProcessing && poolOptions.paymentProcessing.enabled) {
            enabledPools.push(c);
            return true;
        } else {
            return false;
        }
    }

    var setupPools = function(enPools) {
        async.filter(enPools, function(coin, callback){
            SetupForPool(logger, poolConfigs[coin], function(setupResults){
                callback(setupResults);
            });
        }, function(res){
            if (!res) {
                logger.error("Failed to setup pools");
                return;
            }
            enPools.forEach(function(coin){

                var poolOptions = poolConfigs[coin];
                var processingConfig = poolOptions.paymentProcessing;
                var logSystem = 'Payments';
                var logComponent = coin;

                logger.debug(logSystem, logComponent, 'Payment processing setup to run every '
                    + processingConfig.paymentInterval + ' second(s) with daemon ('
                    + processingConfig.daemon.user + '@' + processingConfig.daemon.host + ':' + processingConfig.daemon.port
                    + ') and redis (' + poolOptions.redis.host + ':' + poolOptions.redis.port + ')');

            });
        });
    }

    Object.keys(poolConfigs).forEach(function(coin) {
        addPoolIfEnabled(coin);
    });

    setupPools(enabledPools);

};

function SetupForPool(logger, poolOptions, setupFinished){


    const coin = poolOptions.coin.name;
    const coinSymbol = poolOptions.coin.symbol;
    const processingConfig = poolOptions.paymentProcessing;

    const logSystem = 'Payments';
    const logComponent = coin;

    var daemon = new Stratum.daemon.interface([processingConfig.daemon], function(severity, message){
        logger[severity](logSystem, logComponent, message);
    });
    const redisClient = redis.createClient(poolOptions.redis.port, poolOptions.redis.host);
	//redisClient.auth(poolOptions.redis.password);
	redisClient.select(poolOptions.redis.db);
    utils.redisKeepalive(redisClient);
    const redlock = new Redlock([redisClient]);
    const lock_name = 'payment:lock';

    let magnitude;
    let magnitude_length;
    let minPaymentSatoshis;
    let coinPrecision;
    let paymentInterval;
    
    const {minimumPayment, roundingAdjust = 0.999} = processingConfig;
    let poolAccountName = null;

    async.parallel([
        function(callback){
            daemon.cmd('validateaddress', [poolOptions.address], function(result) {
                if (result.error){
                    logger.error(logSystem, logComponent, 'Error with payment processing daemon ' + JSON.stringify(result.error));
                    callback(true);
                }
                else if (!result.response || !result.response.ismine) {
                    logger.fatal(logSystem, logComponent,
                            'Daemon does not own pool address - payment processing can not be done with this daemon, '
                            + JSON.stringify(result.response));
                    callback(true);
                }
                else{
                    poolAccountName = result.response.account;
                    callback()
                }
            }, true);
        },
        function(callback){
            daemon.cmd('getbalance', [], function(result){
                if (result.error){
                    callback(true);
                    return;
                }
                try {
                    var d = result.data.split('result":')[1].split(',')[0].split('.')[1];
                    magnitude = parseInt('10' + new Array(d.length).join('0'));
                    coinPrecision = magnitude.toString().length - 1;
                    callback();
                }
                catch(e){
                    logger.error(logSystem, logComponent, 'Error detecting number of satoshis in a coin, cannot do payment processing. Tried parsing: ' + result.data + ' error ' + e);
                    callback(true);
                }

            }, true, true);
        }
    ], function(err){
        if (err){
            setupFinished(false);
            return;
        }
        paymentInterval = setInterval(function(){
            try {
                processPayments();
            } catch(e){
                throw e;
            }
        }, processingConfig.paymentInterval * 1000);
        setTimeout(processPayments, 100);
        setupFinished(true);
    });




    var satoshisToCoins = function(satoshis){
        return parseFloat((satoshis / magnitude).toFixed(coinPrecision));
    };

    var coinsToSatoshies = function(coins){
        return coins * magnitude;
    };

    /* Deal with numbers in smallest possible units (satoshis) as much as possible. This greatly helps with accuracy
       when rounding and whatnot. When we are storing numbers for only humans to see, store in whole coin units. */

    var processPayments = function(){

        var startPaymentProcess = Date.now();

        var timeSpentRPC = 0;
        var timeSpentRedis = 0;

        var startTimeRedis;
        var startTimeRPC;

        var startRedisTimer = function(){ startTimeRedis = Date.now() };
        var endRedisTimer = function(){ timeSpentRedis += Date.now() - startTimeRedis };

        var startRPCTimer = function(){ startTimeRPC = Date.now(); };
        var endRPCTimer = function(){ timeSpentRPC += Date.now() - startTimeRedis };
        let lock_held = null;

        async.waterfall([
            function(callback){
                // Reserve enough time
                redlock.lock(lock_name, processingConfig.paymentInterval * 1000 * 10)
                    .then((lock) => {
                        lock_held = lock;
                        callback(null);
                    })
                    .catch((err) => {
                        logger.error(
                                logSystem, logComponent,
                                'Failed to acquire redis lock: ' + JSON.stringify(err));
                        callback(err);
                    });
            },

            /* Call redis to get an array of rounds - which are coinbase transactions and block heights from submitted
               blocks. */
            function(callback){

                startRedisTimer();
                redisClient.multi([
                    ['smembers', coin + ':blocksPending']
                ]).exec(function(error, results){
                    endRedisTimer();

                    if (error){
                        logger.error(logSystem, logComponent, 'Could not get blocks from redis ' + JSON.stringify(error));
                        callback(true);
                        return;
                    }

                    var rounds = results[0].map(function(r){
                        var details = r.split(':');
                        return {
                            blockHash: details[0],
                            txHash: details[1],
                            height: details[2],
                            serialized: r
                        };
                    });

                    callback(null, rounds);
                });
            },

            /* Does a batch rpc call to daemon with all the transaction hashes to see if they are confirmed yet.
               It also adds the block reward amount to the round object - which the daemon gives also gives us. */
            function(rounds, callback){
                var batchRPCcommand = rounds.map(function(r){
                    return ['gettransaction', [r.txHash]];
                });

                startRPCTimer();
                daemon.batchCmd(batchRPCcommand, function(error, txDetails){
                    endRPCTimer();

                    if (error || !txDetails){
                        logger.error(logSystem, logComponent, 'Check finished - daemon rpc error with batch gettransactions '
                            + JSON.stringify(error));
                        callback(true);
                        return;
                    }

                    txDetails.forEach(function(tx, i){
                        var round = rounds[i];

                        if (tx.error && tx.error.code === -5){
                            logger.error(logSystem, logComponent, 'Daemon reports invalid transaction: ' + round.txHash);
                            round.category = 'kicked';
                            return;
                        }
                        else if (tx.error || tx.result == null){
                            logger.error(logSystem, logComponent, 'Odd error with gettransaction ' + round.txHash + ' '
                                + JSON.stringify(tx));
                            return;
                        }
                        else if (tx.result.details == null || (tx.result.details && tx.result.details.length === 0)){
                            logger.debug(logSystem, logComponent, 'Daemon reports no details for transaction: ' + round.txHash);
                            round.category = 'kicked';
                            return;
                        }

                        var generationTx = tx.result.details.filter(function(tx){
                            return tx.address === poolOptions.address;
                        })[0];


                        /*if (!generationTx && tx.result.details.length === 1){
                            generationTx = tx.result.details[0];
                        }*/

                        if (!generationTx){
                            logger.error(logSystem, logComponent, 'Missing output details to pool address for transaction '
                                + round.txHash);
                            return;
                        }

                        round.category = generationTx.category;
                        if (round.category === 'generate') {
                            round.reward = generationTx.amount || generationTx.value;
                        }

                    });

                    var canDeleteShares = function(r){
                        for (var i = 0; i < rounds.length; i++){
                            var compareR = rounds[i];
                            if ((compareR.height === r.height)
                                && (compareR.category !== 'kicked')
                                && (compareR.category !== 'orphan')
                                && (compareR.serialized !== r.serialized)){
                                return false;
                            }
                        }
                        return true;
                    };


                    //Filter out all rounds that are immature (not confirmed or orphaned yet)
                    rounds = rounds.filter(function(r){
                        switch (r.category) {
                            case 'orphan':
                            case 'kicked':
                                r.canDeleteShares = canDeleteShares(r);
                            case 'generate':
                                return true;
                            default:
                                return false;
                        }
                    });


                    callback(null, rounds);

                });
            },


            /* Does a batch redis call to get shares contributed to each round. Then calculates the reward
               amount owned to each miner for each round. */
            function(rounds, callback){
                var shareLookups = rounds.map(function(r){
                    return ['hgetall', coin + ':shares:round' + r.height]
                });

                startRedisTimer();
                redisClient.multi(shareLookups).exec(function(error, allWorkerShares){
                    endRedisTimer();

                    if (error){
                        callback('Check finished - redis error with multi get rounds share');
                        return;
                    }

                    const workers = {};
                    
                    rounds.forEach(function(round, i){
                        const workerShares = allWorkerShares[i];

                        if (!workerShares){
                            logger.error(logSystem, logComponent, 'No worker shares for round: '
                                + round.height + ' blockHash: ' + round.blockHash);
                            return;
                        }

                        switch (round.category){
                            case 'kicked':
                            case 'orphan':
                                round.workerShares = workerShares;
                                break;

                            case 'generate':
                                /* We found a confirmed block! Now get the reward for it and calculate how much
                                   we owe each miner based on the shares they submitted during that block round. */
                                const round_reward = parseInt(round.reward * magnitude);

                                var totalShares = Object.keys(workerShares).reduce(function(p, c){
                                    return p + parseFloat(workerShares[c])
                                }, 0);

                                for (let wname in workerShares){
                                    const percent = parseFloat(workerShares[wname]) / totalShares;
                                    const workerRewardTotal = Math.floor(round_reward * percent);
                                    const worker = workers[wname] = (workers[wname] || {});
                                    worker.satoshi_reward = (worker.satoshi_reward || 0) + workerRewardTotal;
                                }
                                break;
                        }
                    });

                    callback(null, rounds, workers);
                });
            },

            // Database operations
            function(rounds, workers, callback){

                const balanceUpdateCommands = [];

                for (let wname in workers) {
                    balanceUpdateCommands.push([
                        'hincrbyfloat',
                        coin + ':balances',
                        wname,
                        satoshisToCoins(workers[wname].satoshi_reward)
                    ]);
                }

                const updateBlockStatCommands = [];
                const movePendingCommands = [];
                const roundsToDelete = [];
                const orphanMergeCommands = [];

                const moveSharesToCurrent = function(r){
                    const workerShares = r.workerShares;
                    
                    for (const wname in workerShares) {
                        orphanMergeCommands.push([
                            'hincrby',
                            coin + ':shares:roundCurrent',
                            wname,
                            workerShares[wname]]);
                    }
                };

                rounds.forEach(function(r){

                    updateBlockStatCommands.push(['hset', 'Allblocks', coinSymbol +"-"+ r.height, r.serialized + ":" + r.category]); // hashgoal addition for update block stats for all coins
                });
                rounds.forEach(function(r){
                        switch(r.category){
                            case 'kicked':
                                movePendingCommands.push(['smove', coin + ':blocksPending', coin + ':blocksOrphaned', r.serialized]);
                            case 'orphan':
                                movePendingCommands.push(['smove', coin + ':blocksPending', coin + ':blocksOrphaned', r.serialized]);
                                if (r.canDeleteShares){
                                    moveSharesToCurrent(r);
                                    roundsToDelete.push(coin + ':shares:round' + r.height);
                                }
                                return;
                            case 'generate':
                                movePendingCommands.push(['smove', coin + ':blocksPending', coin + ':blocksConfirmed', r.serialized]);
                                roundsToDelete.push(coin + ':shares:round' + r.height);
                                return;
                        }
                });

                var finalRedisCommands = [];

                if (movePendingCommands.length > 0)
                    finalRedisCommands = finalRedisCommands.concat(movePendingCommands);

                if (orphanMergeCommands.length > 0)
                    finalRedisCommands = finalRedisCommands.concat(orphanMergeCommands);

                if (balanceUpdateCommands.length > 0)
                    finalRedisCommands = finalRedisCommands.concat(balanceUpdateCommands);

                if (updateBlockStatCommands.length > 0)
                    finalRedisCommands = finalRedisCommands.concat(updateBlockStatCommands);

                if (roundsToDelete.length > 0)
                    finalRedisCommands.push(['del'].concat(roundsToDelete));

                if (finalRedisCommands.length === 0){
                    callback();
                    return;
                }
                
                // Make sure to execute atomic way
                balanceUpdateCommands.unshift(['MULTI']);
                balanceUpdateCommands.push(['EXEC']);

                startRedisTimer();
                redisClient.multi(finalRedisCommands).exec(function(error, results){
                    endRedisTimer();
                    if (error){
                        logger.error(logSystem, logComponent,
                                'Reward processing failed at Redis. It can be safely repeated. ' + JSON.stringify(error));
                        callback(true);
                    } else {
                        logger.info(logSystem, logComponent,
                                     `Processed ${rounds.length} rounds for later payout.`);
                        callback(null);
                    }
                    
                });
            },

            // Process  actual payouts
            function(callback) {
                startRedisTimer();
                redisClient.multi([
                    ['hgetall', coin + ':balances'],
                    ['incr', coin + ':payout_counter'],
                ]).exec(function(error, results){
                    endRedisTimer();

                    if (error){
                        logger.error(logSystem, logComponent,
                                     'Could not get balances from redis ' + JSON.stringify(error));
                        callback(true);
                        return;
                    }
                    
                    let payout_counter = results[1];
                    const worker_payouts = {};
                    const balances = results[0];
                    let total_payout = 0;

                    for (let wname in balances) {
                        if (!utils.isValidWorker(wname)) {
                            continue;
                        }

                        const amount = parseFloat(balances[wname]);
                        
                        if (amount >= minimumPayment) {
                            worker_payouts[wname] = amount;
                        }
                        
                        if (amount > 0) {
                            total_payout += amount;
                        }
                    }
                    
                    const process_payout = () => {
                        const payout_workers = Object.keys(worker_payouts);
                        
                        if (payout_workers.length <= 0) {
                            callback();
                            return;
                        }
                        
                        if (payout_workers.length > 100) {
                            // Truncate
                            payout_workers.length = 100;
                        }
                        
                        const payout_file = `${coin}:payout_file:${payout_counter}`;
                        const payout_batch = {};
                        const redis_batch = [
                            ['MULTI'],
                        ];
                        
                        for (let i = payout_workers.length - 1; i >= 0; --i) {
                            const wname = payout_workers[i];
                            const address = utils.worker2address(wname);
                            const amount = worker_payouts[wname];

                            payout_batch[address] = amount + (payout_batch[address] || 0);
                            delete worker_payouts[wname];
                            
                            redis_batch.push([
                                'hincrbyfloat',
                                `${coin}:balances`,
                                wname,
                                -amount.toFixed(coinPrecision)
                            ]);
                        }
                        
                        for (let address in payout_batch) {
                            const full_amount = payout_batch[address];
                            // Workaround possible minor rounding errors
                            const safe_amount = full_amount * roundingAdjust;
                            payout_batch[address] = safe_amount.toFixed(coinPrecision);

                            redis_batch.push([
                                'hset',
                                payout_file,
                                address,
                                safe_amount
                            ]);                            
                        }
                        
                        redis_batch.push(['set', coin + ':payout_counter', ++payout_counter]);
                        redis_batch.push(['EXEC']);
                        
                        startRedisTimer();
                        redisClient.multi(redis_batch).exec(function(error, results){
                            endRedisTimer();

                            if (error){
                                logger.error(logSystem, logComponent,
                                            'Could not create payment batch in redis ' + JSON.stringify(error));
                                callback(true);
                                return;
                            }
                            
                            const sendmany_args = [
                                poolAccountName, // fromaccount
                                payout_batch,    // amounts
                                1,               // minconf
                                false,           // addlockconf
                                "",              // comment
                                Object.keys(payout_batch) // subtractfeefromamount
                            ];
                            
                            logger.info(logSystem, logComponent,
                                        `Distributing ${payout_file}: ` + JSON.stringify(payout_batch));
                            logger.debug(logSystem, logComponent,
                                        `"sendmany": ` + JSON.stringify(sendmany_args));
                            
                            startRPCTimer();
                            daemon.cmd('sendmany', sendmany_args, function(results){
                                endRPCTimer();
                                
                                const result = results[0];
                                
                                if (result.error !== null) {
                                    logger.error(logSystem, logComponent,
                                                'Error with "sendmany" payment processing daemon ' + JSON.stringify(result.error));
                                    callback(true);
                                    return;
                                }
                                
                                const redis_batch = [
                                    ['MULTI'],
                                    ['del', payout_file],
                                ];
                                
                                for (let address in payout_batch) {
                                    const amount = payout_batch[address];
                                    redis_batch.push([
                                        'hincrbyfloat',
                                        `${coin}:payouts`,
                                        address,
                                        amount
                                    ]);
                                }
                                
                                redis_batch.push(['EXEC']);
 
                                startRedisTimer();
                                redisClient.multi(redis_batch).exec(function(error, results){
                                    endRedisTimer();

                                    if (error){
                                        logger.error(logSystem, logComponent,
                                                    'Could not save payouts in redis ' + JSON.stringify(error));
                                        callback(true);
                                        return;
                                    }
                                    
                                    process_payout();
                                });
                            });
                        });
                    };

                    // Validate that we can pay everyone
                    startRPCTimer();
                    daemon.cmd('getbalance', [poolAccountName], function(results){
                        endRPCTimer();
                        const result = results[0];
                        
                        if (result.error !== null) {
                            logger.error(logSystem, logComponent,
                                         'Error with payment processing daemon ' + JSON.stringify(result.error));
                            callback(true);
                            return;
                        }
                        
                        const balance = parseFloat(result.response);
                        
                        if (balance < total_payout) {
                            logger.error(logSystem, logComponent,
                                         `Refusing payout: insufficient funds ${balance} < ${total_payout}`);
                            callback(true);
                            return;
                        }

                        process_payout();
                    });
                });
            },
        ], function(){
            if (!lock_held) {
                return;
            }

            redlock.unlock(lock_held).catch(() => {});
            lock_held = null;

            var paymentProcessTime = Date.now() - startPaymentProcess;
            logger.debug(logSystem, logComponent, 'Finished interval - time spent: '
                + paymentProcessTime + 'ms total, ' + timeSpentRedis + 'ms redis, '
                + timeSpentRPC + 'ms daemon RPC');

        });
    };
}
