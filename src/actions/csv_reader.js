const path = require('path');
const { Validators } = require('@genx/data');
const { _, getValueByPath } = require('rk-utils');
const Engine = require('../Engine');
const { EP_STORE, MESSAGE_QUEUE, LOGGER } = require('../enum/strings');
const { WORKING_PATH, SOURCE_PATH } = require('../enum/paths');
const { Utils: { parseCsvFile } } = require('@genx/data');

const queueName = 'jq_' + path.basename(__filename, '.js');

Engine.startQueueWorker('worker', EP_STORE, MESSAGE_QUEUE, LOGGER, WORKING_PATH, SOURCE_PATH, queueName, 
    async (engine, activity, data, next) => {        
        let file = engine.VAR(activity, 'file');
        let options = engine.VAR(activity, 'options', true) ?? {};

        let result = await parseCsvFile(file, options);
        return engine.done(activity, result, next);
    });