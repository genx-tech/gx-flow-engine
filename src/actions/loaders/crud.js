const { _ } = require('rk-utils');
const { EP_STORE } = require('../../enum/strings');

exports.upsertOne = async (engine, activity, record, next) => {
    if (_.isNil(record)) {
        throw new Error('Empty record.');
    }

    let collection = engine.VAR(activity, 'collection');
    let keys = engine.VAR(activity, 'keys');

    let epStore = engine.app.getService(EP_STORE); 

    let ret = await epStore.upsertOne_(collection, record, _.pick(record, keys));

    return engine.done(activity, ret, next);
};

exports.upsertMany = async (engine, activity, records, next) => {
    if (_.isNil(records) | _.isEmpty(records)) {
        throw new Error('Empty records.');
    }

    let collection = engine.VAR(activity, 'collection');
    let keys = engine.VAR(activity, 'keys');

    let epStore = engine.app.getService(EP_STORE); 

    let ret = await epStore.upsertMany_(collection, records, keys);

    return engine.done(activity, ret, next);
};