const { _, fs, template } = require('rk-utils');

exports.pickOneField = (engine, activity, data, next) => engine.done(activity, _.map(data, record => record[engine.VAR(activity, 'field')]), next);

exports.unique = (engine, activity, data, next) => engine.done(activity, _.uniq(data), next);

exports.saveToFile = async (engine, activity, data, next, key) => {
    let filePath = engine.VAR(activity, 'file');

    filePath = template(filePath, { variables: activity.variables, key });

    await fs.outputFile(filePath, typeof data === 'string' ? data : JSON.stringify(data), 'utf8');

    return engine.done(activity, filePath, next);
}