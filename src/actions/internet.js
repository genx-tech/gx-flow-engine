
const UserAgent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36';


exports.httpGet = async (engine, activity, data, next) => {        
    let url = engine.VAR(activity, 'url');
    let query = engine.VAR(activity, 'query', true) ?? {};
    let urlSign = engine.VAR(activity, 'urlSign', true);    
    let debug = engine.VAR(activity, 'debug', true);    
    
    if (urlSign) {
        const sign = require(`../lib/urlSign/${urlSign}`);
        query = sign(engine, activity, query);
    }

    let res = await request.get(url).set('user-agent', UserAgent).query(query);

    if (debug) {
        let contentType = res.type;

        if (contentType === 'text/html' || contentType === 'text/plain') {
            await fs.outputFile(path.join(engine.tempPath, engine.getActivityKey(activity) + '.html'), res.text);
        } else {
            await fs.outputJson(path.join(engine.tempPath, engine.getActivityKey(activity) + '.json'), res.body);
        }            
    }

    if (res.status === 200) {        
        let result = res.body;                
        
        let validationSchema = engine.VAR(activity, 'validation', true);

        if (_.isNil(validationSchema) || Validators.validate(result, validationSchema)) {
            let dataPath = engine.VAR(activity, 'dataPath', true);
            if (!_.isNil(dataPath)) {
                result = getValueByPath(result, dataPath);
            }

            return engine.done(activity, result, next);
        }

        throw new Error('Invalid fetch result.');
    }

    throw new Error('Failed to fetch. HTTP status: ' + res.status);
};