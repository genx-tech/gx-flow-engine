const path = require('path');
const childProcess = require('child_process');
const { eachAsync_, _, fs, getValueByPath, Promise, template } = require('rk-utils');
const { startQueueWorker: startWorker } = require('@genx/app');
const { Types: { DATETIME }, Generators, Convertors, Validators } = require('@genx/data');
const { Graph } = require('@genx/algorithm');

const SEC_MINUTES = 60;
const SEC_PER_HOUR = SEC_MINUTES * 60;
const SEC_PER_DAY = SEC_PER_HOUR * 24;
const SEC_PER_MONTH = SEC_PER_DAY * 30;

const JOB_INFO = 'job';
const TASK_INFO = 'task';
const JOB_PROCESS = 'process';
const TASK_ACTIVITY = 'activity';
const JOB_SCHEDULE = 'schedule';

const TaskTypes = new Set([ 
    'action', // in-process action
    'shell',  // shell command
    'queued', // queued action
    'query'   // mongo-style query
]);

function errorToJson(error) {
    return _.pick(error, ['name', 'message', 'code', 'info', 'stack']);
}

function pipelineTaskSyntax(graph, pipeline, lastStep) {
    function taskX(taskRef) {
        taskRef = taskRef.trim();

        let postFlow;

        if (taskRef.endsWith('|->')) {
            postFlow = 'map';
            taskRef = taskRef.slice(0, -3);
        }

        let [tx, version] = taskRef.split('@');
        taskRef = tx.trim();

        let preFlow;

        if (taskRef.startsWith('->')) {
            preFlow = 'map';
            taskRef = taskRef.substr(2).trimStart();
        } else if (taskRef.startsWith('+>')) {
            preFlow = 'merge';
            taskRef = taskRef.substr(2).trimStart();
        } else {
            preFlow = 'forward';
        }

        let [taskName, batchTag] = taskRef.split('#');

        let taskRefInfo = {
            id: taskRef,
            task: taskName,
            batch: batchTag,
            preFlow,
            postFlow
        };

        if (!_.isNil(version)) {
            taskRefInfo.version = version;
        }

        return taskRefInfo;
    }

    let callFromOuter = false;

    if (_.isNil(graph)) {
        graph = new Graph();
        callFromOuter = true;
    }

    pipeline.forEach(task => {
        let taskInfo = {};

        if (typeof task === 'string') { // [ '<task name>' ] 
            Object.assign(taskInfo, taskX(task));
        } else if (Array.isArray(task)) { // [ ..., [ '<task1 name>', '<task2 name>' ], ... ] 

            lastStep = task.map(subFlow => {
                let [_g, lastStepOfGraph] = pipelineTaskSyntax(graph, [subFlow], lastStep);
                return lastStepOfGraph;
            });

            return;

        } else { // [ ..., { [name: '<task name>'], flow: 'parallel', pipeline: *  }, ... ] 
            if (!_.isPlainObject(task) || (!task.task && !task.tx)) {
                throw new Error(`A task in Simple Workflow Syntax (SWS) should be either a string as the task name, an array as sub-flow or a task info object with "task" or "tx" included. Invalid task: ${JSON.stringify(task)}`);
            }

            taskInfo = _.cloneDeep(task);
            if (taskInfo.tx) {
                let tx = taskInfo.tx;
                delete taskInfo.tx;
                taskInfo = _.defaults(taskInfo, taskX(tx));
            }

            assert: taskInfo.task;

            taskInfo.id ??= _.isNil(taskInfo.batch) ? taskInfo.task : `${taskInfo.task}#${taskInfo.batch}`;
        }

        if (graph.hasNode(taskInfo.id)) {
            if (taskInfo.batch) {
                throw new Error(`The task name and batch combination [${task.id}] is duplicate.`);
            }

            taskInfo.batch = Generators.shortid();
            taskInfo.id = `${taskInfo.task || taskInfo.id}#${taskInfo.batch}`;
        }

        graph.setNode(taskInfo.id, taskInfo);

        if (Array.isArray(lastStep)) {
            lastStep.forEach(stepOfLastStep => {
                graph.setEdge(stepOfLastStep, taskInfo.id);
            });
        } else if (lastStep) {
            graph.setEdge(lastStep, taskInfo.id);
        }

        lastStep = taskInfo.id;
    });

    return callFromOuter ? graph : [graph, lastStep];
}

const mJobCache = {};
const mTaskCache = {};

class Engine {
    static startQueueWorker(configName, storeService, queueService, loggerService, workingPath, localWorkersPath, queueName, worker) {
        let pipeline;

        const init = app => {
            let workersPath = path.resolve(workingPath, localWorkersPath);
            pipeline = new Engine(app, storeService, queueService, loggerService, workersPath);
        };

        startWorker(workingPath, configName, queueService, queueName, async (app, msg) => {
            let { activityId, data, key } = msg;

            let activity = await pipeline.getActivity_(activityId);
            if (!activity) {
                throw new Error(`Activity [id=${activityId}] not found.`);
            }

            try {
                await pipeline.runActivityWorker_(worker, activity, data, key);
                pipeline.log('info', `Queue message [${activityId}] is done.`, { activity, key });
            } catch (error) {
                await pipeline.activityError_(activity, error);
            }

            return true;
        }, init);
    }

    static VAR(activity, varName, optional) {
        let value = activity.variables && activity.variables[varName];

        if (!optional && _.isNil(value)) {
            throw new Error(`Variable "${varName}" is required for activity [job=${activity.job}, taskRef=${activity.taskRef}].`);
        }

        return value;
    }

    constructor(app, storeService, queueService, loggerService, controllerPath) {
        this.app = app;
        this.store = app.getService(storeService);
        this.queue = app.getService(queueService);
        this.logger = app.getService(loggerService);
        this.controllerPath = controllerPath;
        this.tempPath = path.join(this.controllerPath, 'temp');
    }

    VAR = Engine.VAR;

    log = (level, message, ...rest) => {
        this.logger && this.logger.log(level, message, ...rest);
        return this;
    }

    logError = (error, message) => {
        return this.logger && this.logger.logError(error, message);
    }

    async initStore_() {
        //create JOB_INFO index
        await this.store.onCollection_(JOB_INFO, collection => collection.createIndexes([
            { key: { name: 1, version: 1 }, unique: true }
        ]));

        //create TASK_INFO index
        await this.store.onCollection_(TASK_INFO, collection => collection.createIndexes([
            { key: { name: 1, version: 1 }, unique: true }
        ]));

        //create JOB_SCHEDULE index
        await this.store.onCollection_(JOB_SCHEDULE, collection => collection.createIndexes([
            { key: { job: 1, schedule: 1, batch: 1 }, unique: true }
        ]));

        //create JOB_STATUS index
        await this.store.onCollection_(JOB_PROCESS, collection => collection.createIndexes([
            { key: { startedAt: 1 }, expireAfterSeconds: SEC_PER_MONTH }
        ]));

        //create TASK_STATUS index
        await this.store.onCollection_(TASK_ACTIVITY, collection => collection.createIndexes([
            { key: { processId: 1, taskRef: 1, dataKey: 1 }, unique: true },
            { key: { startedAt: 1 }, expireAfterSeconds: SEC_PER_MONTH }
        ]));

        /*
        this.defineJobs_([
            {
                name: '_built_in_timer',
                pipeline: [ '@_timed_task' ],
                priority: 0, 
                expiry: SEC_MINUTES*5
            }
        ]);
        */
    }

    async startBuiltinWorkers_() {
        const builtinQueues = ['_timed_task'];

        await eachAsync_(builtinQueues, queueName => this.queue.workerConsume_(queueName, (channel, msg) => {
            let info = JSON.parse(msg.content.toString());

            worker(app, info).then((shouldAck) => {
                if (shouldAck) {
                    channel.ack(msg);
                } else {
                    channel.nack(msg);
                }
            }).catch(error => {
                this.logError(error);

                if (error.needRetry) {
                    channel.nack(msg);
                } else {
                    channel.ack(msg);
                }
            });
        }));

    }

    async defineJobs_(jobsInfo) {
        jobsInfo = _.castArray(jobsInfo);

        let now = DATETIME.typeObject.local().toJSDate();

        await eachAsync_(jobsInfo, async info => {
            if (!info.name) {
                throw new Error('Job name is required while defining a job.');
            }

            if (!info.pipeline) {
                throw new Error('Job pipeline configuration is required while defining a job.');
            }

            let defaults = {};

            info.variables && (defaults.variables = Convertors.base64ToJson(info.variables));

            let pipeline = pipelineTaskSyntax(null, info.pipeline).calcStartEnd().toJSON();

            await this.setTasksVersion_(info.name, pipeline.nodes);

            let { modifiedCount, upsertedCount } = await this.store.upsertOne_(JOB_INFO, {
                _id: Generators.shortid(),
                priorityWeight: 0,
                expiry: SEC_MINUTES * 10,
                version: 1,
                ...defaults,
                ...info,
                pipeline: Convertors.jsonToBase64(pipeline)
            }, { name: info.name }, null, { createdAt: now });

            if (upsertedCount > 0) {
                this.log('info', `New job [name=${info.name}] is added.`);
            } else if (modifiedCount > 0) {
                this.log('info', `Job [name=${info.name}] is updated.`);
            } else {
                this.log('info', `Job [name=${info.name}] already exists and up to date.`);
            }
        });
    }

    async defineTasks_(tasksInfo) {
        tasksInfo = _.castArray(tasksInfo);

        let now = DATETIME.typeObject.local().toJSDate();

        await eachAsync_(tasksInfo, async info => {
            if (!info.name) {
                throw new Error('Task name is required while defining a task.');
            }

            info.type ?? (info.type = 'action');

            if (!TaskTypes.has(info.type)) {
                throw new Error(`Invalid task type: ${info.type}`);
            }
                        
            let defaults = {};

            info.variables && (defaults.variables = Convertors.base64ToJson(info.variables));

            let { modifiedCount, upsertedCount } = await this.store.upsertOne_(TASK_INFO, {
                _id: Generators.shortid(),
                expiry: SEC_MINUTES * 3,
                version: 1,
                ...defaults,
                ...info
            }, { name: info.name }, null, { createdAt: now });

            if (upsertedCount > 0) {
                this.log('info', `New task [name=${info.name}] is added.`);
            } else if (modifiedCount > 0) {
                this.log('info', `Task [name=${info.name}] is updated.`);
            } else {
                this.log('info', `Task [name=${info.name}] already exists and up to date.`);
            }
        });
    }

    async scheduleJobs(scheduleList) {
        scheduleList = _.castArray(scheduleList);

        let now = DATETIME.typeObject.local().toJSDate();

        await eachAsync_(scheduleList, scheduleRecord => store.insertOneIfNotExist_(JOB_SCHEDULE, { ...scheduleRecord, status: 'pending', createdAt: now }));
    }

    async getJobInfo_(jobName, version) {
        let ref = `${jobName}@${version}`;

        if (mJobCache[ref]) return mJobCache[ref];

        let jobInfo = mJobCache[ref] = await this.store.findOne_(JOB_INFO, { name: jobName, version });

        if (jobInfo) {
            jobInfo.pipeline = Convertors.base64ToJson(jobInfo.pipeline);
            jobInfo.variables && (jobInfo.variables = Convertors.base64ToJson(jobInfo.variables));
        }

        return jobInfo;
    }

    async getLatestJobs_(jobNames) {
        let jobs = await this.store.aggregate_(JOB_INFO, [
            { $match: { name: { $in: jobNames } } },
            { $sort: { name: 1, version: -1 } },
            { $group: { _id: "$name", version: { $max: "$version" }, job: { $first: "$$ROOT" } } }
        ]);
        return jobs && jobs.length > 0 && jobs.map(
            jobGroup => (jobGroup.job.pipeline = Convertors.base64ToJson(jobGroup.job.pipeline),
                jobGroup.job.variables && (jobGroup.job.variables = Convertors.base64ToJson(jobGroup.job.variables)),
                jobGroup.job));
    }

    async getTaskInfo_(taskName, version) {
        let ref = `${taskName}@${version}`;

        if (mTaskCache[ref]) return mTaskCache[ref];

        let taskInfo = mTaskCache[ref] = await this.store.findOne_(TASK_INFO, { name: taskName, version });
        taskInfo.variables && (taskInfo.variables = Convertors.base64ToJson(taskInfo.variables));
        return taskInfo;
    }

    async getLatestTasks_(taskNames) {
        let tasks = await this.store.aggregate_(TASK_INFO, [
            { $match: { name: { $in: taskNames } } },
            { $sort: { name: 1, version: -1 } },
            { $group: { _id: "$name", version: { $max: "$version" }, task: { $first: "$$ROOT" } } }
        ]);
        return tasks && tasks.length > 0 && tasks.map(
            taskGroup => (taskGroup.task.variables && (taskGroup.task.variables = Convertors.base64ToJson(taskGroup.task.variables)),
                taskGroup.task));
    }

    async getActivity_(activityId) {
        let activity = await this.store.findOne_(TASK_ACTIVITY, { _id: activityId });
        activity.variables && (activity.variables = Convertors.base64ToJson(activity.variables));
        return activity;
    }

    async getJobActivities_(processId, taskRefs) {
        let filters = { processId };

        if (!_.isNil(taskRefs)) {
            filters.taskRef = { $in: _.castArray(taskRefs) };
        }

        return await this.store.findAll_(TASK_ACTIVITY, filters);
    }

    async getJobProcess_(processId) {
        let jobProcess = await this.store.findOne_(JOB_PROCESS, { _id: processId });
        jobProcess.variables && (jobProcess.variables = Convertors.base64ToJson(jobProcess.variables));
        return jobProcess;
    }

    async getJobsTable_(jobReqs) {
        let jobs = _.castArray(jobReqs);

        //get all unique job names
        let jobSet = new Set();

        jobs = jobs.map(job => {
            if (typeof job === 'string') {
                jobSet.add(job);

                return {
                    name: job
                }
            } else if (Array.isArray(job)) {
                let [name, variables, data] = job;
                jobSet.add(job);

                return {
                    name,
                    variables,
                    data
                };
            }

            if (!job.name) {
                throw new Error('Missing job name');
            }

            jobSet.add(job.name);

            return job;
        });

        let reqJobNames = Array.from(jobSet);

        //get job definitions
        let jobsInfo = await this.getLatestJobs_(reqJobNames);

        if (_.isEmpty(jobsInfo)) {
            throw new Error(`No job found matching: ${JSON.stringify(reqJobNames)}`);
        }

        return [jobs, _.mapKeys(jobsInfo, (value) => value.name)];
    }

    async setTasksVersion_(jobName, tasks) {
        //get all unique task names
        let taskSet = new Set();

        _.each(tasks, taskRef => {
            if (_.isNil(taskRef.version)) { // skip those with version
                taskSet.add(taskRef.task);
            }
        });

        //get task definitions
        let tasksInfo = await this.getLatestTasks_(Array.from(taskSet));

        if (_.isEmpty(tasksInfo)) {
            throw new Error(`No task found matching: ${JSON.stringify(tasks)}`);
        }

        let tasksMap = _.mapKeys(tasksInfo, (value) => value.name);
        _.each(tasks, taskRef => {
            if (_.isNil(taskRef.version)) {
                let taskInfo = tasksMap[taskRef.task];
                if (!taskInfo) {
                    throw new Error(`Task [name=${taskRef.task}, job=${jobName}] not defined.`);
                }

                taskRef.version = taskInfo.version;
            }
        });
    }

    /*
    async startJobsBySchedule_(filters) {
        let now = DATETIME.typeObject.local().toJSDate();
    
        let jobsToRun = await this.store.updateManyAndReturn_(
            JOB_SCHEDULE, 
            { status: 'running', runningAt: now }, 
            {             
                ...filters,
                status: 'pending', 
                $or: [ 
                    { doneAt: { $exists: false } },
                    { minInterval: { $exists: false } },
                    { $expr: { $lt: [ { $add: ['$doneAt', '$minInterval' ] }, now ]  } } // or expired
                ] 
            }
        );
    
        if (jobsToRun && jobsToRun.length > 0) {
            await eachAsync_(jobsToRun, async jobSchedule => async this.start(engine, jobSchedule));
        }
    
        return jobsToRun;
    }*/

    /**
     * Start immediate jobs
     * @param {array} jobReqs - { name, variables, data }
     */
    async start_(jobReqs) {
        let [jobs, jobsTable] = await this.getJobsTable_(jobReqs);

        let result = await eachAsync_(jobs, async ({ name, variables, data, schedule }) => {
            try {
                let jobInfo = jobsTable[name];
                if (!jobInfo) {
                    throw new Error(`Job [name=${name}] not defined.`);
                }

                //create job process record
                let jobName = jobInfo.name;
                let processId = Generators.shortid();

                let now = DATETIME.typeObject.local().toJSDate();

                let jobProcess = {
                    _id: processId,
                    name: jobName,
                    version: jobInfo.version,
                    status: 'started',
                    startedAt: now
                };

                if (schedule) {
                    jobProcess.schedule = schedule;
                } else {
                    jobProcess.runOnce = true;
                }

                //store the information in base64 to avoid key conflict
                if (!_.isEmpty(variables)) {
                    jobProcess.variables = Convertors.jsonToBase64(variables);
                }

                await this.store.insertOne_(JOB_PROCESS, jobProcess);

                if (jobProcess.variables) {
                    jobProcess.variables = variables;
                }

                let startSteps = this.getNextTasks(jobInfo);

                let activities = await eachAsync_(startSteps, async taskRef => {
                    await this.store.updateOne_(JOB_PROCESS, { $inc: { [`pendingActivities.${taskRef}`]: 1 } }, { _id: processId });

                    return this.doTask_(jobInfo, jobProcess, taskRef, data, null, variables);
                });

                console.dir(jobInfo, { depth: 10 });

                return { status: 'started', processId, activities };
            } catch (error) {
                this.logError(error);

                return { status: 'error', error };
            }
        });

        return result;
    }

    getNextTasks(jobInfo, currentTaskRef) {
        if (currentTaskRef) {
            return jobInfo.pipeline.edges[currentTaskRef];
        } else {
            return jobInfo.pipeline.startNodes;
        }
    }

    getTaskRefInfo(jobInfo, taskRef) {
        return jobInfo.pipeline.nodes[taskRef];
    }

    getActivityKey(activity) {
        let key = `${activity._id}-${activity.job}-${activity.taskRef}`;
        return _.isNil(activity.dataKey) ? key : (key + '-' + activity.dataKey);
    }

    async doTask_(jobInfo, jobProcess, taskRef, data, key, variables) {
        let activityCreated;

        try {
            let job = jobInfo.name;

            if (!_.isPlainObject(jobProcess)) {
                jobProcess = await this.getJobProcess_(jobProcess);
            }

            //get task in job config
            let taskRefInfo = jobInfo.pipeline.nodes[taskRef];
            if (!taskRefInfo) {
                throw new Error(`Task reference [id=${taskRef}] not found in job [name=${job}].`);
            }

            if (taskRefInfo.test) {
                if (!Validators.validate({ data, key }, taskRefInfo.test)) {
                    //cannot pass test, don't need to proceed
                    let reason = 'condition_not_met';
                    await this.taskAbort_(jobProcess, taskRefInfo, reason);
                    return { status: 'aborted', reason };
                }
            }

            //get task info
            let taskInfo = await this.getTaskInfo_(taskRefInfo.task, taskRefInfo.version);
            if (!taskInfo) {
                throw new Error(`Task [name=${taskRefInfo.task}, version=${taskRefInfo.version}] not defined.`);
            }

            let activityId = Generators.shortid();

            let now = DATETIME.typeObject.local().toJSDate();

            let activityVariables = {
                ...jobInfo.variables,
                ...taskInfo.variables,
                ...taskRefInfo.variables,
                ...jobProcess.variables,
                ...variables
            };

            let activity = {
                _id: activityId,
                job,
                jobVersion: jobInfo.version,
                processId: jobProcess._id,
                task: taskRefInfo.task,
                taskVersion: taskInfo.version,
                taskRef: taskRefInfo.id,
                status: 'started',
                startedAt: now
            };

            if (!_.isNil(key)) {
                activity.dataKey = key;
            }

            if (!_.isEmpty(activityVariables)) {
                activity.variables = Convertors.jsonToBase64(activityVariables);
            }

            await this.store.insertOne_(TASK_ACTIVITY, activity);
            activityCreated = activity;

            if (activity.variables) {
                activity.variables = activityVariables;
            }

            try {
                switch (taskInfo.type) {
                    case 'queue':
                        let ret = await this.queue.sendToWorkers_(taskInfo.queue, {
                            activityId,
                            data,
                            key
                        });
                        if (!ret) {
                            throw new Error(`Failed to send the job [${ctx.job}] to the worker queue [${taskInfo.queue}].`);
                        }
                        break;

                    case 'action':
                        await this.doInnerAction_(activity, taskInfo, data, key);
                        break;

                    case 'shell':
                        await this.doShellCommand_(activity, taskInfo, data, key);
                        break;
                }
            } catch (error) {
                await this.activityError_(activity, error);
                throw error;
            }

            return { id: activityId, status: 'started' };
        } catch (error) {

            if (!activityCreated) {
                this.jobError_(jobProcess, error).catch(this.logError);
            }

            return { status: 'error', error: error };
        }
    }

    async doInnerAction_(activity, taskInfo, data, dataKey) {
        let workerFile = path.join(this.controllerPath, taskInfo.file);
        let worker = require(workerFile);

        if (!_.isNil(taskInfo.member)) {
            worker = worker[taskInfo.member];
        }

        if (typeof worker !== 'function') {
            throw new Error(`Failed to locate the action function of task [name=${activity.task}, job=${activity.job}].`);
        }

        await this.runActivityWorker_(worker, activity, data, dataKey);
    }

    async doShellCommand_(activity, taskInfo, data, dataKey) {
        let { command, args } = taskInfo;

        args = args.map(arg => template(arg, activity.variables));

        let options = { windowsHide: true, ..._.pick(taskInfo, ['cwd', 'env', 'gid', 'uid']) };

        await new Promise((resolve, reject) => {
            let ps = childProcess.spawn(command, args, options);
            ps.stdout.on('data', out => this.log('verbose', out));
            ps.stderr.on('data', error => this.log('warn', error));

            let e;

            ps.on('close', (code) => e ? reject(e) : resolve(code));
            ps.on('error', (error) => { e = error; });
        });

        return engine.done(activity, null, next);
    }

    async runActivityWorker_(worker, activity, data, dataKey) {
        let next;

        let jobInfo = await this.getJobInfo_(activity.job, activity.jobVersion);
        if (!jobInfo) {
            throw new Error(`Job [name=${activity.job}, version=${activity.jobVersion}] not found.`);
        }

        //get task reference info
        let taskRefInfo = this.getTaskRefInfo(jobInfo, activity.taskRef);
        if (!taskRefInfo) {
            throw new Error(`Task reference [job=${activity.job}, taskRef=${activity.taskRef}] not found.`);
        }

        if (taskRefInfo.postFlow) {
            let nextSteps = this.getNextTasks(jobInfo, activity.taskRef);
            if (!nextSteps || nextSteps.length === 0) return;

            if (taskRefInfo.postFlow === 'map') {
                next = async (nextRecords, variables) => {
                    this.log('verbose', 'Proceed to next step.', { nextTasks: nextSteps });

                    let size = _.size(nextRecords); // map to number of child task

                    await eachAsync_(nextSteps, taskRef => this.store.updateOne_(
                        JOB_PROCESS,
                        { $inc: { [`pendingActivities.${taskRef}`]: size } },
                        { _id: activity.processId }
                    ));

                    let canProceed = await this.finishActivity_(activity, variables);
                    if (canProceed) {
                        return eachAsync_(
                            nextRecords,
                            (record, key) => Promise.all(nextSteps.map(
                                taskRef => this.doTask_(jobInfo, activity.processId, taskRef, record, key.toString(), variables)
                            ))
                        );
                    }
                };
            } else if (taskRefInfo.postFlow === 'merge') {

            }
        }

        let [records, variables] = await worker(this, activity, data, next, dataKey);

        if (!next) {
            let canProceed = await this.finishActivity_(activity, variables);

            if (canProceed) {
                await this.proceed_(jobInfo, activity, records, dataKey, variables);
            }
        }
    }

    async proceed_(jobInfo, currentActivity, data, dataKey, variables) {
        let nextSteps = this.getNextTasks(jobInfo, currentActivity.taskRef);
        if (!nextSteps || nextSteps.length === 0) return;

        this.log('verbose', 'Proceed to next step.', { nextTasks: nextSteps });

        return eachAsync_(nextSteps, async taskRef => {
            await this.store.updateOne_(JOB_PROCESS, { $inc: { [`pendingActivities.${taskRef}`]: 1 } }, { _id: currentActivity.processId });

            return this.doTask_(jobInfo, currentActivity.processId, taskRef, data, dataKey, variables);
        });
    }

    done(activity, result, next, variables) {
        return next ? next(result, variables) : [result, variables];
    }

    async finishActivity_(activity, variables) {
        this.log('info', `Activity [id=${activity._id}, task=${activity.task}, job=${activity.job}] finished successfully.`);

        let now = DATETIME.typeObject.local().toJSDate();

        let activityUpdate = {
            status: 'success',
            endedAt: now
        };

        if (!_.isEmpty(variables)) {
            activityUpdate.variables = Convertors.jsonToBase64(variables);
        }

        await this.store.updateOne_(TASK_ACTIVITY, activityUpdate, { _id: activity._id });

        let jobStatus = await this.store.updateOneAndReturn_(JOB_PROCESS, { $inc: { [`succeededActivities.${activity.taskRef}`]: 1 } }, { _id: activity.processId });

        if (this.isAllPendingTasksFinished(jobStatus)) {
            let jobFinished = await this.isJobFinished_(jobStatus, activity.taskRef);
            if (jobFinished) {
                await this.store.updateOne_(JOB_PROCESS, { status: 'success', endedAt: now }, { _id: activity.processId });

                this.jobFinish_(jobStatus).catch(this.logError);

                return false; // don't proceed
            }
        }

        return true;
    }

    isAllPendingTasksFinished(jobStatus) {
        let pendindTaskRefs = jobStatus.pendingActivities;
        let totalPending = 0, totalEnded = 0;

        _.forOwn(pendindTaskRefs, (pending, taskRef) => {
            totalPending += pending;

            let succeeded = getValueByPath(jobStatus, `succeededActivities.${taskRef}`, 0);
            let failed = getValueByPath(jobStatus, `failedActivities.${taskRef}`, 0);
            let aborted = getValueByPath(jobStatus, `abortedActivities.${taskRef}`, 0);

            let ended = succeeded + failed + aborted;
            totalEnded += ended;
        });

        console.log(jobStatus.name, '------', '%', (totalEnded / totalPending).toFixed(4) * 100);

        if (totalEnded < totalPending) {
            return false;
        }

        if (totalEnded === totalPending) {
            return true;
        }

        this.log('warn', `Ended activities of task [job=${jobStatus.name}, taskRef=${taskRef}] exceeds pending activities.`, { totalPending, totalEnded });

        return true;
    }

    async isJobFinished_(jobProcess, taskRef) {
        let jobInfo = await this.getJobInfo_(jobProcess.name, jobProcess.version);

        let endTaskRefs = jobInfo.pipeline.endNodes;
        if (endTaskRefs.indexOf(taskRef) > -1) { // check only when the activity is one of the ended node
            let allEndActivities = await this.getJobActivities_(jobProcess._id, endTaskRefs);

            if (allEndActivities && allEndActivities.length === endTaskRefs.length) {
                return _.every(allEndActivities, endActivity => !_.isNil(endActivity.endedAt));
            }
        }

        return false;
    }

    async jobFinish_(jobStatus) {
        console.log(jobStatus);
    }

    async taskAbort_(jobProcess, taskRefInfo, reason) {
        this.log('info', `Task [job=${jobProcess.name}, process=${jobProcess._id}, taskRef=${taskRefInfo.id}] aborted due to "${reason}".`);

        let now = DATETIME.typeObject.local().toJSDate();

        let jobStatus = await this.store.updateOneAndReturn_(JOB_PROCESS, { $inc: { [`abortedActivities.${taskRefInfo.id}`]: 1 } }, { _id: jobProcess._id });

        if (this.isAllPendingTasksFinished(jobStatus)) {
            let jobFinished = await this.isJobFinished_(jobStatus, taskRefInfo.id);
            if (jobFinished) {
                await this.store.updateOne_(JOB_PROCESS, { status: 'error', endedAt: now, $push: { errorActivities: { id: activity._id, taskRef: activity.taskRef } } }, { _id: activity.processId });

                this.jobFinish_(jobStatus).catch(this.logError);
            }
        }
    }

    async activityError_(activity, error) {
        this.logError(error, `Activity [id=${activity._id}, task=${activity.task}, job=${activity.job}] failed.`);

        let now = DATETIME.typeObject.local().toJSDate();
        await this.store.updateOne_(TASK_ACTIVITY, { status: 'error', endedAt: now, error: errorToJson(error) }, { _id: activity._id });
        let jobStatus = await this.store.updateOneAndReturn_(JOB_PROCESS, { $inc: { [`failedActivities.${activity.taskRef}`]: 1 }, $push: { errorActivities: { id: activity._id, taskRef: activity.taskRef } } }, { _id: activity.processId });

        if (this.isAllPendingTasksFinished(jobStatus)) {
            let jobFinished = await this.isJobFinished_(jobStatus, activity.taskRef);
            if (jobFinished) {
                await this.store.updateOne_(JOB_PROCESS, { status: 'error', endedAt: now, $push: { errorActivities: { id: activity._id, taskRef: activity.taskRef } } }, { _id: activity.processId });

                this.jobFinish_(jobStatus).catch(this.logError);
            }
        }
    }

    async jobError_(jobProcess, error) {
        this.logError(error, `Job [id=${jobProcess._id}, name=${jobProcess.name}] failed.`);

        let now = DATETIME.typeObject.local().toJSDate();
        let statusUpdate = { status: 'error', endedAt: now };

        let jobStatus = await this.store.updateOneAndReturn_(JOB_PROCESS, statusUpdate, { _id: jobProcess._id });

        await this.jobFinish_(jobStatus);
    }
}

module.exports = Engine;
