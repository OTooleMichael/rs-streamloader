import { EventEmitter } from 'events';
import assert from 'assert';
import { DateTime } from 'luxon';
import { v4 } from 'uuid';
import AWS from 'aws-sdk';
import pg from 'pg';
import { transactionQuery } from './core';
import { createDefaults, mergeOptions, parseRsTables, createCopyCredString, LoaderError } from './utils';
import { CopySettings, FactoryOptions, PoolLike, RSLoaderOptions, UploadBody, UploadType } from './types';
function createRunTime(format: string = 'yyyyLLdd_HHmmss'): string {
  return DateTime.utc().toFormat(format);
}
interface UploadTask {
  body: UploadBody;
  started: Date;
  i: number;
  key: string;
  managedUpload: AWS.S3.ManagedUpload;
  uploaded: boolean;
  error?: Error;
}
export default class RedshiftLoader extends EventEmitter {
  defaults: FactoryOptions;
  uploadTasks: UploadTask[];
  uploadPromises: any[];
  uploadComplete: boolean;
  _started: boolean;
  jobTime: string;
  S3: AWS.S3;
  table: { schema: string; table: string };
  loadingTable: { schema: string; table: string };
  bucket: string;
  AWS_CREDS: string;
  _debuggingMode: boolean;
  rsPool: PoolLike;
  s3Cleanup: 'ALWAYS' | 'NEVER' | 'SUCCESS';
  manifestKey?: string;
  uploadType?: UploadType;
  constructor(options: RSLoaderOptions) {
    super();
    assert(options, 'Options Required');
    // inits

    this.defaults = mergeOptions(options, createDefaults()) as FactoryOptions;
    this.s3Cleanup =
      this.defaults.s3Cleanup === true
        ? 'SUCCESS'
        : this.defaults.s3Cleanup === false
        ? 'NEVER'
        : this.defaults.s3Cleanup;
    this.uploadTasks = [];
    this.uploadPromises = [];
    this.uploadComplete = false;
    this._started = false;
    this.jobTime = createRunTime();
    this.table = parseRsTables(options.table);
    this.loadingTable = parseRsTables(options.loadingTable || options.table);
    if (!this.loadingTable.table) {
      this.loadingTable.table = this.table.table;
    }
    if (!this.loadingTable.schema) {
      this.loadingTable.schema = this.table.schema;
    }
    const tempBodies = options.body ? [options.body] : options.bodies || []; //
    const bucket = options.bucket || this.defaults.bucket;
    assert(bucket, 'Options.bucket Required');
    this.bucket = bucket;
    this._debuggingMode = options.debug || false;
    let aws = this.defaults.aws;
    const isAWSEnvVars = !!process.env.AWS_ACCESS_KEY_ID;
    if (!aws && isAWSEnvVars) {
      // reduce friction in the AWS env. no neded to provide the options.aws
      // http://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/loading-node-credentials-environment.html
      aws = {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      };
    }
    assert(aws, 'AWS CREDS must be set (options.aws) or be env vars (process.env.AWS_ACCESS_KEY_ID)');
    this.AWS_CREDS = createCopyCredString(aws);
    const { s3Object, s3Settings } = options;
    if (s3Object) {
      assert(s3Object instanceof AWS.S3, 's3Object must be of type AWS.S3');
      this.S3 = s3Object; // provide your own
    } else {
      const credentials = (s3Settings?.credentials || isAWSEnvVars
        ? {
            accessKeyId: process.env.AWS_ACCESS_KEY_ID,
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
          }
        : aws) as AWS.Credentials;
      assert(credentials.accessKeyId && credentials.secretAccessKey, 'AWS keys Required');
      this.S3 = new AWS.S3({
        apiVersion: '2006-03-01',
        ...s3Settings,
        credentials,
      });
    }
    if (this.defaults.redshiftPool) {
      this.rsPool = this.defaults.redshiftPool;
      return this;
    }
    pg.defaults.poolIdleTimeout = 600000;
    this.rsPool = new pg.Pool(this.defaults.redshiftCreds);
    this.debug('Reshift Pool Created');
    assert(tempBodies instanceof Array, 'options.bodies must be an Array');
    this.addFiles(tempBodies);
  }
  debug(...args: any[]) {
    if (!this._debuggingMode) return;
    console.log.apply(null, args);
  }
  getFilePrefix() {
    const { filePrefix } = this.defaults;
    if (typeof filePrefix === 'function') {
      return filePrefix(this);
    }
    return filePrefix;
  }
  addFile(uploadBody: UploadBody) {
    // http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3/ManagedUpload.html
    if (this._started) {
      const e = new Error('RS_LOAD_STARTED:  Cannot add more files after RS load has started');
      console.warn(e);
      console.warn(e.stack);
      return;
    }

    const { bucket } = this;
    const i = this.uploadTasks.length;
    const filePrefix = this.getFilePrefix();
    const key = `${filePrefix}${this.jobTime}_prt_${i}_${v4().replace(/-/g, '')}.txt`;
    const managedUpload = this.S3.upload({
      Key: key,
      Body: uploadBody,
      Bucket: bucket,
    });
    console.log(managedUpload,this.S3.upload )
    const task: UploadTask = {
      body: uploadBody,
      key,
      started: new Date(),
      i,
      managedUpload,
      uploaded: false,
    };
    this.uploadTasks.push(task);

    const uploadPromise = new Promise<UploadTask>((resolve, reject) => {
      this.debug('S3 Upload started. Task : ', task);
      task.managedUpload.send((err, res) => {
        if (!err) {
          task.uploaded = true;
          this.debug('FILE UPLOADED', task);
          resolve(task);
        } else {
          task.error = err;
          const e = new LoaderError('UPLOAD_FAILED', {
            details: err,
          });
          this.debug('UPLOAD_FAILED', task, err);
          this.emit('error', e);
          // safely cancel uploads
          this._wrapUp(e);
          reject(task);
        }
        this.emit('progress', {
          task: 'uploadedFile',
          error: err,
          res: task,
          data: res,
        });
      });
    });
    this.uploadPromises.push(uploadPromise);
    return task;
  }
  addFiles(uploadBodies: UploadBody[]) {
    assert(uploadBodies instanceof Array, 'File Bodies must be an array of bodies');
    return uploadBodies.map((b) => this.addFile(b));
  }
  _wrapUp(error?: Error) {
    this.debug('WRAP UP STARTED');
    this.cancelUploads();
    const s3Cleanup = this.s3Cleanup;
    const isAlways = s3Cleanup === 'ALWAYS';
    const isSuccess = error === undefined && s3Cleanup === 'SUCCESS';
    if (isAlways || isSuccess) {
      this.cleanUpS3();
    }
    return this;
  }
  cancelUploads() {
    this.uploadTasks.forEach((task) => {
      if (task.uploaded) {
        return;
      }
      this.debug('CANCELLING CURRENT UPLOAD', task);
      task.managedUpload.abort();
    });
    return this;
  }
  abort() {
    this.cancelUploads();
    this.cleanUpS3();
    return this;
  }
  cleanUpS3(): Promise<'NOTHING_TO_DELETE' | AWS.S3.DeleteObjectsOutput> {
    const Objects = this.uploadTasks
      .filter((task) => task.uploaded)
      .map(function (task) {
        return { Key: task.key };
      });
    this.debug('CLEAN UP STARTED', Objects);
    if (Objects.length === 0) {
      this.debug('NOTHING_TO_DELETE');
      return Promise.resolve('NOTHING_TO_DELETE');
    }
    if (this.manifestKey) {
      Objects.push({ Key: this.manifestKey });
    }
    const params = {
      Bucket: this.bucket,
      Delete: { Objects },
    };
    return new Promise((resolve, reject) => {
      this.S3.deleteObjects(params, (err, data) => {
        this.emit('progress', {
          task: 'deleteObjects',
          error: err,
          data,
        });
        this.debug('CLEAN UP RESPONSES', err, data);
        if (err) return reject(err);
        return resolve(data);
      });
    });
  }
  getQualifiedTable(tableType = 'table') {
    if (tableType === 'table'){
      const { schema, table } = this.table;
      return schema ? schema + '.' + table : table;
    }
    assert(tableType === 'loadingTable', 'Invalid tableType: ' + tableType);
    const { table, schema } = this.loadingTable;
    return schema ? schema + '.' + table : table;
  }
  insert() {
    return this._start(UploadType.INSERT);
  }
  truncInsert() {
    return this._start(UploadType.TRUNCATE_INSERT);
  }
  upsert() {
    return this._start(UploadType.UPSERT);
  }
  async _start(uploadType: UploadType) {
    console.log(this)
    assert(this.uploadTasks.length > 0, 'Some Files must be added first');
    this.uploadType = uploadType;
    this._started = true;
    try {
      const tasks: UploadTask[] = await Promise.all(this.uploadPromises);
      await this.uploadManifest();
      const { q, cleanUp } = this.makeQueries();
      await transactionQuery(this.rsPool, q, cleanUp);
      this._wrapUp();
      this.emit('progress', {
        task: 'done',
        res: 'SUCCESS',
      });
      this.emit('done', 'SUCCESS');
      return 'SUCCESS';
    } catch (err) {
      this._wrapUp(err);
      this.debug('ERROR', err, err.details, err.stack);
      this.emit('error', err);
      throw err;
    }
  }
  uploadManifest() {
    const { bucket } = this;
    const filePrefix = this.getFilePrefix();
    const manifest = {
      entries: this.uploadTasks.map(function (tasks) {
        return {
          url: `s3://${bucket}/${tasks.key}`,
          mandatory: true,
        };
      }),
    };
    const manifestKey = `${filePrefix}_${this.jobTime}_manifiest_${v4().replace(/-/g, '')}.json`;
    this.manifestKey = manifestKey;
    this.debug('MANIFEST S3 UPLOAD STARTED', manifestKey, manifest);

    return new Promise((resolve, reject) => {
      this.S3.upload({
        Key: manifestKey,
        Body: JSON.stringify(manifest, null, 2),
        Bucket: bucket,
      }).send((err, res) => {
        this.emit('progress', {
          task: 'uploadedManifest',
          error: err,
          data: res,
        });
        if (!err) {
          this.debug('MANIFEST UPLOADED', res);
          return resolve(res);
        }
        const e = new LoaderError('MANIFOLD_UPLOAD_FAILED', { details: err });
        this.debug('MANIFEST UPLOAD FAILED', manifestKey, e);
        this.emit('error', e);
        return reject(e);
      });
    });
  }
  makeQueries(): { q: string[]; cleanUp?: string } {
    const { copySettings, idField, removeTempTable } = this.defaults;
    const { table, loadingTable, bucket, uploadType } = this;
    const { timeFormat } = copySettings;
    const TABLE = this.getQualifiedTable('table');
    let TEMP_TABLE = null;
    if (uploadType === 'UPSERT') {
      if(loadingTable === undefined || loadingTable === null){
        TEMP_TABLE = TABLE
      }else{
        TEMP_TABLE = loadingTable.schema + '.' + (loadingTable.table || table.table);
      }
      TEMP_TABLE = `${TEMP_TABLE}_temp_${createRunTime()}_${v4().replace(/-/g, '')}`; // .replace(/.*\./, '');
    }
    const TIMEFORMAT = timeFormat ? ` TIMEFORMAT '${copySettings.timeFormat}' ` : '';

    const copySQL = `
			COPY ${uploadType === 'UPSERT' ? TEMP_TABLE : TABLE}
			FROM 's3://${bucket}/${this.manifestKey}'
			${this.AWS_CREDS}
			MANIFEST
			${createFormat(copySettings)}
			${copySettings.gzip ? 'GZIP' : ''}
			${TIMEFORMAT}
			${copySettings.truncateCols ? 'TRUNCATECOLUMNS' : ''}
			MAXERROR ${copySettings.maxError}
		`;
    switch (uploadType) {
      case 'INSERT':
        return { q: [copySQL] };
      case 'TRUNCATE_INSERT':
        return { q: [`TRUNCATE ${TABLE};`, copySQL] };
      default:
        const DROP_TABLE = removeTempTable ? `DROP TABLE IF EXISTS ${TEMP_TABLE};` : '';
        const q = [
          `
				BEGIN TRANSACTION;`,
          `DROP TABLE IF EXISTS ${TEMP_TABLE};`,
          `CREATE TABLE ${TEMP_TABLE} (LIKE ${TABLE});`,
          copySQL,
          `DELETE FROM ${TABLE}
				WHERE ${idField} IN (SELECT ${idField} FROM ${TEMP_TABLE});`,
          `INSERT INTO ${TABLE}
				SELECT * FROM ${TEMP_TABLE};`,
          `${DROP_TABLE}`,
          `COMMIT TRANSACTION;`,
        ].filter((q) => q.trim());
        return { q, cleanUp: DROP_TABLE };
    }
  }
}
function createFormat(copySettings: CopySettings) {
  if (copySettings.format !== 'JSON') {
    return copySettings.format;
  }
  return `JSON ${copySettings.columnMap}`;
}

// RedshiftLoader.prototype.createTable = function(tableParams,cb)
// {
// 	assert(tableParams && tableParams.columns,'params and columns required');
// 	if(typeof cb === 'function')
// 	{
// 		promiseToCb(this.createTable(tableParams),cb)
// 		return this
// 	}
// 	console.warn('Cols and types must be correct for Redshift. No validity Check is performed')
//     const TABLE = this.getQualifiedTable('table');
//     let OVERWRITE = (tableParams.overwrite) ? `DROP TABLE IF EXISTS ${TABLE};` : '';
//     let fieldsList = tableParams.columns.map(function(c){
//     	return (typeof c == "string") ? c+' varchar' : `${c.name} ${c.type || 'varchar'}`;
//     }).join(',');
//     let query = `
//     	${OVERWRITE}
//     	CREATE TABLE ${TABLE}
//     	(${fieldsList})
//     	${(tableParams.additionalText) ? tableParams.additionalText : ''}
//     	;
//     `;
//     this.debug('CREATATION STATEMENT PRODUCED IS :\n',query);
// 	let Q = transactionQuery(this.rsPool, [query])
// 	if(this.bodies.length > 0){
// 		Q = Q.then(()=>this.insert());
// 	};
// 	return Q;
// }
