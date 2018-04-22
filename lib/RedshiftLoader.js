const EventEmitter = require('events').EventEmitter;
const util = require('util');
const assert = require('assert');
const moment = require('moment')
const { v4 } = require('uuid')
const AWS = require('aws-sdk');
const { transactionQuery , mergeOptionsAndDefaults } = require('./core');
const pg = require('pg');

function promiseToCb(promise,cb)
{
	// reroutes cbs
	promise.then(function(res)
	{
		return cb(null,res);
	}).catch(cb).catch((err)=>setTimeout(()=>{throw err}));
	return 
}
function parseRsTables(tableParams)
{
	if(typeof tableParams == 'string')
	{
		if(/\./.test(tableParams))
		{
			let prts = tableParams.split('.')
			return { schema:prts[0], table:prts[1] }
		}else{
			return { table:tableParams };
		}
	}
	return tableParams
}
function RedshiftLoader(options)
{
	
	EventEmitter.call(this);
	/*
		{
			aws:{
				aws_access_key_id:"ID",
				aws_secret_access_key:
			},
			s3Object: pass an S3 object aws-sdk module to avoid process.env overwriting if you wish
			redshiftCreds:{
	
			},
			redshiftPool:ollObj:
			s3Settings: // passed directly to new AWS.S3();
			bucket:'',
			filePrefix:'',
			bodies:[],
			body:
			table:{table:'',schema,''},
			loadingTable:{table:'',schema:''*},
			copySettings:{
				timeFormat:,
				format:
				truncateCols:
			},
			s3Cleanup:true "ALWAYS" false "NEVER" "SUCCESS" 
		}
	*/
	// checks
	assert(options,'Options Required');
	// inits
	
	this.defaults = RedshiftLoader.defaults; // 
	this.bodies = [];
	this.uploadPromises = [];
	this.uploadComplete = false;
	this._started = false;
	this.jobTime = moment().format('YYYYMMDD_HHmmss');
	options.table = parseRsTables(options.table);
	options.loadingTable = parseRsTables(options.loadingTable); 
	let tempBodies = (options.body) ? [options.body] : options.bodies || []; // 
	options = mergeOptionsAndDefaults(options,this.defaults.config); // merge will strip instaceof from ReadableStream
	options.bodies = tempBodies;
	assert(options.bucket,"Options.bucket || RedshiftLoader.defaults.config.bucket Required");
	this._debuggingMode = options.debug;
	if(!options.aws && process.env.AWS_ACCESS_KEY_ID)
	{	// reduce friction in the AWS env. no neded to provide the options.aws
		// http://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/loading-node-credentials-environment.html
		options.aws = {
			aws_access_key_id:process.env.AWS_ACCESS_KEY_ID,
			aws_secret_access_key:process.env.AWS_SECRET_ACCESS_KEY
		}
	};
	assert(options.aws,'AWS CREDS must be set (options.aws) or be env vars (process.env.AWS_ACCESS_KEY_ID)');

	this.AWS_CREDS = ` CREDENTIALS 'aws_access_key_id=${options.aws.aws_access_key_id};aws_secret_access_key=${options.aws.aws_secret_access_key}' `;
	this.options = options;
	if(options.s3Object instanceof AWS.S3)
	{
		this.S3 = options.s3Object; // provide your own
	}
	else
	{
		// aws requires this for access
		process.env.AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID || options.aws.aws_access_key_id;
		process.env.AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY || options.aws.aws_secret_access_key;
		this.S3 = new AWS.S3(Object.assign({apiVersion: '2006-03-01' },options.s3Settings || {}));
	}
	this._createRsConnection();
	assert(options.bodies instanceof Array,"options.bodies must be an Array");
	options.bodies.forEach((b)=>this.addFile(b));

    return this
}
RedshiftLoader.prototype.debug = function()
{
	if(!this._debuggingMode) return
	console.log.apply(null,arguments);
}
RedshiftLoader.prototype._createRsConnection = function()
{
	if(this.options.redshiftPool)
	{
		this.rsPool = this.options.redshiftPool
		return this
	}
	pg.defaults.poolIdleTimeout = 600000;
	this.rsPool = pg.Pool(this.options.redshiftCreds);
	this.debug('Reshift Pool Created')
	return this
}

RedshiftLoader.prototype.addFile = function(uploadBody)
{
	// http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3/ManagedUpload.html
	if(this._started)
	{
		let e = new Error("RS_LOAD_STARTED:  Cannot add more files after RS load has started");
		console.warn(e);
		console.warn(e.stack);
		return
	};
	let task = {
		body:uploadBody,
		started:new Date()
	};
	let { bucket , filePrefix} = this.options;
	let i = this.bodies.push(task);
	task.i = i;
	task.key = `${filePrefix}${this.jobTime}_prt_${i}_${v4().replace(/-/g, '')}.txt`;
	task.managedUpload = this.S3.upload({
		Key:task.key,
		Body:task.body,
		Bucket:bucket
	})
	let uploadPromise = new Promise((resolve,reject)=>
	{
		this.debug('S3 Upload started. Task : ',task);
		task.managedUpload.send((err,res)=>
		{
			if(!err){
				task.uploaded = true;
				this.debug('FILE UPLOADED',task);
				resolve(task)
			}else{
				task.error = err;
				let e = new Error('UPLOAD_FAILED');
				e.details = err;
				e.task = task;
				this.debug('UPLOAD_FAILED',task,err);
				this.emit('error',e);
				// safely cancel uploads
				this._wrapUp(e);
				reject(task);
			};
			this.emit('progress',{
				task:'uploadedFile',
				error:err,
				res:task,
				data:res
			});
		})
	})
	this.uploadPromises.push(uploadPromise);
	return task;
}
RedshiftLoader.prototype.addFiles = function(uploadBodies)
{
	assert(uploadBodies instanceof Array,'File Bodies must be an array of bodies');
	return uploadBodies.map((b)=>this.addFile(b));
}
RedshiftLoader.prototype._start = function(uploadType,cb) {
	assert(this.bodies.length,'Some Files must be added first');
	if(typeof cb === 'function')
	{
		promiseToCb( this._start(uploadType) , cb)
		return this
	};
	this.uploadType = uploadType;
	this._started = true;
	let _self = this;
	return Promise.all(this.uploadPromises)
	.then((tasks)=>this.uploadManifest())
	.then(()=>{
		const {q,cleanUp} = this.makeQueries();
		transactionQuery(this.rsPool,q,cleanUp) 
	})
	.then(()=>{
		this._wrapUp();
		this.emit('progress',{
			task:'done',
			res:"SUCCESS"
		});
		this.emit('done',"SUCCESS");
		return Promise.resolve("SUCCESS");
	}).catch((err)=>
	{
		this._wrapUp(err);
		this.debug('ERROR',err,err.details,err.stack);
		this.emit("error",err)
		return Promise.reject(err);
	})
};
RedshiftLoader.prototype.insert = function(cb) {
	return this._start('INSERT',cb);
}
RedshiftLoader.prototype.truncInsert = function(cb) {
	return this._start('TRUNCATE_INSERT',cb);
}
RedshiftLoader.prototype.upsert = function(cb) {
	return this._start('UPSERT',cb);
}
RedshiftLoader.prototype.createTable = function(tableParams,cb)
{
	assert(tableParams && tableParams.columns,'params and columns required');
	if(typeof cb === 'function')
	{
		promiseToCb(this.createTable(tableParams),cb)
		return this
	}
	console.warn('Cols and types must be correct for Redshift. No validity Check is performed')
	let table = this.options.table;
    const TABLE = (table.schema) ? table.schema+'.'+table.table : table.table;
    let OVERWRITE = (tableParams.overwrite) ? `DROP TABLE IF EXISTS ${TABLE};` : '';
    let fieldsList = tableParams.columns.map(function(c){
    	return (typeof c == "string") ? c+' varchar' : `${c.name} ${c.type || 'varchar'}`;
    }).join(',');
    let query = `
    	${OVERWRITE}
    	CREATE TABLE ${TABLE}
    	(${fieldsList})
    	${(tableParams.additionalText) ? tableParams.additionalText : ''}
    	;
    `;
    this.debug('CREATATION STATEMENT PRODUCED IS :\n',query);
	let Q = transactionQuery(this.rsPool, [query])
	if(this.bodies.length > 0){
		Q = Q.then(()=>this.insert());
	};
	return Q;
}
RedshiftLoader.prototype.uploadManifest = function(){

    const { bucket, filePrefix } = this.options;
    let entries = this.bodies.map(function(tasks){
		return {
			url: `s3://${bucket}/${tasks.key}`,
			mandatory: true
		}
	});
    const manifest = {
      entries: entries
    };
    this.manifestKey = `${filePrefix}_${this.jobTime}_manifiest_${v4().replace(/-/g, '')}.json`;
    this.debug('MANIFEST S3 UPLOAD STARTED',this.manifestKey,manifest);
	
    return new Promise((resolve,reject)=>
    {
    	this.S3.upload({
			Key:this.manifestKey,
			Body:JSON.stringify(manifest, null, 2),
			Bucket:bucket
		})
		.send((err,res)=>
		{
			this.emit('progress',{
				task:'uploadedManifest',
				error:err,
				data:res
			});

			if(err){
				this.debug('MANIFEST UPLOAD FAILED',this.manifestKey,e);
				let e = new Error('MANIFOLD_UPLOAD_FAILED');
				e.details = err;
				this.emit('error',e);
				return reject(e)
			};
			this.debug('MANIFEST UPLOADED',res);

			return resolve(res);
		});
    })
}
RedshiftLoader.prototype.cancelUploads = function() {
	this.bodies.forEach((task)=>{
		if(!task.uploaded)
		{
			this.debug('CANCELLING CURRENT UPLOAD',task);
			task.managedUpload.abort();
		}
		return
	});
	return this
}
RedshiftLoader.prototype.abort = function() {
	this.cancelUploads();
	this.cleanUpS3();
	if(typeof this.rsPool.end == 'function') this.rsPool.end();
	return this
}
RedshiftLoader.prototype._wrapUp = function(error) {
	this.debug('WRAP UP STARTED');
	this.cancelUploads();
	let s3Cleanup = this.options.s3Cleanup
	if(s3Cleanup == 'ALWAYS' || s3Cleanup === true || (error == undefined && s3Cleanup == "SUCCESS" ) )
	{
		this.cleanUpS3();
	};
	if(typeof this.rsPool.end == 'function') this.rsPool.end();
	return this
}
RedshiftLoader.prototype.cleanUpS3 = function()
{
	let Objects = this.bodies.filter((task)=>task.uploaded)
	.map(function(task)
	{
		return {Key:task.key}
	});
	this.debug('CLEAN UP STARTED',Objects);
	if(Objects.length == 0 ){
		this.debug('NOTHING_TO_DELETE');
		return Promise.resolve('NOTHING_TO_DELETE')
	};
	if(this.manifestKey){
		Objects.push({Key:this.manifestKey});
	};
	let params = {
		Bucket: this.options.bucket, 
		Delete: {
			Objects:Objects, 
		}
	};
	return new Promise((resolve,reject)=>
	{
		this.S3.deleteObjects(params, (err, data)=>{
			this.emit('progress',{
				task:'deleteObjects',
				error:err,
				data:data
			});
			this.debug('CLEAN UP RESPONSES',err,data);
			if(err) return reject(err)
			return resolve(data)
		})
	});
};
RedshiftLoader.prototype.makeQueries = function()
{
	const { options } = this;
    const {  aws , table, loadingTable, bucket, copySettings ,idField, removeTempTable} = options;
    const TABLE = (table.schema) ? table.schema+'.'+table.table : table.table;
    let TEMP_TABLE = null
    if(this.uploadType == 'UPSERT')
    {
    	TEMP_TABLE = (loadingTable == undefined) ? TABLE : loadingTable.schema+'.'+(loadingTable.table||table.table);
    	TEMP_TABLE = `${TEMP_TABLE}_temp_${moment().format('YYYYMMDD_HHmmss')}_${v4().replace(/-/g, '')}`//.replace(/.*\./, '');
    };
    let FORMAT = (copySettings.format == "json") ? " json 'auto' " : (copySettings.format) ? ` ${copySettings.format} ` : "";
    let TIMEFORMAT = (copySettings.timeFormat) ? ` TIMEFORMAT '${copySettings.timeFormat}' `: "";		

    let copySQL =`
		COPY ${(this.uploadType == 'UPSERT') ? TEMP_TABLE : TABLE}
		FROM 's3://${bucket}/${this.manifestKey}'
		${this.AWS_CREDS}
		MANIFEST
		${FORMAT}
		${copySettings.gzip ? 'GZIP' : ''}
		${TIMEFORMAT}
		${copySettings.truncateCols ? 'TRUNCATECOLUMNS' : ''}
		${copySettings.maxErrors ? `MAXERROR ${this.maxErrors}` : ''};
    `;
    let uploadType = this.uploadType;
    switch(uploadType){
    	case 'INSERT':
    		return {q:[copySQL]}
    	break;
    	case 'TRUNCATE_INSERT':
    		return {q:[`TRUNCATE ${TABLE};`,copySQL]}
    	break;
    	default:
    		let DROP_TABLE = (removeTempTable) ? `DROP TABLE IF EXISTS ${TEMP_TABLE};` : ''
	    	let q = [`
			  BEGIN TRANSACTION;`,
			 `DROP TABLE IF EXISTS ${TEMP_TABLE};`,
			 `CREATE TABLE ${TEMP_TABLE} (LIKE ${TABLE});`,
			  copySQL,
			 `DELETE FROM ${TABLE}
			  WHERE ${idField} IN (SELECT ${idField} FROM ${TEMP_TABLE} );`,
			 `INSERT INTO ${TABLE}
			  SELECT * FROM ${TEMP_TABLE};`,
			  `${DROP_TABLE}`,
			  `COMMIT TRANSACTION;`
			].filter(q=>q.trim());
			return {q,cleanUp:DROP_TABLE}
    	break;
    }; 
}




RedshiftLoader.defaults = {
	config:{
		filePrefix:'rs-streamloader/unnamed',
		removeTempTable:true,
		//loadingTable:{schema:'public'},
		idField:'id',
		s3Cleanup:"SUCCESS",
		awaitS3Cleanup:false,
		copySettings:{
			timeFormat:'auto',
			format:'json',
			truncateCols:true
		}
	}
}



util.inherits(RedshiftLoader, EventEmitter);



module.exports = RedshiftLoader


