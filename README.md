# rs-streamloader
Redshift Loading Tool (ETL) - receives readable streams and Copies to Redshift (upsert or insert) via S3

rs-streamloader is a highly asnycronous and low memory utility to manage Loading data into Amazon Redshift using S3.
It uses 'aws-sdk'and 'pg' under the hood. rs-streamloaded avoids holding anything in memory by passing data effectively to S3. 
It can handle pre-framegmented files which both arrive and need to be uploaded asnycronously. For example a number of report downloads which are provided as seperate files which should all end up in the same Redshift table. rs-streamloader also provides clean up in the form of removing S3 files and temporary table on demand.

```js
const {RedshiftLoader} = require('rs-streamloader');
const fs = require('fs');
let options = {
	aws:{
		accessKeyId:"aws_access_key_id",
		secretAccessKey:"aws_access_key_id"
	},
	redshiftCreds:MY_RS_CREDENTIALS,
	bucket:'MY_BUCKET',
	filePrefix:'upload/pre_',
	bodies:[
		fs.createReadStream('./localFile.json')
	],
	table:'raw_data.my_table'
};

let rl = new RedshiftLoader(options);
await rl.insert()
```
## Contents
- [Installation](#installation)
- [Core Methods](#core-methods)
- [Creating options Object](#creating-options-object)
- [Default Options](#default-options)
- [Options Explained](#options-explained)
- [Sample Usage](#sample-usage)

### N.B
Elements of the configation take strings as arguments. These strings are used to create plain queries that will be run on Redshift. 
ALWAYS think about possible ***SQL Injection***. NEVER pass strings directly from a clientside user. 

### Installation
```$ npm install rs-streamloader --save ```

Requiring the module as standard and creating a RedshiftLoader instance
```javascript
const RedshiftLoader = require('rs-streamloader');
let rl = new RedshiftLoader(options);
``` 

### Core Methods
- ```.insert``` : adds file rows directly to target table
- ```.upsert``` : add file rows to temp table. Inserts any new rows and replaces any existing rows (based on id field)
- ```.createTable``` : creates table based on config before inserting file rows
- ```.addFile(s)``` : adds File(s) to the S3 upload. Beginning upload immediately 

```insert```, ```upsert``` and ```createTable``` will all await any S3 uploads to complete before Redshift load


### Creating options Object
```javascript
let options = {
	aws:{ 
		accessKeyId:"aws_access_key_id",
		secretAccessKey:"aws_access_key_id",
		sessionToken?:'optional session token'
	},
	// required for aws-sdk https://www.npmjs.com/package/aws-sdk
	// can also be provided as envs
	// http://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/loading-node-credentials-environment.html
	s3Object:instanceof AWS.S3 // aws-sdk. Pass your own if you wish
	s3Settings // will be passed to the create S3 new AWS.S3(s3Settings)
	
	redshiftCreds:"url_string", 
	// passed directly to pg.Pool(redshiftCreds) https://www.npmjs.com/package/pg
	redshiftPool:instanceof pg.Pool,// alternative to Credentials passing custom pool getter/pg.Pool

	bucket:'s3://BUCKETREFERENCE',
	filePrefix:'etlLoadingFolder/files_', // location into which files will be loaded
	bodies:[
		ReadableStream | string | instanceof Buffer
	], // list of bodies to be uploaded
	body:'{"id":1}', // single body to be uploaded (see below)
	table:{table:'data_table',schema,'raw_data'}, // or "raw_data.data_table"
	// table and schema for data to end up in
	loadingTable:{table:'temp_table',schema:'loading'},
	// loading location (optional)
	copySettings:{
	// passed into the building of RS COPY statement
		timeFormat:'epochmillisecs',
		format:'JSON',
		truncateCols:true,
		gzip:true
	},
	removeTempTable:true,
    // should the any TEMP RS loading table be removed (false for debugging)
	s3Cleanup:"SUCESS" | "NEVER" | "ALWAYS",
	// When should S3 files be deleted on cleanup
	debug:true // log more for debugging
};
let rl = new RedshiftLoader(options);
rl.upsert().then(doSmthing).catch(thoseErrors)
```

### Default Options
These are the "default" defaults.
```js
defaults.config = {
	removeTempTable:true,
	idField:'id',
	s3Cleanup:"SUCCESS",
	copySettings:{
	    timeFormat:'auto',
		format:'json',
		truncateCols:true
	}
}
```
Using the Factory helper will be helpful in saving time as many elements of the options will always be the same.
```js
const { RedshiftLoaderFactory } = require('rs-streamloader');
const rsLoaderFactory = new RedshiftLoaderFactory({
    bucket:"MY-S3-BUCKET",
    aws:MY_AWS_CONFIGS,
    redshiftPool:MY_ONE_RS_POOL,
    filePrefix:'folder/where-i-put-everything',
    loadingTable:{schema:"my_loading_schema_in_redshift"},
    table:{schema:"my_raw_data_schema_in_rs"}
})

RedshiftLoader.defaults.config = Object.assign(RedshiftLoader.defaults.config, myNewDefaults);
// we could even reexport this as a module , set up once and share among the project
module.exports = RedshiftLoader;
// or we could just fire away and use it
let tinyOpts = {
    table:'table_to_load_to', // goes to "my_raw_data_schema_in_rs"."table_to_load_to" in RS
    body:FILE_IN_QUESTION
}
let rl = rsLoaderFactory.createLoader(tinyOpts) // instaceof RedshiftLoader
rl.insert().then(doSmthg).catch(anyErrors)
```
If you have as extreme an example as above consider inititalizing the defaults in a dedicated module

### Options Explained
##### AWS Credenitals and Settings
AWS access can be provided in a number of ways but is required S3 file upload and Redshift COPY from S3. 
If ```process.env.AWS_ACCESS_KEY_ID```  and ```process.env.AWS_SECRET_ACCESS_KEY``` are set (eg on EC2 instance) then ```options.aws``` is not needed.
- ***aws*** : provide aws_access_key_id && aws_secret_access_key for RS load and S3
- ***s3Object*** : provide your own preset-up S3 instance. eg below. 
```js
// ----- S3Object Example
let AWS = require('aws-sdk');
AWS.config.loadFromPath('./config.json'); // my own way of auth
// http://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/loading-node-credentials-json-file.html
let myS3Instance = new AWS.S3(CUSTOM_CONFIG);
let options = {
    ...,
    S3Object:myS3Instance
};
let rl = new RedshiftLoader(options);
```
- **s3Settings** *(optional)* : will be passed into the created S3 instance at creatation
- ***bucket*** *(required)* : the S3 bucket to upload to 
- ***filePrefix*** *(required)* : the prefix / location for uploaded files.
- **s3Cleanup** *(optional)* : handling the S3 files after everything is done ( default "SUCCESS" ) 
  - **"ALWAYS" / true** : on success or error all S3 files associated will be deleted
  - **"SUCCESS"** : on success  all S3 files associated will be deleted
  - **"NEVER"** / false : never delete files
##### Redshift Credentials and Connection
Similar to AWS above RS credentials are required but can be provided in a number of ways
- ***redshiftPool*** : Provide an instance of pg.Pool or a custom client getter method. This is advised in line with having only one pool for your Redshift connection per project. [Check it out here](https://node-postgres.com/features/pooling). You can set the Pool up how and where you like and just feed it to the RedshiftLoader.
```js
/// Using pg Pool instance
const pg = require('pg');
pg.defaults.poolIdleTimeout = 600000; // I want to change my defaults
let CREDS = JSON.parse(process.env.REDSHIFT_DETAILS); 
const myOnePool = new pg.Pool(CREDS);

// no need to keep passing the pool in  options. now Redshift loader uses our central pool by default
let rl = new RedshiftLoader({...options,rsPool:myOnePool});
```
###### Custom clientGetter Function via ```options.redshiftPool```
If you want to implment Redshift connection some other way with other custom logic or another module than ```pg``` you can do that too. It just needs to have these elements
```js
const myPool = {
	connect:function(fn){
		let error = null
		let client = {
			query:function(sqlQuery){
				return Promise.resolve({ rows:[] })
			}
		}
		let releaseClient = function(err){
			if(err){
				// ...do something
			}
		}
		fn(err, client, releaseClient))
	}
}  
```
- ***redshiftCreds*** : If you arent worried about how the RS connection happens then just pass login credentials. it will go direct to ```pg module``` as found [here](https://node-postgres.com/features/connecting). 
```js
options.redshiftCreds = {
	"user":"USER",
	"password":"PASSWORD",
	"database":"production",
	"port":5439,
	"host":"warehouse.cluster.eu-central-1.redshift.amazonaws.com"	
}
```
##### Core Options
- ***table*** *(required)* : the table option tells RedshiftLoader into which RS table to put the data in the end
  - String eg ```"schema_name.table_name"```
  - Object eg ```{table:"table_name",schema:"schema_name"}```
  - If you commonly load data to the same schema setting up that schema in the defaults would be useful
```js
/// Commonly used schema
RedshiftLoader.defaluts.config.table = {schema:"my_raw_data"};
let options = {
    // ...
    table:"table_name"
    // or
    table:{table:"table_name"}
};
let rl = await new RedshiftLoader(options).insert()
// both will upload to "my_raw_data"."table_name"
```
- ***loadingTable*** *(optional)* : same as ```options.table``` above but set the loading table (for upserts)
- ***body*** *(optional)* : will be passed directly to ```S3.upload({ Body:body })``` from aws-sdk [ read-here](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#upload-property). Can be a ```string```, ```Buffer``` or an instance of a ```ReadableStream```
- ***bodies*** *(optional)* : an Array of bodies to be uploaded as above.
***N.B.*** if ```options.bodies``` or ```options.body``` is not provided initially it should be provided ```rl.addFile(body)``` or ```rl.addFiles(bodies)``` methods later
- ***copySettings*** *(optional)* : an object that describes some of the redshift ```COPY``` settings found [here](http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html#r_COPY-syntax)
  - timeFormat : eg 'epochmillisecs' read [here](http://docs.aws.amazon.com/redshift/latest/dg/r_DATEFORMAT_and_TIMEFORMAT_strings.html). provided string will be passed into single quotes. 
  - format : file format. 'json' is the default.
  - truncateCols (boolean) : should long varchars be truncated if they exceed the column max lenght
  - gzip : are the files gzipped

If there are more settings for the copy query desired just ask me or better yet create a pull request.

***N.B*** These string pass directly to a query string and are ***NOT SQL escaped** and ***should never come from client*** as  there is risk of SQL injection.
- ***idField*** *(optional)* : when performing an upsert what column should be used. ***AGAIN SQL INJECTABLE***. default is "id"

## Sample Usage

#### Simple file to table
```js
const { RedshiftLoader } = require('rs-streamloader');
const fs = require('fs');
let options = {
	aws:AWS_CREDS,
	redshiftCreds:MY_RS_CREDENTIALS,
	bucket:'MY_BUCKET',
	filePrefix:'upload/pre_',
	table:'raw_data.my_table'
};
let rl = new RedshiftLoader(options);
rl.addFile( fs.createReadStream('./localFile.json') )
await rl.insert()
```

#### Asyncronously Add Files
```js
let options = {
    //...
};
let rl = new RedshiftLoader(options);
rl.addFile( fs.createReadStream('./localFile.json') )
setTimeout(function(){
    rl.addFile( fs.createReadStream('./anotherFile.json') ) // uploads begin immediately
     /// call rl.insert only after all files have been added
    rl.upsert().then(function(res){
        ///...
    }).catch(function(err){
        ///....
    })
},1000);
setTimeout(function(){
    // this file will be too late as .upsert has been called.
    rl.addFile( fs.createReadStream('./tooLateFile.json') );
},5000)
```

#### RedshiftLoader as an EventEmiter
```js
let options = {
    //...
    body:fs.createReadStream('./localFile.json')
};
let rl = new RedshiftLoader(options)
.on('error',function(err){
    console.log(err,err.message, err.details); /// what happened?
})
.on('done',function() {
    /// lets keep going
})
.on('progress',function(data) {
    console.log(data); // how are we getting on ? 
})
rl.upsert()
```
#### Complex Async Streams
If stream bodies or files are being recieved asyncronously we can handle that. This might occur for example when pulling reports for multple account from and API all of which should go to the same RS table.
```js
let rl = new RedshiftLoader(options); // create an instance
// FileEmitter is fictional object that emits streams which are a json new line deliminated file
// eg. large graph like navigation of API each downloading a file
// eg Adwords getting multiple CSV download streams.
FileEmitter
.on('fileStream',function(fileStream)
{
	let uploadTask = rl.addFile(fileStream);
	uploadTask.managedUpload.on('progress',doSmthing) // aws-sdk managedUpload object;
})
.on('error',function(err){
	// if an error occurs while we are asyncronously recieving our files we may want to abort the porcess
	rl.abort(); // abort the upload of the currently uploading files and remove any already uploaded files from S3
	// handle error
})
.on('complete',function()
{
	// FileEmmiter confirms that there are no more files;
	rl.insert(cb) // insert all the Files in question to the RS table outlined on rl init
})
FileEmitter.start()
```
#### Debug a Specific Copy Error
```js
let options = {
 // ...
 s3Cleanup:"NEVER" // lets ensure that all our s3 uploads are kept
}
let rl = new RedshiftLoader(options)
.on('error',function(err){
    if(err.message !== 'RS_COPY_ERROR'){ // we are interested in these errors
        console.log(err.details) //  contains load error from stl_load_errors table in RS
        // then you can check out the S3 files on your S3.
        return 
    }else{
        console.log("SOME OTHER ERROR");
        rl.cleanUpS3();
        // since its not the error we are worried about we can delete the S3 files
    };
})
.on('done',function(){
	//..
})
```
#### Huge Number of Streamed Rows
```js
let rs = new Readable();
rs._read = function(size){};

let uploadParams = {
	filePrefix:'test/huge_file',
	table:'huge_table'
};
let rl = new RedshiftLoader(uploadParams)
rl.addFile(rs);

let i = 0;
new GiantReport() /// huge data that should be split (millions of rows)
.on('row',function(row){
	if(i % 100000 == 0 && i > 1){ // every 10k rows we want to split the file so that Redshift can use its clusters effectively
		rs.push(null); // finish stream
		rs = new Readable(); // create new stream
		rs._read = function(size){}; // this isn't good practice in the real world as we can't control the flow of data from source to upload
		rl.addFile(rs); // add the stream to the list of files to upload
	};
	i++;
	rs.push(JSON.stringify( row )+'\n'); // addData to stream & thus to S3;
})
.on('end',function(){
	rs.push(null); // finish final stream
	rl.insert().then(smthg); // confirm insert
})
.stream();

```





