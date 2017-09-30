const RedshiftLoader = require('../index');
const { AWS_CREDS, AWS_BUCKET, REDSHIFT_DETAILS} = require('../config');
let DUMMY_POOL = function(){
	// expects a fn to get a client from a pool or a pg pool object from the package 'pg' // https://www.npmjs.com/package/pg
	// logic for personal Redshift Connection Pool
	// allows you to use a pool shared for the entire project rather than creating connections each time.
	// It is best practice to have only one pool per App. and to hold it in a module after init
	// https://github.com/brianc/node-pg-pool
};
let myDefaults = {
	aws:AWS_CREDS,
	bucket:AWS_BUCKET,
	redshiftPool:DUMMY_POOL
	//redshiftCreds:REDSHIFT_DETAILS
}
RedshiftLoader.defaults.config = Object.assign(RedshiftLoader.defaults.config,myDefaults);
// use our Personal default settings so that each RedshiftLoader is created with these options

function start(FileEmmiter){
	let options = {
		filePrefix:'test/test1', // where to put S3 files
		table:{table:'rs_loader',schema:'tests'}, //WHERE to load to. rs_loader.tests also works
		copySettings:{ // define the copy settings for RS COPY command
			format:"json",
			timeFormat:"epochmillisecs",
			truncateCols:true
		}
	};
	let rl = new RedshiftLoader(options); // create an instance
	// fictional object that emits streams which are a json new line deliminated file
	// eg. large graph like navigation of API each downloading a file
	// eg Adwords getting multiple CSV download streams.
	FileEmmiter
	.on('fileStream',function(fileStream)
	{
		let uploadTask = rl.addFile(fileStream) 
		// fileStream is passed directly to S3 upload Body param. if aws-sdk accepts it so does addFile
		// upload is started immediately
	})
	.on('error',function(err){
		// if an error occurs while we are asyncronously recieving our files we may want to abort the porcess
		rl.abort(); // abort the upload of the currently uploading files and remove any already uploaded files from S3
		// handle error
	})
	.on('complete',function()
	{
		// FileEmmiter confirms that there are no more files;
		rl.insert() // insert all the Files in question to the RS table outlined on rl init
		.then(function(res)
		{
			// insert worked 
		})
		.catch(function(err)
		{
			// handle errors
		})
	})
}