const { AWS_CREDS, AWS_BUCKET, REDSHIFT_DETAILS} = require('../config');
const sinon = require('sinon')
const mockery = require('mockery')
const expect = require('chai').expect;

const moment = require('moment');
const TEST_TABLE = `
CREATE TABLE tests.rs_loader_test 
(
	id int,
  	number FLOAT,
  	text varchar,
  	created_at timestamp
);`;
let time = moment('2017-01-01');
const TEST_ROWS = [
	{ number:56.8, text:"sample" },
	{ number:45, text:"other" },
	{ number:5.8, text:"man" },
	{ number:4, text:"woman" },
	{ number:78, text:"hello" },
	{ number:63, text:"there" },
].map((r,i)=>{
	r.created_at = time.valueOf();
	time = time.add(1,'day');
	r.id = i + 1;
	return r
});
const DUMMY_POOL = function(fn,forceError){
	if(forceError) return fn(new Error('NO_CONNECTION'))
	let done = function(error){

	}
	fn(null,{
		query:function(query,cb)
		{

			let commands = query.split(':');
			switch(commands[0]){
				case 'ERROR':
					return cb(new Error(commands[1]))
				break;
				case 'RS_COPY_ERROR':
					return cb(new Error("'stl_load_errors'"))
				case 'BAD QUERY FN':
					return cb(null,"SHOULDNT RETURN STRING")
				default:
					return cb(null,[{}])
			}
		}
	},done)
}
describe('Redshift Loader ', function() {
	var s3SendStub,
		s3DeleteStub,
		RedshiftLoader
	before(function() {
		process.env.AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID || AWS_CREDS.aws_access_key_id;
		process.env.AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY || AWS_CREDS.aws_secret_access_key;
		mockery.enable({
	      warnOnReplace: false,
	      warnOnUnregistered: false,
	      useCleanCache: true
	    });
	    let AWS = {
	    	S3:function(){
	    		return this
	    	}
	    }
	    s3SendStub = sinon.stub()
	    AWS.S3.prototype.upload = function(param) {
	    	return {
	    		send:s3SendStub
	    	}
	    };
	    s3DeleteStub = sinon.stub();
	    AWS.S3.prototype.deleteObjects = s3DeleteStub;


    	mockery.registerMock('aws-sdk', AWS);
    	RedshiftLoader = require('./RedshiftLoader');
    	RedshiftLoader.defaults.config = Object.assign(RedshiftLoader.defaults.config,{
    		aws:AWS_CREDS,
    		bucket:AWS_BUCKET,
    		redshiftPool:DUMMY_POOL
    		//redshiftCreds:REDSHIFT_DETAILS
    	});
		/*
		const pg = require('pg');
		pg.defaults.poolIdleTimeout = 600000;
		let params = Object.assign({idleTimeoutMillis:600000},CONFIG.REDSHIFT_DETAILS)
		pool = new pg.Pool(params);
		*/
	    // runs before all tests in this block
	});
	beforeEach(function(){
		s3SendStub = sinon.stub()
		s3DeleteStub = sinon.stub()
	})
	after(function(){
	    mockery.disable();
	});
	it('Inserts to Test',function(done){
		//this.timeout(30*1000);
		s3SendStub.yields(null, {});
		s3DeleteStub.yields(null, {});
		let options = {
			filePrefix:'test/test1',
			bodies:[
				TEST_ROWS.filter((r)=> r.id < 3).map((r)=>JSON.stringify(r)).join('\n'),
				TEST_ROWS.filter((r)=> r.id >= 3).map((r)=>{r.text="UPSERT";return JSON.stringify(r)}).join('\n'),
			],
			table:{table:'rs_loader',schema:'tests'},
			//loadingTable:{schema:'loading'},
			copySettings:{
				format:"json",
				timeFormat:"epochmillisecs",
				truncateCols:true
			}
		};
		let ld = new RedshiftLoader(options)
		ld.insert(function(err,res)
		{
			expect(s3SendStub.callCount).to.equal(3);
			done()
		});	
	});
	it('Debugging Throws no errors',function(done){
		//this.timeout(30*1000);
		
		s3SendStub.yields(null, {});
		s3DeleteStub.yields(null, {});
		let options = {
			debug:true,
			filePrefix:'test/test1',
			bodies:[
				TEST_ROWS.filter((r)=> r.id < 3).map((r)=>JSON.stringify(r)).join('\n'),
				TEST_ROWS.filter((r)=> r.id >= 3).map((r)=>{r.text="UPSERT";return JSON.stringify(r)}).join('\n'),
			],
			table:{table:'rs_loader',schema:'tests'},
			//loadingTable:{schema:'loading'},
			copySettings:{
				format:"json",
				timeFormat:"epochmillisecs",
				truncateCols:true
			}
		};	
		let temp = RedshiftLoader.prototype.debug;
		RedshiftLoader.prototype.debug = function(){};
		let ld = new RedshiftLoader(options)
		ld.insert().then(function(res)
		{
			expect(s3SendStub.callCount).to.equal(3);
			done()
		}).catch(function(err){
			console.log(err);
			expect(err).to.equal(null);
			done();
		}).catch(done)	
	});
});







