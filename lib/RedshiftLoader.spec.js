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

describe('Redshift Loader ', function() {
	var s3SendStub,
		s3DeleteStub,
		RedshiftLoader,
		AWS,
		options,
		releaseClient,
		query
	before(function() {
		process.env.AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID || AWS_CREDS.aws_access_key_id;
		process.env.AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY || AWS_CREDS.aws_secret_access_key;
		mockery.enable({
	      warnOnReplace: false,
	      warnOnUnregistered: false,
	      useCleanCache: true
	    });
	    AWS = {
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

	    const DUMMY_POOL = function(fn,forceError){
			if(forceError) return fn(new Error('NO_CONNECTION')) 
			fn(null,{
				query:query
			},releaseClient)
		};

    	mockery.registerMock('aws-sdk', AWS);
    	RedshiftLoader = require('./RedshiftLoader');
    	RedshiftLoader.defaults.config = Object.assign(RedshiftLoader.defaults.config,{
    		aws:AWS_CREDS,
    		bucket:AWS_BUCKET,
    		redshiftPool:DUMMY_POOL
    	});
	});
	beforeEach(function(){
		s3SendStub = sinon.stub()
		AWS.S3.prototype.deleteObjects = s3DeleteStub = sinon.stub();
		s3SendStub.yields(null, {});
		s3DeleteStub.yields(null, {});

		query = sinon.stub()
		releaseClient = sinon.stub()
		query.yields(null, []);

		options = {
			filePrefix:'test/test1',
			bodies:[''],
			table:{table:'rs_loader',schema:'tests'},
			//loadingTable:{schema:'loading'},
			copySettings:{
				format:"json",
				timeFormat:"epochmillisecs",
				truncateCols:true
			}
		};	
	})
	after(function(){
	    mockery.disable();
	});
	it('Inserts Bodies And Deletes',function(done){
		options.bodies = [
			TEST_ROWS.filter((r)=> r.id < 3).map((r)=>JSON.stringify(r)).join('\n'),
			TEST_ROWS.filter((r)=> r.id >= 3).map((r)=>{r.text="UPSERT";return JSON.stringify(r)}).join('\n'),
		]
		let ld = new RedshiftLoader(options)
		let uploadManifest =  sinon.spy(ld,'uploadManifest')
		ld.insert(function(err,res)
		{
			expect(uploadManifest.callCount).to.equal(1)
			expect(s3SendStub.callCount).to.equal(3);
			done()
		});	
	});
	it('Debugging Throws no errors',function(done){	
		options.debug = true;
		let temp = RedshiftLoader.prototype.debug;
		RedshiftLoader.prototype.debug = function(){};
		let ld = new RedshiftLoader(options)
		ld.insert(function(err,res)
		{
			expect(s3SendStub.callCount).to.equal(2);
			expect(s3DeleteStub.callCount).to.equal(1);
			done()
		})
	});
	it('s3Cleanup Called Once on SUCCESS',function(done){
		new RedshiftLoader(options).insert(()=>
		{
			expect(s3DeleteStub.callCount).to.equal(1);
			done();
		})
	});
	it('NEVER calls s3Cleanup',function(done){
		options.s3Cleanup = 'NEVER';
		new RedshiftLoader(options).insert(function(res)
		{
			expect(s3SendStub.callCount).to.equal(2);
			expect(s3DeleteStub.callCount).to.equal(0);
			done()
		});
	});
	it('Add File Increases uploads',function(done){
		options.s3Cleanup = 'NEVER';
		let rl = new RedshiftLoader(options)
		rl.addFile('')
		rl.insert(function(res)
		{
			expect(s3SendStub.callCount).to.equal(3);
			done()
		});
	});
});







