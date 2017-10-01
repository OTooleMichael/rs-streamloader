
const CONFIG = require('../config');
const expect = require('chai').expect;
const { transactionQuery } = require('./core');
var pool;
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
describe('Transaction Query', function() {
	before(function() {
		/*
		const pg = require('pg');
		pg.defaults.poolIdleTimeout = 600000;
		let params = Object.assign({idleTimeoutMillis:600000},CONFIG.REDSHIFT_DETAILS)
		pool = new pg.Pool(params);
		*/
	    // runs before all tests in this block
	});
	it('Runs Three Queries : GetClient Fn',function(done){
		let queries = [
			'SELECT 1 as n;',
			'SELECT 2 as n;',
			'SELECT 3 as n;',
		];
		transactionQuery(DUMMY_POOL,queries).then(function(res){
			expect(res).equals('SUCCESS');
			done()
		})
	});
	it('Runs Three Queries : Using Pool',function(done){
		let queries = [
			'SELECT 1 as n;',
			'SELECT 2 as n;',
			'SELECT 3 as n;',
		];
		//this is how a pg pool object looks at its simplest
		let pool = {
			connect:DUMMY_POOL
		}
		transactionQuery(pool,queries).then(function(res){
			expect(res).equals('SUCCESS');
			done()
		})
	});
	it('Runs Three Queries Second Fails',function(done){
		let queries = [
			'SELECT 1 as n;',
			'ERROR:syntax error at or near "typo"',
			'SELECT 3 as n;',
		];
		transactionQuery(DUMMY_POOL,queries).catch(function(err)
		{
			expect(err).to.be.an.instanceof(Error);
			expect(err.message).equals('syntax error at or near "typo"')
			done()
		}).catch(done);
	});
	it('Reject Pool',function(done){
		let queries = [
			'SELECT 1 as n;',
			'SELECT 3 as n;',
		];
		expect(function(){
			transactionQuery("INALID",queries)
		}).to.throw(Error)
		.that.has.property('message')
		.that.equals("Pool must be a fn or pg Pool");
		done()
	});
	it('Fail At Load : RS_COPY_ERROR',function(done){
		let queries = [
			'SELECT 1 as n;',
			'RS_COPY_ERROR',
			'SELECT 1 as n;'
		];
		transactionQuery(DUMMY_POOL,queries).catch(function(err) {
			expect(err).to.be.an.instanceof(Error)
			.that.has.property('message')
			.that.equals("RS_COPY_ERROR")
			expect(err).to.have.property('details')
			.that.deep.equals({})
			done();
		}).catch(done)
	});
	after(function() {
		//pool.end()
	    // runs before all tests in this block
	});
})
