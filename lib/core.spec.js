
const CONFIG = require('../config');
const expect = require('chai').expect;
const sinon = require('sinon');
const { transactionQuery , mergeOptionsAndDefaults } = require('./core');

describe('Core Tools', function() {
	var query,
		releaseClient,
		DUMMY_POOL
	before(function() {
		DUMMY_POOL = function(fn,forceError){
			if(forceError) return fn(new Error('NO_CONNECTION')) 
			fn(null,{
				query:query
			},releaseClient)
		};
		/*
		const pg = require('pg');
		pg.defaults.poolIdleTimeout = 600000;
		let params = Object.assign({idleTimeoutMillis:600000},CONFIG.REDSHIFT_DETAILS)
		pool = new pg.Pool(params);
		*/
	    // runs before all tests in this block
	});
	beforeEach(function(){
		query = sinon.stub()
		releaseClient = sinon.stub()
		query.yields(null, []);
	})
	it('Runs Three Queries : GetClient Fn',function(done){
		let queries = [
			'SELECT 1 as n;',
			'SELECT 2 as n;',
			'SELECT 3 as n;',
		];
		transactionQuery(DUMMY_POOL,queries).then(function(res){
			expect(query.callCount).to.equal(3);
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
			expect(query.callCount).to.equal(3);
			expect(res).equals('SUCCESS');
			done()
		})
	});
	it('Runs Three Queries Second Fails',function(done){
		query = sinon.stub();
		let queries = [
			'SELECT 1 as n;',
			'ERROR:syntax error at or near "typo"',
			'SELECT 1 as n;',
		];
		let error = new Error('syntax error at or near "typo"');
		query.withArgs(queries[0]).yields(null,[])
		query.withArgs(queries[1]).yields(error)
		query.yields(null,[])
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
			'RS_COPY_ERROR'
		];
		query = sinon.stub();
		query.withArgs(queries[1]).yields(new Error("'stl_load_errors'"))
		query.withArgs('SELECT * FROM stl_load_errors ORDER BY starttime DESC LIMIT 1').yields(null,[{error:'load_error'}])
		query.yields(null,[])

		transactionQuery(DUMMY_POOL,queries).catch(function(err) {
			expect(query.callCount).to.equal(4) // ROLLBACK && GET last error
			expect(err).to.be.an.instanceof(Error)
			.that.has.property('message')
			.that.equals("RS_COPY_ERROR")
			expect(err).to.have.property('details')
			.that.deep.equals({error:'load_error'})
			done();
		}).catch(done)
	});
	it('It Cleans Up',function(done){
		let queries = [
			'SELECT 1 as n;',
			'RS_COPY_ERROR'
		];
		query = sinon.stub();
		query.withArgs(queries[1]).yields(new Error("'stl_load_errors'"))
		query.withArgs('SELECT * FROM stl_load_errors ORDER BY starttime DESC LIMIT 1').yields(null,[{error:'load_error'}])
		query.yields(null,[])

		transactionQuery(DUMMY_POOL,queries,'CLEANUP QUERY').catch(function(err) {
			expect(query.callCount).to.equal(5) // ROLLBACK && GET last error
			expect(err).to.be.an.instanceof(Error)
			.that.has.property('message')
			.that.equals("RS_COPY_ERROR")
			expect(err).to.have.property('details')
			.that.deep.equals({error:'load_error'})
			done();
		}).catch(done)
	});
	it('Merges nicely',function(done)
	{
		let options = {
			hello:"man",
			ob:{name:'kate'},

		}
		let defaults = {
			hello:"man",
			ob:{name:'john',age:30},
			word:true,
			lemon:{h:9}
		};
		let out = mergeOptionsAndDefaults(options,defaults)
		expect(out).to.deep.equal({ 
			hello: 'man',
  			ob: { name: 'kate', age: 30 },
  			word: true,
  			lemon: { h: 9 } 
  		})
		done();
	})

	after(function() {
		//pool.end()
	    // runs before all tests in this block
	});
})
