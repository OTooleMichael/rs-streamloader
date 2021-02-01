import { transactionQuery }  from './core'
import {ClientLike, PoolLike, QueryResLike, ReleaseFn} from './types'
type QueryFn = (sql:string)=>Promise<QueryResLike>;
describe('Core Tools', function () {
  var query = jest.fn() as jest.MockedFunction<QueryFn>;
  var releaseClient = jest.fn() as jest.MockedFunction<ReleaseFn>
  var DUMMY_POOL: PoolLike;
  var forceConnectError = false
  beforeAll(function () {
    DUMMY_POOL = {
      connect:function(fn:(err?:Error, client?:ClientLike, done?:ReleaseFn)=>void){
        if(forceConnectError){
          return fn(new Error('NO_CONNECTION'))
        }
        fn(undefined,{ query }, releaseClient,);
      }
    }
    /*
		const pg = require('pg');
		pg.defaults.poolIdleTimeout = 600000;
		let params = Object.assign({idleTimeoutMillis:600000},CONFIG.REDSHIFT_DETAILS)
		pool = new pg.Pool(params);
		*/
    // runs before all tests in this block
  });
  beforeEach(function () {
    query = jest.fn() as jest.MockedFunction<QueryFn>
    query.mockResolvedValue({rows:[]})
    releaseClient = jest.fn()
  });
  it('Runs Three Queries : GetClient Fn', function (done) {
    let queries = ['SELECT 1 as n;', 'SELECT 2 as n;', 'SELECT 3 as n;'];
    transactionQuery(DUMMY_POOL, queries).then(function (res) {
      expect(query.mock.calls.length).toEqual(3);
      expect(res).toEqual('SUCCESS');
      done();
    });
  });
  it('Runs Three Queries Second Fails', function (done) {
    let queries = ['SELECT 1 as n;', 'ERROR:syntax error at or near "typo"', 'SELECT 1 as n;'];
    let error = new Error('syntax error at or near "typo"');
    query.mockImplementation(async function(sql){
      switch(sql){
        case queries[0]:
          return {rows:[]}
        case queries[1]:
          throw error
        default:
          return {rows:[]}
      }
    })
    transactionQuery(DUMMY_POOL, queries)
      .catch(function (err) {
        expect(err).toBeInstanceOf(Error);
        expect(err.message).toEqual('syntax error at or near "typo"');
        expect(query.mock.calls.length).toBeGreaterThan(2)
        done();
      })
      .catch(done);
  });
  it('Reject Pool', async function () {
    let queries = ['SELECT 1 as n;', 'SELECT 3 as n;'];
    expect.hasAssertions();
    try{
      await transactionQuery('INALID' as unknown as PoolLike, queries);
    }catch(error){
      expect(error).toHaveProperty('message','pool.connect is not a function')
    }
  });
  it('Fail At Load : RS_COPY_ERROR', function (done) {
    let queries = ['SELECT 1 as n;', 'RS_COPY_ERROR'];
    query.mockImplementation(async function(sql){
      switch(sql){
        case queries[0]:
          throw new Error("'stl_load_errors'")
        case 'SELECT * FROM stl_load_errors ORDER BY starttime DESC LIMIT 1':
          return {rows:[{ error: 'load_error' }]}
        default:
          return {rows:[]}
      }
    })
    transactionQuery(DUMMY_POOL, queries)
      .catch(function (err) {
        expect(query.mock.calls.length).toEqual(3); // ROLLBACK && GET last error
        expect(err).toBeInstanceOf(Error)
        expect(err).toHaveProperty('message','RS_COPY_ERROR');
        expect(err).toHaveProperty('details',{ error: 'load_error' });
        done();
      })
      .catch(done);
  });
  it('It Cleans Up', function (done) {
    expect.hasAssertions();
    let queries = ['SELECT 1 as n;', 'RS_COPY_ERROR'];
    query.mockImplementation(async function(sql){
      switch(sql){
        case queries[0]:
          throw new Error("'stl_load_errors'")
        case 'SELECT * FROM stl_load_errors ORDER BY starttime DESC LIMIT 1':
          return {rows:[{ error: 'load_error' }]}
        default:
          return {rows:[]}
      }
    })
    transactionQuery(DUMMY_POOL, queries, 'CLEANUP QUERY')
      .catch(function(err){
        expect(query.mock.calls.length).toEqual(4); // ROLLBACK && GET last error
        expect(err).toBeInstanceOf(Error)
        expect(err).toHaveProperty('message','RS_COPY_ERROR');
        expect(err).toHaveProperty('details',{ error: 'load_error' });
        done();
      })
  });
});
