import {DateTime} from 'luxon'
import AWS from 'aws-sdk'
import RedshiftLoader from './RedshiftLoader'
import { createDefaults } from './utils';
import { ClientLike, PoolLike, QueryResLike, ReleaseFn, RSLoaderOptions } from './types';
type MiniS3 = Partial<AWS.S3> & {
  upload:jest.MockedFunction<AWS.S3['upload']>;
  deleteObjects:jest.MockedFunction<AWS.S3['deleteObjects']>;
}
type MiniS3Upload = Partial<AWS.S3.ManagedUpload> & {
  promise:jest.MockedFunction<AWS.S3.ManagedUpload['promise']>
}
type QueryFn = (sql:string)=>Promise<QueryResLike>;
var mS3Instance: MiniS3;
var managedUpload: MiniS3Upload;
managedUpload =  {
  promise:jest.fn().mockResolvedValue({} as AWS.S3.ManagedUpload.SendData),
  abort:jest.fn()
}
mS3Instance = {
  upload: jest.fn().mockReturnValue(managedUpload),
  deleteObjects:jest.fn(function(params: AWS.S3.DeleteObjectsRequest){
    return {
      promise:jest.fn().mockResolvedValue({} as AWS.S3.DeleteObjectsOutput)
    } as unknown as AWS.Request<AWS.S3.DeleteObjectsOutput, AWS.AWSError>
  }) as unknown as jest.MockedFunction<AWS.S3['deleteObjects']>
};
jest.mock('aws-sdk', () => {
  return { S3: jest.fn(() => mS3Instance) };
});
const CONFIG = {
  REDSHIFT_DETAILS: {
    port: 5239,
    host: 'aws-redshift.stuff',
    user: 'lemon',
    password: 'pie time',
  },
  AWS_CREDS: {
    aws_access_key_id: '123r34',
    aws_secret_access_key: '342352345',
  },
  AWS_BUCKET: 'buiectkIndeed',
};
const { AWS_CREDS } = CONFIG;
let time = DateTime.fromFormat('2017-01-01', 'yyyy-LL-dd', { zone: 'utc' });
const TEST_ROWS = [
  { number: 56.8, text: 'sample', created_at:0, id:0 },
  { number: 45, text: 'other', created_at:0, id:0 },
  { number: 5.8, text: 'man', created_at:0, id:0 },
  { number: 4, text: 'woman', created_at:0, id:0 },
  { number: 78, text: 'hello', created_at:0, id:0 },
  { number: 63, text: 'there', created_at:0, id:0 },
].map((r, i) => {
  r.created_at = time.valueOf();
  time = time.plus({day:1});
  r.id = i + 1;
  return r;
});

describe('Redshift Loader ', function () {
  var options: RSLoaderOptions ;
  var query = jest.fn() as jest.MockedFunction<QueryFn>;
  var releaseClient = jest.fn() as jest.MockedFunction<ReleaseFn>
  var forceConnectError = false
  var DUMMY_POOL: PoolLike;
  beforeAll(function () {
    process.env.AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID || AWS_CREDS.aws_access_key_id;
    process.env.AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY || AWS_CREDS.aws_secret_access_key;
    DUMMY_POOL = {
      connect:function(fn:(err?:Error, client?:ClientLike, done?:ReleaseFn)=>void){
        if(forceConnectError){
          return fn(new Error('NO_CONNECTION'))
        }
        fn(undefined,{ query }, releaseClient,);
      }
    }
  });
  beforeEach(function (){
    mS3Instance.upload = jest.fn().mockReturnValue(managedUpload)
    mS3Instance.deleteObjects = jest.fn(function(params: AWS.S3.DeleteObjectsRequest){
      return {
        promise:jest.fn().mockResolvedValue({} as AWS.S3.DeleteObjectsOutput)
      } as unknown as AWS.Request<AWS.S3.DeleteObjectsOutput, AWS.AWSError>
    }) as unknown as jest.MockedFunction<AWS.S3['deleteObjects']>
    managedUpload.promise = jest.fn().mockResolvedValue({} as AWS.S3.ManagedUpload.SendData)
    query = jest.fn() as jest.MockedFunction<QueryFn>
    query.mockResolvedValue({rows:[]})
    releaseClient = jest.fn()
    options = {
      ...createDefaults(),
     // s3Object:mS3Instance as AWS.S3,
      redshiftPool:DUMMY_POOL,
      table:'schema.table_name',
      bucket:'this-is-S3-bucket',
      bodies: [
        TEST_ROWS.filter((r) => r.id < 3)
          .map((r) => JSON.stringify(r))
          .join('\n'),
        TEST_ROWS.filter((r) => r.id >= 3)
          .map((r) => {
            r.text = 'UPSERT';
            return JSON.stringify(r);
          })
          .join('\n'),
      ]
    }
  })
  it('Inserts Bodies And Deletes', async function(done){
    let ld = new RedshiftLoader(options);
    let uploadManifest = jest.spyOn(ld, 'uploadManifest');
    await ld.insert();
    expect(uploadManifest).toHaveBeenCalledTimes(1);
    expect(mS3Instance.upload.mock.calls.length).toEqual(3);
    return done()
  });
  it('Debugging Throws no errors', async function (done){
    options = {
      ...options, debug:true
    }
    let log = console.log
    console.log = jest.fn()
    let ld = new RedshiftLoader(options);
    ld.debug = jest.fn()
    await ld.insert()
    expect(managedUpload.promise.mock.calls.length).toEqual(3);
    expect(mS3Instance.deleteObjects.mock.calls.length).toEqual(1);
    console.log = log
    return done()
  });
  it('s3Cleanup Called Once on SUCCESS', async function(done){
    await new RedshiftLoader(options).insert()
    expect(mS3Instance.deleteObjects.mock.calls.length).toEqual(1);
    return done()
  });
  it('NEVER calls s3Cleanup', async function(done){
    options.s3Cleanup = 'NEVER';
    await new RedshiftLoader(options).insert()
    expect(managedUpload.promise.mock.calls.length).toEqual(3);
    expect(mS3Instance.deleteObjects.mock.calls.length).toEqual(0);
    return done()
  });
  it('Add File Increases uploads', async function(done){
    options.s3Cleanup = 'NEVER';
    let rl = new RedshiftLoader(options);
    rl.addFile('');
    await rl.insert()
    expect(managedUpload.promise.mock.calls.length).toEqual(4);
    return done()
  });
  it('Gets Quailified Tables', function () {
    options.table =  { table: 'rs_loader', schema: 'schema1' }
    let ld = new RedshiftLoader(options);
    expect(ld.getQualifiedTable()).toEqual('schema1.rs_loader'); // defaults to 'table'
    expect(ld.getQualifiedTable('table')).toEqual('schema1.rs_loader');
    expect(ld.getQualifiedTable('loadingTable')).toEqual('schema1.rs_loader');
    options = {
      ...options,
      table: { table: 'rs_loader', schema: 'schema1' },
      loadingTable: { table: 'rs_loader_temp', schema: 'loading' },
    }
    ld = new RedshiftLoader(options);
    expect(ld.getQualifiedTable('loadingTable')).toEqual('loading.rs_loader_temp');
    options = {
      ...options,
      table: 'schema1.rs_loader',
      loadingTable: { table: 'rs_loader_temp', schema: 'loader' },
    }
    ld = new RedshiftLoader(options);
    expect(ld.getQualifiedTable('loadingTable')).toEqual('loader.rs_loader_temp');
  });
  it('Gets Quailified Tables only accepts certain params', function(){
    options = {
      ...options,
      table: { table: 'rs_loader', schema: 'schema1' },
    }
    let ld = new RedshiftLoader(options);
    expect(ld.getQualifiedTable()).toEqual('schema1.rs_loader');
    ld = new RedshiftLoader(options);
    expect(() => ld.getQualifiedTable('LEMON')).toThrow('Invalid tableType');
  });
  it('Accepts a  or string as filePrefix', function () {
    let params: RSLoaderOptions = {
      ...options,
      filePrefix: (rs: RedshiftLoader) => '/' + rs.getQualifiedTable('table').replace('.', '/'),
      table: { table: 'rs_loader', schema: 'schema1' },
    }
    let ld = new RedshiftLoader(params);
    expect(ld.getFilePrefix()).toEqual('/schema1/rs_loader');
    params = {
      ...options,
      filePrefix: 'lemon/',
      table: { table: 'rs_loader', schema: 'schema1' },
    }
    ld = new RedshiftLoader(params);
    expect(ld.getFilePrefix()).toEqual('lemon/');
  });
});
