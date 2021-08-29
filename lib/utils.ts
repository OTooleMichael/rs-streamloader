import assert from 'assert';
import {Stream, Readable, Transform} from 'stream'
import {
  S3UploadBody,
  UploadBody, AWSCredentials, CopySettings,
  DefaultOptionInputs, FactoryOptions, RSLoaderOptions,
  TableName
} from './types';
interface LoaderErrorDetails {
  query?: string;
  step?: number;
  retries?: number;
  details?: any;
}
type RowObject = Record<string, any>;
export function isObjectStream(obj: Readable | Stream | any): boolean{
  if(!(obj instanceof Stream)){
    return false
  }
  // @ts-ignore
  return typeof obj._read === 'function' && obj.readableObjectMode
}
function toJSONLine(obj: RowObject){
  return Buffer.from(JSON.stringify(obj) + '\n', 'utf-8')
}
export function toNewLineJSON(stream: Readable){
  const tx = new Transform({
    writableObjectMode:true,
    readableObjectMode:false,
    transform(obj: RowObject, encoding, callback) {
      return callback(null, toJSONLine(obj))
    }
  });
  return stream.pipe(tx)
}

export function generatorToReadable<T extends RowObject = RowObject>(
  generator: AsyncGenerator<T>,
): Readable {
  const stream = new Readable({
      objectMode: false,
      read:async function(){
          try{
              while(true){
                  let result = await generator.next()
                  if(result.done){
                    this.push(null)
                    break
                  }
                  const value = toJSONLine(result.value);
                  if(!this.push(value)){
                      break
                  }
              }
          }catch(error){
              this.emit('error', error)
          }
      }
  })
  return stream;
}

export function ensureS3BodyAcceptable(obj: UploadBody): S3UploadBody {
  if(typeof obj === 'string' || obj instanceof Buffer){
    return obj
  }
  if(obj instanceof Readable && isObjectStream(obj)){
    return toNewLineJSON(obj)
  }
  const isAsnycIterator = obj[Symbol.asyncIterator] !== undefined
  const hasPipeFn = typeof (obj as Readable).pipe === 'function'
  // it is an async generator
  if(isAsnycIterator && !hasPipeFn){
    return generatorToReadable(obj as unknown as AsyncGenerator<RowObject>)
  }
  return obj as Readable
}

export class LoaderError extends Error {
  query?: string;
  step?: number;
  retries?: number;
  details?: any;
  constructor(message: string, extraDetails: LoaderErrorDetails = {}) {
    super(message);
    const { step, retries, details } = extraDetails;
    let { query } = extraDetails;
    if (query) {
      query = query.trim().replace(/CREDENTIALS [^\n]+/, "CREDENTIALS ='SECRET_XXXX'");
    }
    this.query = query;
    this.step = step;
    this.retries = retries;
    this.details = details;
  }
}
export function createDefaults(): FactoryOptions {
  return {
    idField: 'id',
    removeTempTable: true,
    awaitS3Cleanup: true,
    filePrefix: 'rs-streamloader/unnamed',
    s3Cleanup: 'SUCCESS',
    copySettings: {
      maxError: 0,
      gzip: false,
      timeFormat: 'auto',
      format: 'JSON',
      columnMap: 'auto',
      encoding: 'UTF8',
      ignoreHeader: false,
      truncateCols: true,
    },
  };
}

export function mergeOptions<T extends FactoryOptions | RSLoaderOptions = FactoryOptions>(
  options: DefaultOptionInputs,
  defaultOptions: DefaultOptionInputs = {},
): T {
    const copySettings = options.copySettings || {} as Partial<CopySettings>;
    const defaultCopySettings = defaultOptions.copySettings || {} as Partial<CopySettings>;
    delete options.copySettings;
    const output: T = {
    ...createDefaults(),
    ...defaultOptions,
    ...options
    } as T

    output.copySettings = {
        ...createDefaults().copySettings,
        ...defaultCopySettings,
        ...copySettings,
    } as CopySettings;
    return output;
}
export function parseRsTables(tableParams: TableName): { table: string; schema: string } {
  if (typeof tableParams === 'string') {
    if (/\./.test(tableParams)) {
      const prts = tableParams.split('.');
      return { schema: prts[0], table: prts[1] };
    } else {
      return { table: tableParams, schema: '' };
    }
  }
  return tableParams;
}

export function createCopyCredString(aws: AWSCredentials): string {
  const { role, accessKeyId, secretAccessKey, sessionToken } = aws;
  if (role) {
    return `CREDENTIALS AS 'aws_iam_rol=${role}'`;
  }
  assert(accessKeyId, 'AWS aws_access_key_id is required');
  assert(secretAccessKey, 'AWS aws_secret_access_key is required');
  if (sessionToken) {
    return `CREDENTIALS AS 'aws_access_key_id=${accessKeyId};aws_secret_access_key=${secretAccessKey};token=${sessionToken}'`;
  }
  return `CREDENTIALS AS 'aws_access_key_id=${accessKeyId};aws_secret_access_key=${secretAccessKey}'`;
}
