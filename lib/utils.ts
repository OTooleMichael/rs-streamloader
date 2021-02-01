import assert from 'assert';
import { AWSCredentials, CopySettings, DefaultOptionInputs, FactoryOptions, RSLoaderOptions, TableName } from './types';
interface LoaderErrorDetails {
  query?: string;
  step?: number;
  retries?: number;
  details?: any;
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
