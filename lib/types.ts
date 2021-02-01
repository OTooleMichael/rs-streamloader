import AWS from 'aws-sdk';
import { PoolConfig } from 'pg';
import { Readable } from 'stream';
export enum UploadType {
  INSERT = 'INSERT',
  UPSERT = 'UPSERT',
  TRUNCATE_INSERT = 'TRUNCATE_INSERT',
}
export type AWSCredentials = {
  accessKeyId?: string;
  secretAccessKey?: string;
  sessionToken?: string;
  role?: string;
};
export type TableName =
  | string
  | {
      table: string;
      schema: string;
    };
export type UploadBody = Readable | string | Buffer;
interface CopySettingsBase {
  maxError: number;
  gzip: boolean;
  encoding: 'UTF8' | 'UTF16' | 'UTF16LE' | 'UTF16BE';
  ignoreHeader: number | boolean;
  timeFormat: 'auto' | 'epochsecs' | 'epochmillisecs' | string;
  truncateCols: boolean;
}
interface CopySettingsJSONAuto extends CopySettingsBase {
  format: 'JSON' | 'AVRO';
  columnMap: 'auto' | 'auto ignorecase';
}
interface CopySettingsJSONPath extends CopySettingsBase {
  format: 'JSON' | 'AVRO';
  columnMap: 'jsonpaths';
  jsonpaths: string;
}
type CopySettingsJSON = CopySettingsJSONAuto | CopySettingsJSONPath;
interface CopySettingsCSV extends CopySettingsBase {
  format: 'CSV';
  quote: 'string';
  delimiter: 'string';
}
interface CopySettingsOther extends CopySettingsBase {
  format: 'PARQUET' | 'ORC';
}
export type CopySettings = CopySettingsJSON | CopySettingsCSV | CopySettingsOther;
export interface FactoryOptions {
  idField: string;
  removeTempTable: boolean;
  awaitS3Cleanup: boolean;
  filePrefix: string | ((options: any) => string);
  aws?: AWSCredentials;
  s3Object?: AWS.S3;
  redshiftCreds?: PoolConfig;
  redshiftPool?: PoolLike;
  s3Settings?: AWS.S3.ClientConfiguration; // passed directly to new AWS.S3();
  bucket?: string;
  loadingTable?: TableName;
  s3Cleanup: true | 'ALWAYS' | false | 'NEVER' | 'SUCCESS';
  copySettings: CopySettings;
  debug?: boolean;
}
export type DefaultOptionInputs = Omit<Partial<FactoryOptions>,'copySettings'> & {copySettings?:Partial<CopySettings>};
export interface RSLoaderOptions extends DefaultOptionInputs {
  table: TableName;
  bodies?: UploadBody[];
  body?: UploadBody;
  bucket: string;
}
export type ReleaseFn = (release?: any) => void;
export type Rows = Record<string, any>[];
export interface QueryResLike {
  rows: Rows;
}
export interface ClientLike {
  query: (sql: string) => Promise<QueryResLike>;
  [x: string]: any;
}
export interface PoolLike {
  connect(callback: (err?: Error, client?: ClientLike, done?: ReleaseFn) => void): void;
  [x: string]: any;
}
