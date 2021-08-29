import assert from 'assert';
import { PoolLike, ClientLike, Rows, ReleaseFn } from './types';
import { LoaderError } from './utils';
enum Result {
  SUCCESS = 'SUCCESS',
}
async function runTransactionQuery(
  rsPool: PoolLike,
  queries: string[],
  cleanUpQuery?: string,
  retries: number = 0,
): Promise<Result> {
  const maxRetries = 4;
  const [clientObject, releaseClient] = await getClientFromPool(rsPool);
  try {
    let i = 0;
    for (const q of queries) {
      try {
        await queryClient(clientObject, q);
      } catch (err) {
        const error = new LoaderError(err.message || err, {
          query: q,
          step: i,
          retries,
        });
        throw error;
      }
      i++;
    }
    releaseClient();
    return Result.SUCCESS;
  } catch (err) {
    await queryClient(clientObject, 'ROLLBACK;');
    releaseClient(err);
    if (cleanUpQuery) {
      await runQuery(rsPool, cleanUpQuery);
    }

    const message = err.message || err.details || err;
    if (/'stl_load_errors'/.test(message)) {
      try {
        const errQ = 'SELECT * FROM stl_load_errors ORDER BY starttime DESC LIMIT 1';
        const rows = await runQuery(rsPool, errQ);
        const error = new LoaderError('RS_COPY_ERROR', { details: rows[0] });
        return Promise.reject(error);
      } catch (err) {
        return Promise.reject(err);
      }
    } else if (/serializable isolation violation on table/i.test(message) && retries < maxRetries) {
      await wait(1000 * Math.pow(3, retries));
      retries++;
      return runTransactionQuery(rsPool, queries, cleanUpQuery, retries);
    }
    return Promise.reject(err);
  }
}
export function transactionQuery(rsPool: PoolLike, queries: string[], cleanUpQuery?: string): Promise<Result> {
  assert(rsPool, 'Pool required');
  return runTransactionQuery(rsPool, queries, cleanUpQuery, 0);
}
function wait(time: number = 200) {
  return new Promise((resolve) => {
    setTimeout(resolve, time);
  });
}
async function queryClient(client: ClientLike, sql: string): Promise<Rows> {
  assert(client, 'client Required');
  const result = await client.query(sql);
  if (result.rows instanceof Array) {
    return result.rows;
  }
  if (result instanceof Array) {
    return result as Rows;
  }
  throw new Error('Query result is not an array and doesnt contain result.rows ');
}
async function getClientFromPool(pool: PoolLike): Promise<[ClientLike, ReleaseFn]> {
  return new Promise((resolve, reject) => {
    pool.connect(
      (
        err,
        client,
        done = function (error?: any) {
          return undefined;
        },
      ) => {
        if (err) return reject(err);
        if (!client) return reject(new Error('NO_CLIENT'));
        assert(typeof client.query === 'function', 'Client Object from Pool must have a query fn');
        return resolve([client, done]);
      },
    );
  });
}
async function runQuery(pool: PoolLike, sql: string): Promise<Rows> {
  const [client, release] = await getClientFromPool(pool);
  try {
    const res = await queryClient(client, sql);
    return res;
  } catch (err) {
    throw err;
  } finally {
    release();
  }
}
