const assert = require('assert');
function breakdownQueries(queries){
	let output = [];
	queries.forEach(function(q){
		q = q.split(';').filter(b=>b.trim()).map(el=>el+';');
		output = output.concat(q)
	})
	return output
}
function transactionQuery(rsPool,queries,cleanUpQuery){
	assert(rsPool,"Pool required")
	if(typeof rsPool.connect == 'function'){
		let poolObject = rsPool
		rsPool = function(fn){
			poolObject.connect(fn);
		};
	};
	assert(typeof rsPool == 'function',"Pool must be a fn or pg Pool")
	return runTransactionQuery(rsPool,queries,cleanUpQuery,0);
}
async function runTransactionQuery(rsPool,queries,cleanUpQuery,retries){
	retries = retries || 0;
	let maxRetries = 4;
	//queries = breakdownQueries(queries);
	let clientObject = await getClientFromPool(rsPool)
	try{
		let i = 0;
		for(q of queries){
			try{
				await queryClient(clientObject,q);
			}catch(err){
				err = new Error(err.message || err);
				err.query = q;
				err.step = i;
				err.retries = retries;
				throw err;
			}
			i++;
		}
		clientObject._releaseClient();
		return 'SUCCESS';
	}catch(err){
		console.log(err,'RS LOAD ERR');
		await queryClient(clientObject,'ROLLBACK;');
		clientObject._releaseClient(err);
		if(cleanUpQuery){
			clientObject = await getClientFromPool(rsPool)
			await queryClient(clientObject,cleanUpQuery);
			clientObject._releaseClient();
		}
		
		let message = err.message || err.details || err;
		if( /'stl_load_errors'/.test(message) )
		{
			try{
				const errQ = 'SELECT * FROM stl_load_errors ORDER BY starttime DESC LIMIT 1';
				clientObject = await getClientFromPool(rsPool)
				let rows = await queryClient(clientObject,errQ);
				clientObject._releaseClient();
				let e = new Error('RS_COPY_ERROR');
				e.details = rows[0];
				return Promise.reject(e);
			}catch(err){
				clientObject._releaseClient(err); // removes from pool in pg
				return Promise.reject(err);
			}
		}
		else if(/serializable isolation violation on table/i.test(message) && retries < maxRetries)
		{
			await wait(1000 * Math.pow(3, retries) );
			console.warn('RETRYING QUERY',retries)
			retries++;
			return runTransactionQuery(rsPool,queries,cleanUpQuery,retries)
		};
		return Promise.reject(err);
	};
}
function wait(time){
	return new Promise(function(resolve){
		setTimeout(resolve,time);
	})
}
function queryClient(client,query)
{
	assert(client,'client Required');
	return new Promise(function(resolve,reject)
	{
		client.query(query,function(err,result){
			if(err) return reject(err);
			if(result.rows instanceof Array)
			{
				return resolve(result.rows);
			}
			if(result instanceof Array){
				return resolve(result);
			};
			return reject("Query result is not an array and doesnt contain result.rows ");
		});
	});	
}
function getClientFromPool(pool){
	return new Promise(function(resolve,reject)
	{
		pool(function(err,client,done)
		{
			if(err) return reject(err);
			assert(typeof client.query == 'function',"Client Object from Pool must have a query fn");
			client._releaseClient = done || function(){};
			return resolve(client);
		})
	});
}

function mergeOptionsAndDefaults(options,defaultConfig)
{
	for(var el in options)
	{
		if(typeof options[el] == 'object' && !(options[el] instanceof Array)){
			options[el] = Object.assign({}, defaultConfig[el] || {},options[el]);
		}
	};
	options = Object.assign({},defaultConfig,options);
	return options;
}

module.exports = {
	transactionQuery:transactionQuery,
	mergeOptionsAndDefaults:mergeOptionsAndDefaults
}


