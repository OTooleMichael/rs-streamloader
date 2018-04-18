const assert = require('assert');
function breakdownQueries(queries){
	let output = [];
	queries.forEach(function(q){
		q = q.split(';').filter(b=>b.trim()).map(el=>el+';');
		output = output.concat(q)
	})
	return output
}
function transactionQuery(rsPool,queries,retries){
	retries = retries || 0;
	let maxRetries = 4;
	assert(rsPool,"Pool required")
	if(typeof rsPool.connect == 'function'){
		let poolObject = rsPool
		rsPool = function(fn){
			poolObject.connect(fn);
		};
	};
	assert(typeof rsPool == 'function',"Pool must be a fn or pg Pool")
	//queries = breakdownQueries(queries);
	let clientObject = null;
	let chain = getClientFromPool(rsPool)
	.then(function(client){
		clientObject = client;
		// release globally
		return 
	})

	queries.forEach(function(q){
		chain = chain.then(function(res){
			return queryClient(clientObject,q);
		})
	});

	chain = chain.then(function(rows){
		clientObject._releaseClient();
		return Promise.resolve('SUCCESS');
	})
	.catch(function(err)
	{
		let Q = queryClient(clientObject,'ROLLBACK;')
		.then(function(){
			clientObject._releaseClient(err);
			return getClientFromPool(rsPool)
		})
		.then(function(client){
			clientObject = client;
		})
		

		let message = err.message || err.details || err;
		if( /'stl_load_errors'/.test(message) )
		{
			const errQ = 'SELECT * FROM stl_load_errors ORDER BY starttime DESC LIMIT 1';
			Q = Q.then(()=> queryClient(clientObject,errQ))
			.then(function(rows)
			{
				let e = new Error('RS_COPY_ERROR');
				e.details = rows[0];
				return Promise.reject(e);
			})
			.catch(function(err){
				clientObject._releaseClient(err); // removes from pool in pg
				return Promise.reject(err);
			});
		}
		else if( /serializable isolation violation on table/i.test(message) && retries < maxRetries)
		{
			Q = Q.catch((err)=> Promise.resolve("ERROR"))
			.then((err)=>clientObject._releaseClient(err === "ERROR"))
			.then(()=>wait(1000 * Math.pow(3, retries)))
			.then(()=>{
				console.warn('RETRYING QUERY',retries)
				retries++;
				return transactionQuery(rsPool,queries,retries)
			});
		}
		else
		{
			Q = Q.catch((err)=> Promise.resolve("ERROR"))
			.then((isError)=>{
				clientObject._releaseClient(isError === 'ERROR');
				return Promise.reject(message);
			});
		}
		return Q;
	});
	return chain
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


