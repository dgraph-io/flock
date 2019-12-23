const dgraph = require('dgraph-js');
const grpc = require('grpc');
const fetcher = require('./fetcher');

// Global constants
const ALPHA_ADDR = process.env.ALPHA_ADDR || "localhost:9080"
const LOG_INTERVAL_TIME = process.env.LOG_INTERVAL_TIME || 2000;
const LOG_INTERVAL_TIME_IN_SECONDS = LOG_INTERVAL_TIME/1000;
const startStatus = Date.now();

// Global Variables
let successes = 0;
let failures = 0;
let oldSuccesses = 0;
let errMsg, prevErrMsg;

// Set Dgraph client and Dgraph client stub
const dgraphClientStub = new dgraph.DgraphClientStub(ALPHA_ADDR, grpc.credentials.createInsecure());
const dgraphClient = new dgraph.DgraphClient(dgraphClientStub);

// Generic Query class implementation
class Query {
    // constructor takes in the query number(index) to set two of the queries and reference
    constructor(index) {
        this.query1 = fetcher.paramsQuery(index);
        this.query2 = fetcher.runQuery(index);
        this.reference = fetcher.reference(index);
    }
    
    // this function usually obtains the parameters needed for the runQuery function except for few cases
    async getParams() {
        // skip when query is null
        if(this.query1 == null) {
            return;
        }

        // run the query
        const data = await queryData(this.query1);
        // check response mismatch
        if (data.hasOwnProperty('dataquery') == false) {
            console.log(`dataquery key not found in the response of the query:\n${this.query1}\n`);
            failures += 1;
            return false;
        }
        // check empty response
        if (data.dataquery.length < 0) {
            console.log(`Empty response returned from Dgraph for query:\n${this.query1}\n`);
            failures += 1;
            return false;
        }
        // extract the data from object arrays to arrays
        this.params = data.dataquery.map(element => element[this.reference]);
        successes += 1;
        return true;
    }

    // runQuery function 
    async runQuery() {
        // skip when query is null
        if(this.query2 == null) {
            return;
        }

        let data, variable;
        // run the query - if params is defined, pass variables
        if (this.params == undefined) {
            data = await queryData(this.query2);
        } else {
            variable = this.params[fetcher.getRandomInt(this.params.length)];
            if (variable != undefined && this.reference === "hashtags") {
                variable = variable[fetcher.getRandomInt(variable.length)];
            }
            data = await queryData(this.query2, { "$var": variable });
        }
        // check response mismatch
        if (data.hasOwnProperty('dataquery') === false) {
            console.log(`dataquery key not found in the response of the query:\n${this.query2}\nvariables:\t$var = ${variable}`);
            failures += 1;
            return false;
        }
        // check empty response
        if (data.dataquery.length <= 0) {
            console.log(`Empty response returned from Dgraph for query:\n${this.query2}\nvariables:\n$var = ${variable}`);
            failures += 1;
            return false;
        }
        successes += 1;
        return true;
    }
}

// Creating an array of query instances
const queryArray = [];
for (let i=1; i<10; i++) {
    queryArray.push(new Query(i));
}

// Query tweet data in Dgraph
async function queryData(query, vars) {
    // create a transaction
    const txn = dgraphClient.newTxn({ readOnly: true });
    try {
        // quering dgraph with vars and returning the response JSON
        return (await txn.queryWithVars(query, vars)).getJson();
    } catch (err) {
        failures += 1;
        errMsg = err.message;
        if (errMsg.includes("Stream removed")) {
            console.log(`Error: Stream removed.\nIs Dgraph running at address ${ALPHA_ADDR}? Please try again.`);
            process.exit(1);
        } else {
            // unable to query
            if (errMsg !== prevErrMsg) {
                console.log(`ERROR Unable to query.\n${errMsg}\n`);
                prevErrMsg = errMsg;
            }
        }
    }
}

// Report Stats of the tweet loader
function reportStats() {
    console.log(`STATS \tSuccess: ${successes}, \tFailures: ${failures}, \
 \tQuery Rate: ${Math.round((successes-oldSuccesses)/LOG_INTERVAL_TIME_IN_SECONDS)}, \
 \tUptime: ${Math.round((Date.now() - startStatus)/1000)}s`);
    oldSuccesses = successes;
}
  
// Wait function that takes time in milliseconds
async function wait(time) {
    return new Promise((resolve) => setTimeout(resolve, time));
}

// Run getParams and runQuery functions for the query instance passed in the function parameter
async function runQueries(query) {
    await query.getParams();
    await query.runQuery();
}

async function main() {
    // report stats in specific intervals
    setInterval(reportStats, LOG_INTERVAL_TIME);

    // infinitely randomly pick queries to run
    for (;;) {
        runQueries(queryArray[fetcher.getRandomInt(queryArray.length)]);

        // adding delay to avoid JS heap OOM due to the infinite loop
        await wait(200);
    };
}
  
main().catch((e) => {
    console.log("ERROR: ", e);
});