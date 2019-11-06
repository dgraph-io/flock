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

// Set Dgraph client and Dgraph client stub
const dgraphClientStub = new dgraph.DgraphClientStub(ALPHA_ADDR, grpc.credentials.createInsecure());
const dgraphClient = new dgraph.DgraphClient(dgraphClientStub);

// Generic Query class implementation
class Query {
    // constructor takes in the query number(index) to set two of the queries and reference
    constructor(index) {
        this.query1 = fetcher.query1(index);
        this.query2 = fetcher.query2(index);
        this.reference = fetcher.ref(index);
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

        let data;
        // run the query - if params is defined, pass variables
        if (this.params == undefined) {
            data = await queryData(this.query1);
        } else {
            data = await queryData(this.query1, { "$var": this.params });
        }
        // check response mismatch
        if (data.hasOwnProperty('dataquery') == false) {
            console.log(`dataquery key not found in the response of the query:\n${this.query2}\n`);
            failures += 1;
            return false;
        }
        // check empty response
        if (data.dataquery.length < 0) {
            console.log(`Empty response returned from Dgraph for query:\n${this.query2}\n`);
            failures += 1;
            return false;
        }
        successes += 1;
        return true;
    }
}

// Creating query instances
const queryOne = new Query(1);
const queryTwo = new Query(2);
const queryThree = new Query(3);
const queryFour = new Query(4);
const queryFive = new Query(5);
const querySix = new Query(6);
const querySeven = new Query(7);
const queryEight = new Query(8);
const queryNine = new Query(9);

// Query tweet data in Dgraph
async function queryData(query, vars) {
    // create a transaction
    const txn = dgraphClient.newTxn({ readOnly: true });
    // create a response object
    let response;
    // if variable is defined 
    if (vars == undefined) {
        response = await txn.query(query);
    } else {
        // quering with vars
        response = await txn.queryWithVars(query, vars);
    }
    return response.getJson();
}

// Report Stats of the tweet loader
function reportStats() {
    const now = Date.now();
    console.log(`STATS \tSuccess: ${successes}, \tFailures: ${failures}, \
 \tQuery Rate: ${Math.round((successes-oldSuccesses)/LOG_INTERVAL_TIME_IN_SECONDS)}, \
 \tUptime: ${Math.round((now - startStatus)/1000)}s`);
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

    // arrays of queries
    const queries = [
        queryOne,
        queryTwo,
        queryThree,
        queryFour,
        queryFive,
        querySix,
        querySeven,
        queryEight,
        queryNine,
    ];

    // infinitely run queries in circle
    for (let i = 0;; i++) {
        runQueries(queries[i]);
        if(i < queries.length) {
            i = 0;
        }
        // adding delay to avoid JS heap OOM due to the infinite loop
        await wait(100);
    };
}
  
main().then().catch((e) => {
    console.log("ERROR: ", e);
});