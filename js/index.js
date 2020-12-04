const fs = require('fs');
const dgraph = require('dgraph-js');
const grpc = require('grpc');
const readline = require('readline');
const twitter = require('twitter');

// Twitter credentials
const creds = require('./credentials.json');

// Global constants
const ALPHA_ADDR = process.env.ALPHA_ADDR || "localhost:9080";
const LOG_INTERVAL_TIME = process.env.LOG_INTERVAL_TIME || 2000;
const LOG_INTERVAL_TIME_IN_SECONDS = LOG_INTERVAL_TIME/1000;
const TWEETS_DATA_PATH = process.env.TWEETS_DATA_PATH;
const startStatus = Date.now();

// Global Variables
let retry = true;
let failures = 0;
let totalTweets = 0;
let commits = 0;
let oldCommits = 0;
let retries = 0;
let errors = 0;
let connectionFailed = false;
let reportStatsIntervalId = 0;
let errMsg, prevErrMsg;

// Set Dgraph client and Dgraph client stub
const dgraphClientStub = new dgraph.DgraphClientStub(ALPHA_ADDR, grpc.credentials.createInsecure());
const dgraphClient = new dgraph.DgraphClient(dgraphClientStub);

// Set the schema for types: Tweet and User
async function setSchema() {
  const schema = `
    type Tweet {
      id_str
      created_at
      message
      urls
      hashtags
      author
      mention
      retweet
    }

    type User {
      user_id
      user_name
      screen_name
      description
      friends_count
      followers_count
      verified
      profile_banner_url
      profile_image_url
    }

    user_id: string @index(exact) @upsert .
    user_name: string @index(hash) .
    screen_name: string @index(term) .
    description: string .
    friends_count: int .
    followers_count: int .
    verified: bool .
    profile_banner_url: string .
    profile_image_url: string .
    id_str: string @index(exact) @upsert .
    created_at: dateTime @index(hour) .
    message: string .
    urls: [string] .
    hashtags: [string] @index(exact) .
    author: uid @count @reverse .
    mention: [uid] @reverse .
    retweet: bool .
  `;
  const op = new dgraph.Operation();
  op.setSchema(schema)
  await dgraphClient.alter(op);
}

// Upsert Tweet JSON data into Dgraph
async function upsertData(jsonObj, query) {
  // create a new transaction
  const txn = dgraphClient.newTxn();
  try {
    // create a mutation of the JSON object
    const mu = new dgraph.Mutation();
    mu.setSetJson(jsonObj);

    // create a request for the upsertion
    const req = new dgraph.Request();
    req.setMutationsList([mu]);
    req.setQuery(query);
    req.setCommitNow(true);

    // perform the upsert with doRequest
    await txn.doRequest(req);

    // successfully committed to Dgraph
    commits += 1;
    // in case resuming from connection failure, we restart report stats
    if (connectionFailed) {
      connectionFailed = false;
      reportStatsIntervalId = setInterval(reportStats, LOG_INTERVAL_TIME);
    }
  } catch (err) {
    errMsg = err.message;
    if (errMsg.includes('connection refused')) {
      // wait for alpha to restart
      console.log('ERROR Connection refused... waiting a bit');
      await wait(5000);
    } else if (errMsg.includes('already been committed or discarded')) {
      // failed to upsert; transaction already commited or discarded
      failures += 1;
    } else if (retry && errMsg.includes('Please retry')) {
      // wait and retry upsert for only one more time
      retries += 1;
      await wait(200);
      retry = false;
      await upsertData(jsonObj, query);
    } else if (errMsg.includes("failed to connect to all addresses")) {
      // we only print log for connection failure the first time
      if (!connectionFailed) {
        clearInterval(reportStatsIntervalId);
        connectionFailed = true;
        console.log(`Error: Failed to connect to all addresses.\nIs Dgraph running at address ${ALPHA_ADDR}?`);
      }
      // wait for a while and retry
      await wait(500);
      await upsertData(jsonObj, query);
    } else if (errMsg.includes("Stream removed")) {
      console.log(`Error: Stream removed.\nIs Dgraph running at address ${ALPHA_ADDR}? Please try again.`);
      process.exit(1);
    } else {
      // unable to upsert
      errors += 1;
      if (errMsg !== prevErrMsg) {
        console.log(`ERROR Unable to commit.\n${errMsg}\n`);
        prevErrMsg = errMsg;
      }
    }
  } finally {
    await txn.discard();
  }
}

// Filter the Tweet
async function filterTweet(tweet) {
  // create constants for filtered tweet object
  const hashtags = [];
  const userMentions = [];
  const usersObject = [];
  // assign `uid(u)` for the author id
  usersObject[tweet.user.id_str] = 'uid(u)';
  // assign `uid(mK)` for the mentioned user ids
  // or `uid(u)` if the author himself is mentioned
  tweet.entities.user_mentions.forEach((element, index) => {
    let uid;
    if (usersObject[element.id_str] != undefined) {
      uid = usersObject[element.id_str];
    } else {
      uid = `uid(m${index+1})`;
      usersObject[element.id_str] = uid;
    }
    userMentions.push({
      'uid': uid,
      'user_id': element.id_str,
      'dgraph.type': 'User',
      'user_name': element.name,
      'screen_name': element.screen_name,
    });
  });
  // extract hashtags and store them in an array
  tweet.entities.hashtags.forEach((element) => {
    hashtags.push(element.text);
  });
  // create author object
  const authorObj = {
    'uid': 'uid(u)',
    'user_id': tweet.user.id_str,
    'dgraph.type': 'User',
    'user_name': tweet.user.name,
    'screen_name': tweet.user.screen_name,
    'description': tweet.user.description,
    'friends_count': tweet.user.friends_count,
    'followers_count': tweet.user.followers_count,
    'verified': tweet.user.verified,
    'profile_banner_url': tweet.user.profile_banner_url,
    'profile_image_url': tweet.user.profile_image_url,
  };
  // create tweet object
  const tweetObj = {
    'uid': 'uid(t)',
    'id_str': tweet.id_str,
    'dgraph.type': 'Tweet',
    'created_at': new Date(tweet.created_at),
    'message': tweet.text,
    'urls': tweet.urls,
    'hashtags': hashtags,
    'mention': userMentions,
    'author': authorObj,
  };
  return tweetObj;
}

// Build the query to be used for upsert
async function buildQuery(tweet) {
  // create constants for building upsert query
  const usersObject = [];
  const query = [
    `t as var(func: eq(id_str, "${tweet.id_str}"))`,
    `u as var(func: eq(user_id, "${tweet.author.user_id}"))`,
  ];
  // assign `u` for the author id
  usersObject[tweet.author.user_id] = 'u';
  // assign `mK` for the mentioned user ids
  tweet.mention.forEach((element, index) => {
    let name;
    if (usersObject[element.user_id] != undefined) {
      name = usersObject[element.user_id];
    } else {
      name = `m${index+1}`;
      query.push(`${name} as var(func: eq(user_id, ${element.user_id}))`);
      usersObject[element.user_id] = name;
    }
  });

  return `query {${query.join('\n')}}`;;
}

// Add Tweet data to Dgraph
async function addTweetData(tweet) {
  totalTweets += 1;
  const tweetObj = await filterTweet(tweet);
  const queries = await buildQuery(tweetObj);
  retry = true;
  await upsertData(tweetObj, queries);
}

// Report Stats of the tweet loader
function reportStats() {
  const now = Date.now();
  console.log(`STATS Tweets: ${totalTweets}, Failures: ${failures}, Retries: ${retries}, \
Errors: ${errors}, Commit Rate: ${Math.round((commits-oldCommits)/LOG_INTERVAL_TIME_IN_SECONDS)}, \
Uptime: ${Math.round((now - startStatus)/1000)}s`);
  oldCommits = commits;
}

// Wait function that takes time in milliseconds
async function wait(time) {
  return new Promise((resolve) => setTimeout(resolve, time));
}

async function main() {
  // set Dgraph schema
  await setSchema();
  // report stats in specific intervals
  reportStatsIntervalId = setInterval(reportStats, LOG_INTERVAL_TIME);
  if (TWEETS_DATA_PATH === undefined) {
    // create twitter client
    const client = new twitter(creds);
    // fetch tweets from the twitter stream
    client.stream('statuses/sample.json', function(stream) {
      stream.on('data', async function(tweet) {
        await addTweetData(tweet);
      });
      stream.on('error', function(err) {
        errors += 1;
        console.log(err.message);
      });
    });
  } else {
    const fileStream = fs.createReadStream(TWEETS_DATA_PATH);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });
    // Note: we use the crlfDelay option to recognize all instances of CR LF
    // ('\r\n') in input.txt as a single line break.

    rl.on('line', async function(tweet) {
      try {
        // Each line in input.txt will be successively available here as `tweet`.
        await addTweetData(JSON.parse(tweet));
      } catch (err) {
        // Pass when parsing error occurs
        errMsg = err.message;
        errors += 1;
        if (errMsg !== prevErrMsg) {
          console.log(`Parse Error: \n${errMsg}\n`);
          prevErrMsg = errMsg;
        }
      }
      // adding delay to avoid JS heap OOM
      await wait(500);
    })
    .on('close', function() {
      // report stats for the final time and stop
      reportStats();
      clearInterval(reportStatsIntervalId);
    });
  }
}

main().then(() => {
  console.log(`\nReporting stats every ${LOG_INTERVAL_TIME_IN_SECONDS} seconds\n`)
}).catch((e) => {
  console.log(e);
});
