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

// Set Dgraph client and Dgraph client stub
const dgraphClientStub = new dgraph.DgraphClientStub(ALPHA_ADDR, grpc.credentials.createInsecure());
const dgraphClient = new dgraph.DgraphClient(dgraphClientStub);

// Set the schema for types: Tweet and User
async function setSchema() {
  const schema = `
    type Tweet {
      id_str: string
      created_at: dateTime
      message: string
      urls: [string]
      hashtags: [string]
      author: User
      mention: [User]
      retweet: bool
    }

    type User {
      user_id: string
      user_name: string
      screen_name: string
      description: string
      friends_count: int
      followers_count: int
      verified: bool
      profile_banner_url: string
      profile_image_url: string
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
  } catch (err) {
    const errMsg = err.message;
    if (errMsg.includes('connection refused')) {
      // wait for alpha to restart
      console.log('ERROR Connection refused... waiting a bit');
      await wait(5000);
    } else if (errMsg.includes('already been committed or discarded')) {
      // failed to upsert; transaction already commited or discarded
      failures += 1;
    } else if (retry && errMsg.includes('Please retry')) {
      // retry upsert once again after 100ms wait
      retries += 1;
      await wait(100);
      retry = false;
      await upsertData(jsonObj, query);
    } else {
      // unable to upsert
      errors += 1;
      console.log(`ERROR Unable to commit.\n${err}\n`);
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
  commits += 1;
}

// Report Stats of the tweet loader
function reportStats() {
  const now = Date.now();
  console.log(`STATS Tweets: ${totalTweets}, Failues: ${failures}, Retries: ${retries}, \
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
  setInterval(reportStats, LOG_INTERVAL_TIME);
  console.log(TWEETS_DATA_PATH);
  if (TWEETS_DATA_PATH === undefined) {
    // create twitter client
    const client = new twitter(creds);
    // fetch tweets from the twitter stream
    client.stream('statuses/sample.json', function(stream) {
      stream.on('data', async function(tweet) {
        await addTweetData(tweet);
      });
      stream.on('error', function(error) {
        errors += 1;
        console.log(error);
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

    for await (const tweet of rl) {
      try {
        // Each line in input.txt will be successively available here as `tweet`.
        await addTweetData(JSON.parse(tweet));
      } catch (error) {
        // Ignore when parsing error occurs
        errors += 1;
        console.log(error);
      }
    }
  }
}

main().then(() => {
  console.log(`\nReporting stats every ${LOG_INTERVAL_TIME_IN_SECONDS} seconds\n`)
}).catch((e) => {
  console.log(e);
});
