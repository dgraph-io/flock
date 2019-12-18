const fs = require('fs');
const twitter = require('twitter');

// Twitter credentials
const creds = require('./credentials.json');

// Global constants
const LOG_INTERVAL_TIME = process.env.LOG_INTERVAL_TIME || 2000;
const LOG_INTERVAL_TIME_IN_SECONDS = LOG_INTERVAL_TIME/1000;
const startStatus = Date.now();

// Global Variables
let totalTweets = 0;
let totalErrors = 0;

// Report Stats of the tweet loader
function reportStats() {
  const now = Date.now();
  console.log(`STATS Tweets: ${totalTweets}\tErrors: ${totalErrors}\tUptime: ${Math.round((now - startStatus)/1000)}s`);
}

async function main() {
  // create twitter client
  const client = new twitter(creds);
  // report stats in specific intervals
  setInterval(reportStats, LOG_INTERVAL_TIME);

  // fetch tweets from the twitter stream
  client.stream('statuses/sample.json', function(stream) {
      stream.on('data', async function(tweet) {
        fs.appendFile('tweets.json', JSON.stringify(tweet)+"\n", function (err) {
            if (!err) {
                totalTweets += 1;
            } else {
                totalErrors += 1;
                console.log(error);
            }
        });
      });
      stream.on('error', function(error) {
        totalErrors += 1;
        console.log(error);
      });
  });
}

main().then(() => {
  console.log(`\nReporting stats every ${LOG_INTERVAL_TIME_IN_SECONDS} seconds\n`)
}).catch((e) => {
  console.log(e);
});
