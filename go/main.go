/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ChimeraCoder/anaconda"
	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	"github.com/dgraph-io/ristretto/z"
	"github.com/dustin/go-humanize"
	"google.golang.org/grpc"
)

const (
	cTimeFormat       = "Mon Jan 02 15:04:05 -0700 2006"
	cDgraphTimeFormat = "2006-01-02T15:04:05.999999999-07:00"

	cDgraphSchema = `
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
	`
)

var (
	opts  progOptions
	stats progStats
	fid   uint64

	errNotATweet      = errors.New("message in the stream is not a tweet")
	errShouldNotReach = errors.New("invariant failed to satisfy")
)

type twitterCreds struct {
	AccessSecret   string `json:"access_secret"`
	AccessToken    string `json:"access_token"`
	ConsumerKey    string `json:"consumer_key"`
	ConsumerSecret string `json:"consumer_secret"`
}

type progOptions struct {
	numClients       int
	numWriters       int
	credentialsFile  string
	dataFilesPath    string
	reportPeriodSecs int
	noCommitRatio    float64
	alphaSockAddr    []string
	outFilesPath     string
	fileSize         int
}

type progStats struct {
	TotalDownloaded uint64
	Tweets          uint32
	Commits         uint32
	LeakedCommits   uint32
	Retries         uint32
	Failures        uint32
	ErrorsJSON      uint32
	ErrorsDgraph    uint32
	NumFiles        uint32
}

type twitterUser struct {
	UID              string `json:"uid,omitempty"`
	DgraphType       string `json:"dgraph.type,omitempty"`
	UserID           string `json:"user_id,omitempty"`
	UserName         string `json:"user_name,omitempty"`
	ScreenName       string `json:"screen_name,omitempty"`
	Description      string `json:"description,omitempty"`
	FriendsCount     int    `json:"friends_count,omitempty"`
	FollowersCount   int    `json:"followers_count,omitempty"`
	Verified         bool   `json:"verified,omitempty"`
	ProfileBannerURL string `json:"profile_banner_url,omitempty"`
	ProfileImageURL  string `json:"profile_image_url,omitempty"`
}

type twitterTweet struct {
	UID        string        `json:"uid,omitempty"`
	DgraphType string        `json:"dgraph.type,omitempty"`
	IDStr      string        `json:"id_str"`
	CreatedAt  string        `json:"created_at"`
	Message    string        `json:"message,omitempty"`
	URLs       []string      `json:"urls,omitempty"`
	HashTags   []string      `json:"hashtags,omitempty"`
	Author     twitterUser   `json:"author"`
	Mention    []twitterUser `json:"mention,omitempty"`
	Retweet    bool          `json:"retweet"`
}

func buildQuery(tweet *twitterTweet) string {
	tweetQuery := `t as var(func: eq(id_str, "%s"))`
	userQuery := `%s as var(func: eq(user_id, "%s"))`

	query := make([]string, len(tweet.Mention)+2)

	query[0] = fmt.Sprintf(tweetQuery, tweet.IDStr)
	tweet.UID = "uid(t)"

	query[1] = fmt.Sprintf(userQuery, "u", tweet.Author.UserID)
	tweet.Author.UID = "uid(u)"

	usersMap := make(map[string]string)
	usersMap[tweet.Author.UserID] = "u"

	// We will query only once for every user. We are storing all the users in the map who
	// we have already queried. If a user_id is repeated, we will just use uid that we got
	// in the previous query.
	for i, user := range tweet.Mention {
		var varName string
		if name, ok := usersMap[user.UserID]; ok {
			varName = name
		} else {
			varName = fmt.Sprintf("m%d", i+1)
			query[i+2] = fmt.Sprintf("%s as var(func: eq(user_id, %s))", varName, user.UserID)
			usersMap[user.UserID] = varName
		}

		tweet.Mention[i].UID = fmt.Sprintf("uid(%s)", varName)
	}

	finalQuery := fmt.Sprintf("query {%s}", strings.Join(query, "\n"))
	return finalQuery
}

type writer struct {
	dir   string
	f     *os.File
	w     *gzip.Writer
	fsz   uint64
	maxSz uint64
}

func newWriter() *writer {
	var w writer
	w.maxSz = uint64(opts.fileSize)
	w.dir = opts.outFilesPath
	w.newFiles()
	return &w
}

func (w *writer) Write(buf []byte) (int, error) {
	if w.f == nil {
		w.newFiles()
	}

	sz, err := w.w.Write(buf)
	if err != nil {
		return sz, err
	}
	if atomic.AddUint64(&w.fsz, uint64(sz)) > w.maxSz {
		w.Finish()
		w.newFiles()
	}
	return sz, nil
}

func (w *writer) newFiles() {
	id := atomic.AddUint64(&fid, 1)
	fname := fmt.Sprintf("%06d", id)

	absDir, err := filepath.Abs(opts.outFilesPath)
	checkFatal(err, "absdir %s", absDir)
	path := path.Join(absDir, fname+gzFileSuffix)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	checkFatal(err, "Failed to open file %s", fname+gzFileSuffix)

	gw := gzip.NewWriter(f)
	gw.Header.Name = fname + jsonFileSuffix

	w.f, w.w, w.fsz = f, gw, 0
	atomic.AddUint32(&stats.NumFiles, 1)
}

func (w *writer) Finish() {
	if w.f == nil && w.w == nil {
		return
	}
	checkFatal(w.w.Flush(), "writer flush failed")
	checkFatal(w.w.Close(), "writer close failed")
	checkFatal(w.f.Close(), "file close failed")
}
func runWriter(c *z.Closer, tweets <-chan interface{}) {
	defer c.Done()
	w := newWriter()
	defer func() {
		w.Finish()
	}()
	for {
		select {
		case <-c.HasBeenClosed():
			return
		case jsn, more := <-tweets:
			if !more {
				return
			}
			var tweet anaconda.Tweet
			switch msg := jsn.(type) {
			case anaconda.Tweet:
				tweet = msg
			default:
				// Not a tweet
				atomic.AddUint32(&stats.ErrorsJSON, 1)
			}

			atomic.AddUint32(&stats.Tweets, 1)

			data, err := json.Marshal(tweet)
			checkFatal(err, "Json marshal failed for %+v", tweet)

			sz, err := w.Write(data)
			checkFatal(err, "failed to write %s", string(data))

			atomic.AddUint64(&stats.TotalDownloaded, uint64(sz))
		}
	}
}

func runInserter(alphas []api.DgraphClient, c *z.Closer, tweets <-chan interface{}) {
	defer c.Done()

	if tweets == nil {
		return
	}

	dgr := dgo.NewDgraphClient(alphas...)
	for {
		select {
		case <-c.HasBeenClosed():
			return

		case jsn, more := <-tweets:
			if !more {
				return
			}

			atomic.AddUint32(&stats.Tweets, 1)

			ft, err := filterTweet(jsn)
			if err != nil {
				atomic.AddUint32(&stats.ErrorsJSON, 1)
				continue
			}

			// Now, we need query UIDs and ensure they don't already exists
			txn := dgr.NewTxn()
			// txn is not being discarded deliberately
			// defer txn.Discard()

			queryStr := buildQuery(ft)

			tweet, err := json.Marshal(ft)
			if err != nil {
				atomic.AddUint32(&stats.ErrorsJSON, 1)
				continue
			}

			commitNow := true
			if rand.Float64() < opts.noCommitRatio {
				commitNow = false
			}

			// only ONE retry attempt is made
			retry := true
		RETRY:
			apiUpsert := &api.Request{
				Mutations: []*api.Mutation{
					&api.Mutation{
						SetJson: tweet,
					},
				},
				CommitNow: commitNow,
				Query:     queryStr,
			}
			_, err = txn.Do(context.Background(), apiUpsert)
			switch {
			case err == nil:
				if commitNow {
					atomic.AddUint32(&stats.Commits, 1)
				} else {
					atomic.AddUint32(&stats.LeakedCommits, 1)
				}
			case strings.Contains(err.Error(), "connection refused"):
				// wait for alpha to (re)start
				log.Printf("ERROR Connection refused... waiting a bit\n")
				time.Sleep(5 * time.Second)
			case strings.Contains(err.Error(), "already been committed or discarded"):
				atomic.AddUint32(&stats.Failures, 1)
			case retry && strings.Contains(err.Error(), "Please retry"):
				atomic.AddUint32(&stats.Retries, 1)
				time.Sleep(100 * time.Millisecond)
				retry = false
				goto RETRY
			default:
				atomic.AddUint32(&stats.ErrorsDgraph, 1)
				log.Printf("ERROR Unable to commit: %v\n", err)
			}
		}
	}
}

func filterTweet(jsn interface{}) (*twitterTweet, error) {
	var tweet anaconda.Tweet
	switch msg := jsn.(type) {
	case anaconda.Tweet:
		tweet = msg
	default:
		return nil, errNotATweet
	}

	x, err := json.Marshal(tweet)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(x))

	// fmt.Println(tweet)
	createdAt, err := time.Parse(cTimeFormat, tweet.CreatedAt)
	if err != nil {
		return nil, err
	}

	expandedURLs := make([]string, len(tweet.Entities.Urls))
	for _, url := range tweet.Entities.Urls {
		expandedURLs = append(expandedURLs, url.Expanded_url)
	}

	hashTagTexts := make([]string, 0)
	for _, tag := range tweet.Entities.Hashtags {
		if tag.Text != "" {
			hashTagTexts = append(hashTagTexts, tag.Text)
		}
	}

	var userMentions []twitterUser
	for _, userMention := range tweet.Entities.User_mentions {
		userMentions = append(userMentions, twitterUser{
			UserID:     userMention.Id_str,
			DgraphType: "User",
			UserName:   userMention.Name,
			ScreenName: userMention.Screen_name,
		})
	}

	return &twitterTweet{
		IDStr:      tweet.IdStr,
		DgraphType: "Tweet",
		CreatedAt:  createdAt.Format(cDgraphTimeFormat),
		Message:    tweet.FullText,
		URLs:       expandedURLs,
		HashTags:   hashTagTexts,
		Author: twitterUser{
			UserID:           tweet.User.IdStr,
			DgraphType:       "User",
			UserName:         tweet.User.Name,
			ScreenName:       tweet.User.ScreenName,
			Description:      tweet.User.Description,
			FriendsCount:     tweet.User.FriendsCount,
			FollowersCount:   tweet.User.FollowersCount,
			Verified:         tweet.User.Verified,
			ProfileBannerURL: tweet.User.ProfileBannerURL,
			ProfileImageURL:  tweet.User.ProfileImageURL,
		},
		Mention: userMentions,
		Retweet: tweet.Retweeted,
	}, nil
}

func readCredentials(path string) twitterCreds {
	jsn, err := ioutil.ReadFile(path)
	checkFatal(err, "Unable to open twitter credentials file '%s'", path)

	var creds twitterCreds
	err = json.Unmarshal(jsn, &creds)
	checkFatal(err, "Unable to parse twitter credentials file '%s'", path)

	return creds
}

func newTwitterClient(creds twitterCreds) *anaconda.TwitterApi {
	client := anaconda.NewTwitterApiWithCredentials(
		creds.AccessToken, creds.AccessSecret,
		creds.ConsumerKey, creds.ConsumerSecret,
	)

	ok, err := client.VerifyCredentials()
	checkFatal(err, "error in verifying credentials")
	if !ok {
		checkFatal(errors.New("invalid credentials"), "twitter")
	}

	return client
}

func newAPIClients(sockAddr []string) []api.DgraphClient {
	var clients []api.DgraphClient

	for _, sa := range sockAddr {
		conn, err := grpc.Dial(sa, grpc.WithInsecure())
		checkFatal(err, "Unable to connect to dgraph")
		clients = append(clients, api.NewDgraphClient(conn))
	}

	return clients
}

func reportWriteStats(c *z.Closer) {
	defer c.Done()

	var oldStats, newStats progStats
	ticker := time.NewTicker(time.Duration(opts.reportPeriodSecs) * time.Second)
	defer ticker.Stop()

	log.Printf("Reporting stats every %v seconds\n", opts.reportPeriodSecs)
	for {
		select {
		case <-c.HasBeenClosed():
			return
		case <-ticker.C:
		}
		newStats = stats

		tweets := atomic.LoadUint32(&newStats.Tweets)
		errorsJSON := atomic.LoadUint32(&newStats.ErrorsJSON)
		numFiles := atomic.LoadUint32(&newStats.NumFiles)
		totalDownloaded := atomic.LoadUint64(&newStats.TotalDownloaded)

		oldTweets := atomic.LoadUint32(&oldStats.Tweets)
		oldSz := atomic.LoadUint64(&oldStats.TotalDownloaded)

		rate := humanize.IBytes(uint64((totalDownloaded - oldSz) / uint64(opts.reportPeriodSecs)))

		log.Printf("STATS tweets: %d, json_errs: %d, created files: %d "+
			"download_rate: %d tweets/sec download_speed: %s/sec Total: %s\n", tweets, errorsJSON, numFiles,
			(tweets-oldTweets)/uint32(opts.reportPeriodSecs), rate, humanize.IBytes(totalDownloaded))

		oldStats = newStats
	}
}

func reportInsertStats(c *z.Closer) {
	defer c.Done()

	var oldStats, newStats progStats
	ticker := time.NewTicker(time.Duration(opts.reportPeriodSecs) * time.Second)
	defer ticker.Stop()

	log.Printf("Reporting stats every %v seconds\n", opts.reportPeriodSecs)
	for {
		select {
		case <-c.HasBeenClosed():
			return
		case <-ticker.C:
		}
		newStats = stats
		tweets := atomic.LoadUint32(&newStats.Tweets)
		commits := atomic.LoadUint32(&newStats.Commits)
		leakedCommits := atomic.LoadUint32(&newStats.LeakedCommits)
		errorsJSON := atomic.LoadUint32(&newStats.ErrorsJSON)
		retries := atomic.LoadUint32(&newStats.Retries)
		failures := atomic.LoadUint32(&newStats.Failures)
		errorsDgraph := atomic.LoadUint32(&newStats.ErrorsDgraph)

		oldCommits := atomic.LoadUint32(&oldStats.Commits)
		log.Printf("STATS tweets: %d, commits: %d, leaked: %d, json_errs: %d, "+
			"retries: %d, failures: %d, dgraph_errs: %d, "+"commit_rate: %d/sec\n",
			tweets, commits, leakedCommits, errorsJSON, retries, failures, errorsDgraph,
			(commits-oldCommits)/uint32(opts.reportPeriodSecs))

		oldStats = newStats
	}
}

func checkFatal(err error, format string, args ...interface{}) {
	if err != nil {
		msg := fmt.Sprintf(format, args...)
		_, _ = fmt.Fprintf(os.Stderr, "%s :: %s\n", msg, err)
		os.Exit(1)
	}
}

func main() {
	flag.IntVar(&opts.numClients, "l", 1, "number of dgraph clients to run")
	flag.IntVar(&opts.numWriters, "num-writers", 4, "number of writer to run")
	flag.StringVar(&opts.credentialsFile, "c", "credentials.json", "path to credentials file")
	flag.StringVar(&opts.dataFilesPath, "d", "", "path containing json files with tweets in each line")
	flag.StringVar(&opts.outFilesPath, "o", "./data", "Directory to store compressed json tweets")
	flag.Float64Var(&opts.noCommitRatio, "p", 0, "prob of CommitNow=False, from 0.0 to 1.0")
	flag.IntVar(&opts.fileSize, "fsz", 100, "Max size of the generated gz file (in MB)")

	alphasAddress := flag.String("a", ":9180,:9182,:9183", "comma separated addresses to alphas")
	downloadTweets := flag.Bool("download", false, "Download tweets and save to directory specified by -o flag")

	flag.Parse()

	if opts.noCommitRatio > 1 || opts.noCommitRatio < 0 {
		log.Fatalf("invalid value for commit=false probability")
	}
	opts.reportPeriodSecs = 2
	opts.fileSize = opts.fileSize << 20
	opts.alphaSockAddr = strings.Split(*alphasAddress, ",")

	var tweetChannel chan interface{}
	if opts.dataFilesPath == "" {
		creds := readCredentials(opts.credentialsFile)
		client := newTwitterClient(creds)
		stream := client.PublicStreamSample(nil)
		tweetChannel = stream.C
		defer stream.Stop()
	} else {
		tweetChannel = setupChannelFromDir(opts.dataFilesPath)
	}

	if *downloadTweets {
		startWriters(tweetChannel)
	} else {
		startInserters(tweetChannel)
	}
}

var (
	gzFileSuffix   = ".tweets.gz"
	jsonFileSuffix = ".tweets.json"
)

func parseFid(name string) uint64 {
	name = path.Base(name)
	if !strings.HasSuffix(name, gzFileSuffix) {
		return 0
	}
	switch {
	case strings.HasSuffix(name, gzFileSuffix):
		name = strings.TrimSuffix(name, gzFileSuffix)
	case strings.HasSuffix(name, jsonFileSuffix):
		name = strings.TrimSuffix(name, jsonFileSuffix)
	default:
		log.Fatalf("Unknown file suffix %s", name)
	}
	name = strings.TrimSuffix(name, gzFileSuffix)
	id, err := strconv.Atoi(name)
	if err != nil {
		return 0
	}
	if id <= 0 {
		log.Fatal("Id %d should be greater than 0", id)
	}
	return uint64(id)
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func startWriters(tweetChannel <-chan interface{}) {
	cl := z.NewCloser(1)
	go reportWriteStats(cl)
	numWriters := opts.numWriters
	log.Printf("Using %d writers\n", numWriters)

	sdCh := make(chan os.Signal, 1)

	exit := z.NewCloser(1)
	// sigint : Ctrl-C, sigterm : kill command.
	signal.Notify(sdCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		defer exit.Done()
		<-sdCh
		log.Printf("Caught Ctrl-C. Terminating now (this may take a few seconds)...")
		cl.SignalAndWait()
	}()
	dirExists, err := exists(opts.outFilesPath)
	checkFatal(err, "Dir exist check failed for %s", opts.outFilesPath)
	if !dirExists {
		checkFatal(os.MkdirAll(opts.outFilesPath, 0777), "failed to create %s dir", opts.outFilesPath)
	}

	dInfo, err := ioutil.ReadDir(opts.outFilesPath)
	checkFatal(err, "ReadDir %s", opts.outFilesPath)

	fid = uint64(0)
	for _, i := range dInfo {
		id := parseFid(i.Name())
		if id > fid {
			fid = id
		}
	}
	log.Println("Found Max fid:", fid)

	// read twitter stream
	cl.AddRunning(numWriters)
	for i := 0; i < numWriters; i++ {
		go runWriter(cl, tweetChannel)
	}
	exit.Wait()
}
func startInserters(tweetChannel <-chan interface{}) {
	alphas := newAPIClients(opts.alphaSockAddr)

	// setup schema
	dgr := dgo.NewDgraphClient(alphas...)
	op := &api.Operation{
		Schema: cDgraphSchema,
	}
	retryCount := 0
	for {
		err := dgr.Alter(context.Background(), op)
		if err == nil {
			break
		}

		retryCount++
		if retryCount == 3 {
			checkFatal(err, "error in creating indexes")
		}

		log.Println("sleeping for 1 sec, alter failed")
		time.Sleep(1 * time.Second)
	}

	// report stats
	r := z.NewCloser(1)
	go reportInsertStats(r)
	log.Printf("Using %v dgraph clients on %v alphas\n", opts.numClients, len(opts.alphaSockAddr))

	// read twitter stream
	c := z.NewCloser(0)
	for i := 0; i < opts.numClients; i++ {
		c.AddRunning(1)
		go runInserter(alphas, c, tweetChannel)
	}

	c.Wait()
	r.SignalAndWait()
	log.Println("Stopping stream...")
}

func setupChannelFromDir(dataPath string) chan interface{} {
	info, err := os.Stat(dataPath)
	checkFatal(err, "error in opening path to json files")

	var files []string
	switch {
	case info.IsDir():
		// handle directory case
		err := filepath.Walk(dataPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			files = append(files, path)
			return nil
		})

		checkFatal(err, "error in walking input data directory")
	default:
		// handle single file
		files = append(files, dataPath)
	}

	dataChan := make(chan interface{})
	go func() {
		for _, dataFile := range files {
			log.Println("reading file:", dataFile)

			fd, err := os.Open(dataFile)
			if err != nil {
				checkFatal(err, "error in opening file: %v", dataFile)
			}

			scanner := bufio.NewScanner(fd)
			for scanner.Scan() {
				var t anaconda.Tweet
				if err := json.Unmarshal(scanner.Bytes(), &t); err != nil {
					atomic.AddUint32(&stats.ErrorsJSON, 1)
					continue
				}

				dataChan <- t
			}

			checkFatal(scanner.Err(), "error in scanning file: %v", dataFile)
			fd.Close()
		}

		close(dataChan)
	}()

	return dataChan
}
