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
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ChimeraCoder/anaconda"
	"github.com/dgraph-io/badger/y"
	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
	"google.golang.org/grpc"
)

const (
	cTimeFormat       = "Mon Jan 02 15:04:05 -0700 2006"
	cDgraphTimeFormat = "2006-01-02T15:04:05.999999999+10:00"

	cDgraphSchema = `
user_id: string @index(exact) @upsert .
user_name: string @index(hash) .
screen_name: string @index(term) .

id_str: string @index(exact) @upsert .
created_at: dateTime @index(hour) .
hashtags: [string] @index(exact) .

author: uid @count @reverse .
mention: uid @reverse .
`

	cDgraphUserQuery = `
query all($userID: string) {
	v as var(func: eq(user_id, $userID))
}
`
)

var (
	opts  progOptions
	stats progStats

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
	NumClients       int
	CredentialsFile  string
	DataFilesPath    string
	ReportPeriodSecs int
	NoCommitRatio    float64
	AlphaSockAddr    []string
}

type progStats struct {
	Tweets        uint32
	Commits       uint32
	LeakedCommits uint32
	Retries       uint32
	Failures      uint32
	ErrorsJSON    uint32
	ErrorsDgraph  uint32
}

type twitterUser struct {
	UID              string `json:"uid,omitempty"`
	UserID           string `json:"user_id,omitempty"`
	UserName         string `json:"user_name,omitempty"`
	ScreenName       string `json:"screen_name,omitempty"`
	Description      string `json:"description,omitempty"`
	FriendsCount     int    `json:"friends_count,omitempty"`
	Verified         bool   `json:"verified,omitempty"`
	ProfileBannerURL string `json:"profile_banner_url,omitempty"`
	ProfileImageURL  string `json:"profile_image_url,omitempty"`
}

type twitterTweet struct {
	UID       string        `json:"uid,omitempty"`
	IDStr     string        `json:"id_str"`
	CreatedAt string        `json:"created_at"`
	Message   string        `json:"message,omitempty"`
	URLs      []string      `json:"urls,omitempty"`
	HashTags  []string      `json:"hashtags,omitempty"`
	Author    twitterUser   `json:"author"`
	Mention   []twitterUser `json:"mention,omitempty"`
	Retweet   bool          `json:"retweet"`
}

func runInserter(alphas []api.DgraphClient, c *y.Closer, tweets <-chan interface{}) {
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

			updateFilteredTweet(ft, txn)

			tweet, err := json.Marshal(ft)
			if err != nil {
				atomic.AddUint32(&stats.ErrorsJSON, 1)
				continue
			}

			commitNow := true
			if rand.Float64() < opts.NoCommitRatio {
				commitNow = false
			}

			// only ONE retry attempt is made
			retry := true
		RETRY:
			queryVars := map[string]string{"userID": ft.Author.UserID}
			apiUpsert := &api.Request{
				Mutations: []*api.Mutation{
					&api.Mutation{
						SetJson: tweet,
					},
				},
				CommitNow: commitNow,
				Query:     cDgraphUserQuery,
				Vars:      queryVars,
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
			UserName:   userMention.Name,
			ScreenName: userMention.Screen_name,
		})
	}

	return &twitterTweet{
		IDStr:     tweet.IdStr,
		CreatedAt: createdAt.Format(cDgraphTimeFormat),
		Message:   tweet.FullText,
		URLs:      expandedURLs,
		HashTags:  hashTagTexts,
		Author: twitterUser{
			UserID:           tweet.User.IdStr,
			UserName:         tweet.User.Name,
			ScreenName:       tweet.User.ScreenName,
			Description:      tweet.User.Description,
			FriendsCount:     tweet.User.FriendsCount,
			Verified:         tweet.User.Verified,
			ProfileBannerURL: tweet.User.ProfileBannerURL,
			ProfileImageURL:  tweet.User.ProfileImageURL,
		},
		Mention: userMentions,
		Retweet: tweet.Retweeted,
	}, nil
}

func updateFilteredTweet(ft *twitterTweet, txn *dgo.Txn) {

	// map to check for duplicates
	users := make(map[string]string)

	userID := ft.Author.UserID
	users[userID] = ft.Author.UID

	userMentions := make([]twitterUser, 0)
	for _, m := range ft.Mention {
		if dup, ok := users[m.UserID]; ok && dup != "" {
			userMentions = append(userMentions, twitterUser{UID: dup})
			users[m.UserID] = m.UID
		}
	}
	ft.Mention = userMentions
	ft.UID = "uid(v)"
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

// TODO: fix the race condition here
func reportStats(c *y.Closer) {
	defer c.Done()

	var oldStats, newStats progStats
	log.Printf("Reporting stats every %v seconds\n", opts.ReportPeriodSecs)
	for {
		newStats = stats
		log.Printf("STATS tweets: %d, commits: %d, leaked: %d, json_errs: %d, "+
			"retries: %d, failures: %d, dgraph_errs: %d, "+
			"commit_rate: %d/sec\n",
			newStats.Tweets, newStats.Commits, newStats.LeakedCommits, newStats.ErrorsJSON,
			newStats.Retries, newStats.Failures, newStats.ErrorsDgraph,
			(newStats.Tweets-oldStats.Tweets)/uint32(opts.ReportPeriodSecs))
		oldStats = newStats

		select {
		case <-c.HasBeenClosed():
			return
		default:
			time.Sleep(time.Second * time.Duration(opts.ReportPeriodSecs))
		}
	}
}

func checkFatal(err error, format string, args ...interface{}) {
	if err != nil {
		msg := fmt.Sprintf(format, args...)
		_, _ = fmt.Fprintf(os.Stderr, "%s :: %s\n", msg, err)
		os.Exit(1)
	}
}

func getSchema() string {
	var schema string
	schema += fmt.Sprintf("user_id: string @index(exact) @upsert .\n")
	schema += fmt.Sprintf("user_name: string @index(hash) .\n")
	schema += fmt.Sprintf("screen_name: string @index(term) .\n")

	schema += fmt.Sprintf("id_str: string @index(exact) @upsert .\n")
	schema += fmt.Sprintf("created_at: dateTime @index(hour) .\n")
	schema += fmt.Sprintf("hashtags: [string] @index(exact) .\n")

	schema += fmt.Sprintf("author: uid @count @reverse .\n")
	switch dgraphVersion() {
	case "v1.0":
		schema += fmt.Sprintf("mention: uid @reverse .\n")
	case "v1.1":
		schema += fmt.Sprintf("mention: [uid] @reverse .\n")
	default:
		log.Fatalf("cannot generate schema for unknown dgraph version")
	}

	return schema
}

func dgraphVersion() string {
	grpcAddr := strings.Split(opts.AlphaSockAddr[0], ":")
	hostname := grpcAddr[0]
	port, err := strconv.Atoi(grpcAddr[1])
	checkFatal(err, "error getting Dgraph HTTP port")

	alphaHttpAddr := fmt.Sprintf("http://%s:%d", hostname, port-1000)
	url := fmt.Sprintf("%s/health", alphaHttpAddr)
	resp, err := http.Get(url)
	checkFatal(err, "error with health check")

	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	checkFatal(err, "error while getting health check response")

	if string(data) == "OK" {
		return "v1.0"
	}

	var info struct {
		Version  string        `json:"version"`
		Instance string        `json:"instance"`
		Uptime   time.Duration `json:"uptime"`
	}
	if err := json.Unmarshal(data, &info); err == nil {
		return "v1.1"
	}

	return "unknown"
}

func main() {
	dgclients := flag.Int("l", 8, "number of dgraph clients to run")
	credentialsFile := flag.String("c", "credentials.json", "path to credentials file")
	dataFilesPath := flag.String("d", "", "path containing json files with tweets in each line")
	noCommitRatio := flag.Float64("p", 0, "prob of CommitNow=False, from 0.0 to 1.0")
	alphasAddress := flag.String("a", ":9180,:9182,:9183", "comma separated addresses to alphas")
	flag.Parse()

	if *noCommitRatio > 1 || *noCommitRatio < 0 {
		log.Fatalf("invalid value for commit=false probability")
	}
	opts = progOptions{
		NumClients:       *dgclients,
		CredentialsFile:  *credentialsFile,
		DataFilesPath:    *dataFilesPath,
		ReportPeriodSecs: 2,
		NoCommitRatio:    *noCommitRatio,
		AlphaSockAddr:    strings.Split(*alphasAddress, ","),
	}

	alphas := newAPIClients(opts.AlphaSockAddr)

	// setup schema
	dgr := dgo.NewDgraphClient(alphas...)
	op := &api.Operation{
		Schema: getSchema(),
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
	r := y.NewCloser(1)
	go reportStats(r)
	log.Printf("Using %v dgraph clients on %v alphas\n",
		opts.NumClients, len(opts.AlphaSockAddr))

	var tweetChannel chan interface{}
	if opts.DataFilesPath == "" {
		creds := readCredentials(opts.CredentialsFile)
		client := newTwitterClient(creds)
		stream := client.PublicStreamSample(nil)
		tweetChannel = stream.C
		defer stream.Stop()
	} else {
		tweetChannel = setupChannelFromDir(opts.DataFilesPath)
	}

	// read twitter stream
	c := y.NewCloser(0)
	for i := 0; i < opts.NumClients; i++ {
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
