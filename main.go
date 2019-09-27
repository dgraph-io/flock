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
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ChimeraCoder/anaconda"
	"github.com/dgraph-io/badger/y"
	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	"google.golang.org/grpc"
)

const (
	cTimeFormat       = "Mon Jan 02 15:04:05 -0700 2006"
	cDgraphTimeFormat = "2006-01-02T15:04:05.999999999+10:00"

	cDgraphSchema = `
		type Tweet {
			id_str: string
			created_at: dateTime
			message: string
			urls: [string]
			hashtags: [string]
			author: [User]
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
		id_str: string @index(exact) @upsert .
		created_at: dateTime @index(hour) .
		hashtags: [string] @index(exact) .
		author: uid @count @reverse .
		mention: [uid] @reverse .
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

			queryStr := buildQuery(ft)

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
