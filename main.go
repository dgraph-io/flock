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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
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
`

	cDgraphTweetQuery = `
query all($tweetID: string) {
	all(func: eq(id_str, $tweetID)) {
		uid
	}
}
`

	cDgraphUserQuery = `
query all($userID: string) {
	all(func: eq(user_id, $userID)) {
		uid
		user_id
		user_name
		screen_name
		description
		friends_count
		verified
		profile_banner_url
		profile_image_url
	}
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
	NumDgrClients    int
	CredentialsFile  string
	KeywordsFile     string
	ReportPeriodSecs int
	AlphaSockAddr    []string
}

type progStats struct {
	Tweets       uint32
	Commits      uint32
	Retries      uint32
	Failures     uint32
	ErrorsJSON   uint32
	ErrorsDgraph uint32
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

func main() {
	// TODO: Allow setting these from cmdline.
	opts = progOptions{
		NumDgrClients:    6,
		CredentialsFile:  "credentials.json",
		KeywordsFile:     "keywords.txt",
		ReportPeriodSecs: 2,
		AlphaSockAddr:    []string{":9180", ":9182", ":9183"},
	}

	creds := readCredentials(opts.CredentialsFile)
	kwds := readKeyWords(opts.KeywordsFile)
	client := newTwitterClient(creds)
	alphas := newAPIClients(opts.AlphaSockAddr)

	params := &twitter.StreamFilterParams{
		Track:         kwds,
		StallWarnings: twitter.Bool(true),
	}
	stream, err := client.Streams.Filter(params)
	checkFatal(err, "Unable to get twitter stream")

	// setup schema
	dgr := dgo.NewDgraphClient(alphas...)
	op := &api.Operation{
		Schema: cDgraphSchema,
	}
	err = dgr.Alter(context.Background(), op)
	checkFatal(err, "error in creating indexes")

	// read twitter stream
	log.Printf("Using %v dgraph clients on %v alphas\n",
		opts.NumDgrClients, len(opts.AlphaSockAddr))
	var wg sync.WaitGroup
	for i := 0; i < opts.NumDgrClients; i++ {
		wg.Add(1)
		go runInserter(alphas, &wg, stream.Messages)
	}
	go reportStats()
	wg.Wait()
}

func runInserter(alphas []api.DgraphClient, wg *sync.WaitGroup, tweets <-chan interface{}) {
	defer wg.Done()

	dgr := dgo.NewDgraphClient(alphas...)
	for jsn := range tweets {
		atomic.AddUint32(&stats.Tweets, 1)

		ft, err := filterTweet(jsn)
		if err != nil {
			atomic.AddUint32(&stats.ErrorsJSON, 1)
			continue
		}

		// Now, we need query UIDs and ensure they don't already exists
		txn := dgr.NewTxn()
		if err := updateFilteredTweet(ft, txn); err != nil {
			atomic.AddUint32(&stats.ErrorsDgraph, 1)
			continue
		}

		tweet, err := json.Marshal(ft)
		if err != nil {
			atomic.AddUint32(&stats.ErrorsJSON, 1)
			continue
		}

		// only ONE retry attempt is made
		retry := true
	RETRY:
		_, err = txn.Mutate(context.Background(), &api.Mutation{SetJson: tweet, CommitNow: true})
		switch {
		case err == nil:
			atomic.AddUint32(&stats.Commits, 1)
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

func filterTweet(jsn interface{}) (*twitterTweet, error) {
	var tweet *twitter.Tweet
	switch msg := jsn.(type) {
	case *twitter.Tweet:
		tweet = msg
	default:
		return nil, errNotATweet
	}

	createdAt, err := time.Parse(cTimeFormat, tweet.CreatedAt)
	if err != nil {
		return nil, err
	}

	var tweetText string
	if tweet.Truncated {
		tweetText = tweet.ExtendedTweet.FullText
	} else {
		tweetText = tweet.FullText
	}

	var urlEntities []twitter.URLEntity
	if tweet.Truncated {
		urlEntities = tweet.ExtendedTweet.Entities.Urls
	} else {
		urlEntities = tweet.Entities.Urls
	}
	expandedURLs := make([]string, len(urlEntities))
	for _, url := range urlEntities {
		expandedURLs = append(expandedURLs, url.ExpandedURL)
	}

	var hashTags []twitter.HashtagEntity
	if tweet.Truncated {
		hashTags = tweet.ExtendedTweet.Entities.Hashtags
	} else {
		hashTags = tweet.Entities.Hashtags
	}
	hashTagTexts := make([]string, len(hashTags))
	for _, tag := range hashTags {
		hashTagTexts = append(hashTagTexts, tag.Text)
	}

	var userMentions []twitterUser
	for _, userMention := range tweet.Entities.UserMentions {
		userMentions = append(userMentions, twitterUser{
			UserID:     userMention.IDStr,
			UserName:   userMention.Name,
			ScreenName: userMention.ScreenName,
		})
	}

	return &twitterTweet{
		IDStr:     tweet.IDStr,
		CreatedAt: createdAt.Format(cDgraphTimeFormat),
		Message:   tweetText,
		URLs:      expandedURLs,
		HashTags:  hashTagTexts,
		Author: twitterUser{
			UserID:           tweet.User.IDStr,
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

func updateFilteredTweet(ft *twitterTweet, txn *dgo.Txn) error {
	// first ensure that tweet doesn't exists
	resp, err := txn.QueryWithVars(context.Background(), cDgraphTweetQuery,
		map[string]string{"$tweetID": ft.IDStr})
	if err != nil {
		return err
	}
	var r struct {
		All []struct {
			UID string `json:"uid"`
		} `json:"all"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		return err
	}

	// possible duplicate, shouldn't happen
	if len(r.All) > 0 {
		log.Println("found duplicate tweet with id:", ft.IDStr)
		return errShouldNotReach
	}

	if u, err := queryUser(txn, &ft.Author); err != nil {
		return err
	} else if u != nil {
		ft.Author = *u
	}

	for i, m := range ft.Mention {
		if u, err := queryUser(txn, &m); err != nil {
			return err
		} else if u != nil {
			ft.Mention[i] = *u
		}
	}

	return nil
}

func equalsUser(src, dst *twitterUser) bool {
	return src.UserID == dst.UserID &&
		src.UserName == dst.UserName &&
		src.ScreenName == dst.ScreenName &&
		src.Description == dst.Description &&
		src.FriendsCount == dst.FriendsCount &&
		src.Verified == dst.Verified &&
		src.ProfileBannerURL == dst.ProfileBannerURL &&
		src.ProfileImageURL == dst.ProfileImageURL
}

func queryUser(txn *dgo.Txn, src *twitterUser) (*twitterUser, error) {
	resp, err := txn.QueryWithVars(context.Background(), cDgraphUserQuery,
		map[string]string{"$userID": src.UserID})
	if err != nil {
		return nil, err
	}

	var r struct {
		All []twitterUser `json:"all"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		return nil, err
	}

	if len(r.All) > 1 {
		log.Println("found duplicate users in Dgraph with id:", r.All[0].UserID)
		return nil, errShouldNotReach
	} else if len(r.All) == 0 {
		return nil, nil
	} else if len(r.All) == 1 && !equalsUser(src, &r.All[0]) {
		return &r.All[0], nil
	} else {
		return &twitterUser{UID: r.All[0].UID}, nil
	}
}

func readCredentials(path string) twitterCreds {
	jsn, err := ioutil.ReadFile(path)
	checkFatal(err, "Unable to open twitter credentials file '%s'", path)

	var creds twitterCreds
	err = json.Unmarshal(jsn, &creds)
	checkFatal(err, "Unable to parse twitter credentials file '%s'", path)

	return creds
}

func readKeyWords(path string) []string {
	txt, err := ioutil.ReadFile(path)
	checkFatal(err, "Unable to read keywords file '%s'", path)
	return strings.Split(string(txt), "\n")
}

func newTwitterClient(creds twitterCreds) *twitter.Client {
	token := oauth1.NewToken(creds.AccessToken, creds.AccessSecret)
	config := oauth1.NewConfig(creds.ConsumerKey, creds.ConsumerSecret)
	httpClient := config.Client(oauth1.NoContext, token)
	twitterClient := twitter.NewClient(httpClient)

	// verify credentials before returning them
	verifyParams := &twitter.AccountVerifyParams{
		SkipStatus:   twitter.Bool(true),
		IncludeEmail: twitter.Bool(true),
	}
	_, _, err := twitterClient.Accounts.VerifyCredentials(verifyParams)
	checkFatal(err, "invalid credentials")

	return twitterClient
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

func reportStats() {
	var oldStats, newStats progStats
	log.Printf("Reporting stats every %v seconds\n", opts.ReportPeriodSecs)
	for {
		newStats = stats
		time.Sleep(time.Second * time.Duration(opts.ReportPeriodSecs))
		log.Printf("STATS tweets: %d, commits: %d, json_errs: %d, "+
			"retries: %d, failures: %d, dgraph_errs: %d, "+
			"commit_rate: %d/sec\n",
			newStats.Tweets, newStats.Commits, newStats.ErrorsJSON,
			newStats.Retries, newStats.Failures, newStats.ErrorsDgraph,
			(newStats.Tweets-oldStats.Tweets)/uint32(opts.ReportPeriodSecs))
		oldStats = newStats
	}
}

func checkFatal(err error, format string, args ...interface{}) {
	if err != nil {
		msg := fmt.Sprintf(format, args...)
		_, _ = fmt.Fprintf(os.Stderr, "%s: %s\n", msg, err)
		os.Exit(1)
	}
}
