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
	"strconv"
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
)

var (
	errNotATweet      = errors.New("message in the stream is not a tweet")
	errShouldNotReach = errors.New("invariant failed to satisfy")
)

type TwitterCreds struct {
	AccessSecret   string `json:"access_secret"`
	AccessToken    string `json:"access_token"`
	ConsumerKey    string `json:"consumer_key"`
	ConsumerSecret string `json:"consumer_secret"`
}

type Options struct {
	NumDgrClients    int
	CredentialsFile  string
	KeywordsFile     string
	ReportPeriodSecs int
	AlphaSockAddr    []string
}

type Stats struct {
	NumTweets       uint32
	NumCommits      uint32
	NumErrorsJson   uint32
	NumErrorsDgraph uint32
}

type twitterUser struct {
	UID              string `json:"uid"`
	UserID           string `json:"user_id,omitempty"`
	UserName         string `json:"user_name,omitempty"`
	ScreenName       string `json:"screen_name,omitempty"`
	Description      string `json:"description,omitempty"`
	FriendsCount     int    `json:"friends_count,omitempty"`
	Verified         bool   `json:"verified,omitempty"`
	ProfileBannerURL string `json:"profile_banner_url,omitempty"`
	ProfileImageURL  string `json:"profile_image_url,omitempty"`
	Tweet            []struct {
		UID string `json:"uid"`
	} `json:"tweet"`
}

type twitterTweet struct {
	UID       string        `json:"uid"`
	IDStr     string        `json:"id_str"`
	CreatedAt string        `json:"created_at"`
	Message   string        `json:"message,omitempty"`
	URLs      []string      `json:"urls,omitempty"`
	HashTags  []string      `json:"hashtags,omitempty"`
	Author    twitterUser   `json:"author"`
	Mention   []twitterUser `json:"mention,omitempty"`
	Retweet   bool          `json:"retweet"`
}

var (
	opts  Options
	stats Stats
)

func main() {
	// TODO: Allow setting these from cmdline.
	opts = Options{
		NumDgrClients:    6,
		CredentialsFile:  "credentials.json",
		KeywordsFile:     "keywords.txt",
		ReportPeriodSecs: 2,
		AlphaSockAddr:    []string{":9180", ":9182", ":9183"},
	}

	creds := readCredentials(opts.CredentialsFile)
	kwds := readKeyWords(opts.KeywordsFile)
	client := newTwitterClient(creds)
	alphas := newApiClients(opts.AlphaSockAddr)

	params := &twitter.StreamFilterParams{
		Track:         kwds,
		StallWarnings: twitter.Bool(true),
	}
	stream, err := client.Streams.Filter(params)
	CheckFatal(err, "Unable to get twitter stream")

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

func CheckFatal(err error, format string, args ...interface{}) {
	if err != nil {
		msg := fmt.Sprintf(format, args...)
		_, _ = fmt.Fprintf(os.Stderr, "%s: %s\n", msg, err)
		os.Exit(1)
	}
}

func runInserter(alphas []api.DgraphClient, wg *sync.WaitGroup, tweets <-chan interface{}) {
	defer wg.Done()

	dgr := dgo.NewDgraphClient(alphas...)

	// create index on id strings
	op := &api.Operation{
		Schema: `user_id: string @index(exact) .
id_str: string @index(exact) .`,
	}
	err := dgr.Alter(context.Background(), op)
	CheckFatal(err, "error in creating indexes")

	for jsn := range tweets {
		atomic.AddUint32(&stats.NumTweets, 1)

		ft, err := filterTweet(jsn)
		if err != nil {
			atomic.AddUint32(&stats.NumErrorsJson, 1)
			continue
		}

		// Now, we need query UIDs and ensure they don't already exists
		txn := dgr.NewTxn()
		if err := updateFilteredTweet(ft, txn); err != nil {
			atomic.AddUint32(&stats.NumErrorsDgraph, 1)
			continue
		}

		tweet, err := json.Marshal(ft)
		if err != nil {
			atomic.AddUint32(&stats.NumErrorsJson, 1)
			continue
		}

		_, err = txn.Mutate(context.Background(), &api.Mutation{SetJson: tweet, CommitNow: true})
		switch {
		case err == nil:
			atomic.AddUint32(&stats.NumCommits, 1)
		case strings.Contains(err.Error(), "connection refused"):
			log.Printf("ERROR Connection refused... waiting a bit\n")
			time.Sleep(5 * time.Second)
		default:
			atomic.AddUint32(&stats.NumErrorsDgraph, 1)
			log.Printf("ERROR Unable to commit: %v\n", err)
			time.Sleep(100 * time.Millisecond)
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
			UID:        fmt.Sprintf("_:%v", userMention.IDStr),
			UserID:     userMention.IDStr,
			UserName:   userMention.Name,
			ScreenName: userMention.ScreenName,
		})
	}

	return &twitterTweet{
		UID:       fmt.Sprintf("_:%v", tweet.IDStr),
		IDStr:     tweet.IDStr,
		CreatedAt: createdAt.Format(cDgraphTimeFormat),
		Message:   unquote(strconv.Quote(tweetText)),
		URLs:      expandedURLs,
		HashTags:  hashTagTexts,
		Author: twitterUser{
			UID:              fmt.Sprintf("_:%v", tweet.User.IDStr),
			UserID:           tweet.User.IDStr,
			UserName:         unquote(strconv.Quote(tweet.User.Name)),
			ScreenName:       tweet.User.ScreenName,
			Description:      unquote(strconv.Quote(tweet.User.Description)),
			FriendsCount:     tweet.User.FriendsCount,
			Verified:         tweet.User.Verified,
			ProfileBannerURL: tweet.User.ProfileBannerURL,
			ProfileImageURL:  tweet.User.ProfileImageURL,
			Tweet: []struct {
				UID string `json:"uid"`
			}{
				{
					UID: fmt.Sprintf("_:%v", tweet.User.IDStr),
				},
			},
		},
		Mention: userMentions,
		Retweet: tweet.Retweeted,
	}, nil
}

func updateFilteredTweet(ft *twitterTweet, txn *dgo.Txn) error {
	// first ensure that tweet doesn't exists
	qTweet := `query all($tweetID: string) {
	    all(func: eq(id_str, $tweetID)) {
	      uid
	    }
	  }`
	resp, err := txn.QueryWithVars(context.Background(), qTweet, map[string]string{"$tweetID": ft.IDStr})
	if err != nil {
		return err
	}
	var respJSON struct {
		All []struct {
			UID string `json:"uid"`
		} `json:"all"`
	}
	if err := json.Unmarshal(resp.Json, &respJSON); err != nil {
		return err
	}

	// possible duplicate, shouldn't happen
	if len(respJSON.All) > 0 {
		return errShouldNotReach
	}

	if u, err := queryUser(txn, ft.Author.UserID); err != nil {
		return err
	} else if u != nil {
		u.Tweet = ft.Author.Tweet
		ft.Author = *u
	}

	for i, m := range ft.Mention {
		if u, err := queryUser(txn, m.UserID); err != nil {
			return err
		} else if u != nil {
			ft.Mention[i] = *u
		}
	}

	return nil
}

func queryUser(txn *dgo.Txn, userID string) (*twitterUser, error) {
	// query author of the tweet
	q := `query all($userID: string) {
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
		  }`
	resp, err := txn.QueryWithVars(context.Background(), q, map[string]string{"$userID": userID})
	if err != nil {
		return nil, err
	}
	var respJSON struct {
		All []twitterUser `json:"all"`
	}
	if err := json.Unmarshal(resp.Json, &respJSON); err != nil {
		return nil, err
	}

	if len(respJSON.All) > 1 {
		return nil, errShouldNotReach
	}

	if len(respJSON.All) == 1 {
		u := &twitterUser{}
		u.UID = fmt.Sprintf("%s", respJSON.All[0].UID)
		if u.UserID != respJSON.All[0].UserID {
			u.UserID = respJSON.All[0].UserID
		}
		if u.UserName != respJSON.All[0].UserName {
			u.UserName = respJSON.All[0].UserName
		}
		if u.ScreenName != respJSON.All[0].ScreenName {
			u.ScreenName = respJSON.All[0].ScreenName
		}
		if u.Description != respJSON.All[0].Description {
			u.Description = respJSON.All[0].Description
		}
		if u.FriendsCount != respJSON.All[0].FriendsCount {
			u.FriendsCount = respJSON.All[0].FriendsCount
		}
		if u.Verified != respJSON.All[0].Verified {
			u.Verified = respJSON.All[0].Verified
		}
		if u.ProfileBannerURL != respJSON.All[0].ProfileBannerURL {
			u.ProfileBannerURL = respJSON.All[0].ProfileBannerURL
		}
		if u.ProfileImageURL != respJSON.All[0].ProfileImageURL {
			u.ProfileImageURL = respJSON.All[0].ProfileImageURL
		}

		return u, nil
	}

	return nil, nil
}

func readCredentials(path string) TwitterCreds {
	jsn, err := ioutil.ReadFile(path)
	CheckFatal(err, "Unable to open twitter credentials file '%s'", path)

	var creds TwitterCreds
	err = json.Unmarshal(jsn, &creds)
	CheckFatal(err, "Unable to parse twitter credentials file '%s'", path)

	return creds
}

func readKeyWords(path string) []string {
	txt, err := ioutil.ReadFile(path)
	CheckFatal(err, "Unable to read keywords file '%s'", path)
	return strings.Split(string(txt), "\n")
}

func newTwitterClient(creds TwitterCreds) *twitter.Client {
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
	CheckFatal(err, "invalid credentials")

	return twitterClient
}

func reportStats() {
	log.Printf("Reporting stats every %v seconds\n", opts.ReportPeriodSecs)
	for {
		time.Sleep(time.Second * time.Duration(opts.ReportPeriodSecs))
		log.Printf("STATS tweets: %d, commits: %d, json_errs: %d, dgraph_errs: %d\n",
			stats.NumTweets, stats.NumCommits, stats.NumErrorsJson, stats.NumErrorsDgraph)
	}
}

func newApiClients(sockAddr []string) []api.DgraphClient {
	var clients []api.DgraphClient

	for _, sa := range sockAddr {
		conn, err := grpc.Dial(sa, grpc.WithInsecure())
		CheckFatal(err, "Unable to connect to dgraph")
		clients = append(clients, api.NewDgraphClient(conn))
	}

	return clients
}

func unquote(s string) string {
	return s[1 : len(s)-1]
}
