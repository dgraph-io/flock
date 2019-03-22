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
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
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
	NumClients       int
	CredentialsFile  string
	Timeout          time.Duration
	ReportPeriodSecs int
	CommitFalseProb  float64
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

func runInserter(alphas []api.DgraphClient, c *y.Closer, tweets <-chan interface{}) {
	defer c.Done()

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
			if errTweet := updateFilteredTweet(ft, txn); errTweet != nil {
				atomic.AddUint32(&stats.ErrorsDgraph, 1)
				continue
			}

			tweet, err := json.Marshal(ft)
			if err != nil {
				atomic.AddUint32(&stats.ErrorsJSON, 1)
				continue
			}

			commitNow := true
			if rand.Float64() < opts.CommitFalseProb {
				commitNow = false
			}

			// only ONE retry attempt is made
			retry := true
		RETRY:
			mut := &api.Mutation{SetJson: tweet, CommitNow: commitNow}
			_, err = txn.Mutate(context.Background(), mut)
			switch {
			case err == nil:
				if commitNow {
					atomic.AddUint32(&stats.Commits, 1)
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

	// map to check for duplicates
	users := make(map[string]string)

	userID := ft.Author.UserID
	if u, err := queryUser(txn, &ft.Author); err != nil {
		return err
	} else if u != nil {
		ft.Author = *u
	}
	users[userID] = ft.Author.UID

	userMentions := make([]twitterUser, 0)
	for i, m := range ft.Mention {
		if dup, ok := users[m.UserID]; ok && dup != "" {
			userMentions = append(userMentions, twitterUser{UID: dup})
			continue
		} else if ok && dup == "" {
			// TODO: find a way to not ignore this mention
			continue
		}

		userID := m.UserID
		if u, err := queryUser(txn, &m); err != nil {
			return err
		} else if u != nil {
			ft.Mention[i] = *u
		}
		users[userID] = m.UID
	}
	ft.Mention = userMentions

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

func getTrends(id int64, api *anaconda.TwitterApi) ([]string, error) {
	resp, err := api.GetTrendsByPlace(id, nil)
	if err != nil {
		return nil, err
	}

	trends := make([]string, len(resp.Trends))
	for i, t := range resp.Trends {
		trends[i] = t.Name
	}

	return trends, nil
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

func main() {
	dgclients := flag.Int("l", 8, "number of dgraph clients to run")
	credentialsFile := flag.String("c", "credentials.json", "path to credentials file")
	programDuration := flag.Int64("t", 3600, "duration in seconds for program")
	commitFalseProb := flag.Float64("p", 0, "prob of CommitNow=False, from 0 to 1")
	alphasAddress := flag.String("a", ":9180,:9182,:9183", "comma separated addresses to alphas")
	flag.Parse()

	if *commitFalseProb > 1 || *commitFalseProb < 0 {
		log.Fatalf("invalid value for commit=false probability")
	}
	opts = progOptions{
		NumClients:       *dgclients,
		CredentialsFile:  *credentialsFile,
		Timeout:          time.Duration(*programDuration) * time.Second,
		ReportPeriodSecs: 2,
		CommitFalseProb:  *commitFalseProb,
		AlphaSockAddr:    strings.Split(*alphasAddress, ","),
	}

	creds := readCredentials(opts.CredentialsFile)
	client := newTwitterClient(creds)
	alphas := newAPIClients(opts.AlphaSockAddr)

	// setup schema
	dgr := dgo.NewDgraphClient(alphas...)
	op := &api.Operation{
		Schema: cDgraphSchema,
	}
	err := dgr.Alter(context.Background(), op)
	checkFatal(err, "error in creating indexes")

	// report stats
	go reportStats()
	log.Printf("Using %v dgraph clients on %v alphas\n",
		opts.NumClients, len(opts.AlphaSockAddr))

	// read twitter stream
	for {
		// choose top 20 trending keywords
		log.Println("Getting trends...")
		trends, err := getTrends(1, client)
		if len(trends) > 20 {
			trends = trends[:20]
		}
		checkFatal(err, "error in getting trends")

		// stream := client.PublicStreamFilter(map[string][]string{"track": allKwds})
		stream := client.PublicStreamSample(nil)
		log.Printf("Updating keywords to: %s\n", strings.Join(trends, ", "))

		c := y.NewCloser(0)
		for i := 0; i < opts.NumClients; i++ {
			c.AddRunning(1)
			go runInserter(alphas, c, stream.C)
		}

		time.Sleep(opts.Timeout)
		log.Println("Stopping stream...")
		stream.Stop()
		c.SignalAndWait()
		log.Println("Stream stopped.")
	}
}
