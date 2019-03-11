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
	for jsn := range tweets {
		atomic.AddUint32(&stats.NumTweets, 1)

		tweet, err := json.Marshal(jsn)
		if err != nil {
			atomic.AddUint32(&stats.NumErrorsJson, 1)
			continue
		}

		txn := dgr.NewTxn()
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
	return twitter.NewClient(httpClient)
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
