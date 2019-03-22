package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/y"
	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
	"google.golang.org/grpc"
)

var (
	opts  progOptions
	stats progStats

	errInvalidResponse = errors.New("response from Dgraph is unexpected")
)

type progOptions struct {
	NumDgrClients    int
	QueriesFile      string
	ReportPeriodSecs int
	NumQueryAtATime  int
	AlphaSockAddr    []string
}

type progStats struct {
	Success  uint32
	Failures uint32
}

// dgraphQuery interface represents an agent query
type dgraphQuery interface {
	// getParams is called infrequently to query parameters for the actual query
	getParams(dgr *dgo.Dgraph) error
	// runQuery runs the actual query
	runQuery(dgr *dgo.Dgraph) error
}

// Query Type 1
type hashtagQuery struct {
	hashtags []string
}

func (hq *hashtagQuery) getParams(dgr *dgo.Dgraph) error {
	const query = `
{
  hquery(func:has(hashtags), first: 100)
  {
    hashtags
  }
}
`
	hashtags := make(map[string]bool)
	txn := dgr.NewReadOnlyTxn()
	resp, err := txn.Query(context.Background(), query)
	if err != nil {
		return err
	}

	var r struct {
		HQuery []struct {
			Hashtags []string `json:"hashtags"`
		} `json:"hquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		return err
	}

	for _, hts := range r.HQuery {
		for _, h := range hts.Hashtags {
			if h != "" {
				hashtags[h] = true
			}
		}
	}

	hq.hashtags = make([]string, 0, len(hashtags))
	for h := range hashtags {
		hq.hashtags = append(hq.hashtags, h)
	}

	return nil
}

func (hq *hashtagQuery) runQuery(dgr *dgo.Dgraph) error {
	const query = `
query all($tagVal: string) {
  hquery(func: eq(hashtags, $tagVal))
  {
    uid
    id_str
    retweet
    message
		hashtags
  }
}
`
	hashtag := hq.hashtags[rand.Intn(len(hq.hashtags))]
	txn := dgr.NewReadOnlyTxn()
	resp, err := txn.QueryWithVars(context.Background(), query,
		map[string]string{"$tagVal": hashtag})
	if err != nil {
		return err
	}

	var r struct {
		HQuery []struct {
			UID      string   `json:"uid"`
			IDStr    string   `json:"id_str"`
			Retweet  bool     `json:"retweet"`
			Message  string   `json:"message"`
			Hashtags []string `json:"hashtags"`
		} `json:"hquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		return err
	}

	// ensure that each query has the hashtag that we asked for
	for _, t := range r.HQuery {
		if !strings.Contains(t.Message, hashtag) {
			return errInvalidResponse
		}

		found := false
		for _, h := range t.Hashtags {
			if h == hashtag {
				found = true
				break
			}
		}

		if !found {
			return errInvalidResponse
		}
	}

	return nil
}

func main() {
	dgclients := flag.Int("l", 6, "number of dgraph clients to run")
	queriesAtATime := flag.Int("q", 4, "number of queries running at a time")
	alphasAddress := flag.String("a", ":9180,:9182,:9183", "comma separated addresses to alphas")
	flag.Parse()

	opts = progOptions{
		NumDgrClients:    *dgclients,
		ReportPeriodSecs: 2,
		NumQueryAtATime:  *queriesAtATime,
		AlphaSockAddr:    strings.Split(*alphasAddress, ","),
	}

	alphas, err := newAPIClients(opts.AlphaSockAddr)
	if err != nil {
		log.Println("error in creating dgraph clients ::", err)
		panic(err)
	}

	// report stats
	go reportStats()
	log.Printf("Using %v dgraph clients on %v alphas\n",
		opts.NumDgrClients, len(opts.AlphaSockAddr))

	// run queries
	var wg sync.WaitGroup
	th := y.NewThrottle(opts.NumQueryAtATime)
	for _, query := range []dgraphQuery{&hashtagQuery{}} {
		wg.Add(1)
		go runQuery(alphas, &wg, th, query)
	}

	wg.Wait()
}

func runQuery(alphas []api.DgraphClient, wg *sync.WaitGroup,
	th *y.Throttle, query dgraphQuery) {

	defer wg.Done()

	dgr := dgo.NewDgraphClient(alphas...)
	for {
		th.Do()
		if err := query.getParams(dgr); err != nil {
			atomic.AddUint32(&stats.Failures, 1)
			log.Printf("error in running parameter query :: %v", err)
			continue
		}
		th.Done(nil)

		for i := 0; i < 1000; i++ {
			th.Do()
			if err := query.runQuery(dgr); err != nil {
				atomic.AddUint32(&stats.Failures, 1)
				log.Printf("error in running query :: %v", err)
				continue
			}
			th.Done(nil)

			atomic.AddUint32(&stats.Success, 1)
		}
	}
}

// TODO: fix the race condition here
func reportStats() {
	var oldStats, newStats progStats
	log.Printf("Reporting stats every %v seconds\n", opts.ReportPeriodSecs)
	for {
		time.Sleep(time.Second * time.Duration(opts.ReportPeriodSecs))

		oldStats = newStats
		newStats = stats
		log.Printf("STATS success: %d, failures: %d, query_rate: %d/sec\n",
			newStats.Success, newStats.Failures,
			(newStats.Success-oldStats.Success)/uint32(opts.ReportPeriodSecs))
	}
}

func newAPIClients(sockAddr []string) ([]api.DgraphClient, error) {
	var clients []api.DgraphClient

	for _, sa := range sockAddr {
		conn, err := grpc.Dial(sa, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}

		clients = append(clients, api.NewDgraphClient(conn))
	}

	return clients, nil
}
