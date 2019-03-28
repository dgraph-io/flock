package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
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
type queryOne struct {
	hashtags []string
}

func (q *queryOne) getParams(dgr *dgo.Dgraph) error {
	query := fmt.Sprintf(`
{
  dataquery(func:has(hashtags), first: 100, offset: %v) {
    hashtags
  }
}
`, rand.Intn(1000))

	txn := dgr.NewReadOnlyTxn()
	resp, err := txn.Query(context.Background(), query)
	if err != nil {
		log.Printf("error in quering dgraph :: %v\n", err)
		return err
	}

	var r struct {
		QueryData []struct {
			Hashtags []string `json:"hashtags"`
		} `json:"dataquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		log.Printf("error in unmarshaling result :: %v\n", err)
		return err
	}

	hashtags := make(map[string]bool)
	for _, t := range r.QueryData {
		for _, h := range t.Hashtags {
			if h != "" {
				hashtags[h] = true
			}
		}
	}

	q.hashtags = make([]string, 0, len(hashtags))
	for h := range hashtags {
		q.hashtags = append(q.hashtags, h)
	}

	return nil
}

func (q *queryOne) runQuery(dgr *dgo.Dgraph) error {
	const query = `
query all($tagVal: string) {
  dataquery(func: eq(hashtags, $tagVal))
  {
    uid
    id_str
    retweet
    message
    hashtags
  }
}
`
	hashtag := q.hashtags[rand.Intn(len(q.hashtags))]
	txn := dgr.NewReadOnlyTxn()
	resp, err := txn.QueryWithVars(context.Background(), query,
		map[string]string{"$tagVal": hashtag})
	if err != nil {
		log.Printf("error in quering dgraph :: %v\n", err)
		return err
	}

	var r struct {
		QueryData []struct {
			UID      string   `json:"uid"`
			IDStr    string   `json:"id_str"`
			Retweet  bool     `json:"retweet"`
			Message  string   `json:"message"`
			Hashtags []string `json:"hashtags"`
		} `json:"dataquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		log.Printf("error in unmarshalling result :: %v\n", err)
		return err
	}

	// ensure that each query has the hashtag that we asked for
	for _, t := range r.QueryData {
		if !strings.Contains(t.Message, hashtag) {
			log.Printf("message doesn't contain hashtag, hashtag: %v, message: %v\n",
				hashtag, t.Message)
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
			log.Printf("response doesn't contain hashtag, expected: %v, actual: %v\n",
				hashtag, t.Hashtags)
			return errInvalidResponse
		}
	}

	return nil
}

// Query Type 2
type queryTwo struct {
	screenNames []string
}

func (q *queryTwo) getParams(dgr *dgo.Dgraph) error {
	query := fmt.Sprintf(`
{
  dataquery(func: has(screen_name), first: 100, offset: %v) {
    screen_name
  }
}
`, rand.Intn(1000))

	txn := dgr.NewReadOnlyTxn()
	resp, err := txn.Query(context.Background(), query)
	if err != nil {
		log.Printf("error in querying dgraph :: %v\n", err)
		return err
	}

	var r struct {
		QueryData []struct {
			ScreenName string `json:"screen_name"`
		} `json:"dataquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		log.Printf("error in unmarshalling result :: %v\n", err)
		return err
	}

	screenNames := make(map[string]bool)
	for _, u := range r.QueryData {
		if u.ScreenName != "" {
			screenNames[u.ScreenName] = true
		}
	}

	q.screenNames = make([]string, 0, len(screenNames))
	for h := range screenNames {
		q.screenNames = append(q.screenNames, h)
	}

	return nil
}

func (q *queryTwo) runQuery(dgr *dgo.Dgraph) error {
	const query = `
query all($screenName: string) {
  dataquery(func: eq(screen_name, $screenName)) {
    uid
    screen_name
    user_id
    user_name
    profile_banner_url
    profile_image_url
    friends_count
    description
  }
}
`
	screenName := q.screenNames[rand.Intn(len(q.screenNames))]
	txn := dgr.NewReadOnlyTxn()
	resp, err := txn.QueryWithVars(context.Background(), query,
		map[string]string{"$screenName": screenName})
	if err != nil {
		log.Printf("error in querying dgraph :: %v\n", err)
		return err
	}

	var r struct {
		QueryData []struct {
			UID          string `json:"uid"`
			ScreenName   string `json:"screen_name"`
			UserID       string `json:"user_id"`
			UserName     string `json:"user_name"`
			BannerURL    string `json:"profile_banner_url"`
			ImageURL     string `json:"profile_image_url"`
			FriendsCount int64  `json:"friends_count"`
			Description  string `json:"description"`
		} `json:"dataquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		log.Printf("error in unmarshalling result :: %v\n", err)
		return err
	}

	// verification
	for _, t := range r.QueryData {
		if !strings.Contains(t.ScreenName, screenName) {
			log.Printf("screen name doesn't match, expected: %v, actual: %v\n",
				screenName, t.ScreenName)
			return errInvalidResponse
		}

		if t.UID == "" || t.UserID == "" || t.UserName == "" || t.ImageURL == "" {
			log.Printf("response is empty :: %+v\n", t)
			return errInvalidResponse
		}
	}

	return nil
}

// Query Type 3
type queryThree struct{}

func (q *queryThree) getParams(dgr *dgo.Dgraph) error {
	return nil
}

func (q *queryThree) runQuery(dgr *dgo.Dgraph) error {
	query := fmt.Sprintf(`
{
  var(func: has(<~mention>)) {
    ~mention @groupby(mention) {
      a as count(uid)
    }
  }

  dataquery(func: uid(a), orderdesc: val(a), first: 100, offset: %v) {
    uid
    screen_name
    user_id
    user_name
    profile_banner_url
    profile_image_url
    friends_count
    description
    total_mentions : val(a)
  }
}
`, rand.Intn(1000))

	txn := dgr.NewReadOnlyTxn()
	resp, err := txn.Query(context.Background(), query)
	if err != nil {
		log.Printf("error in querying dgraph :: %v\n", err)
		return err
	}

	var r struct {
		QueryData []struct {
			UID           string `json:"uid"`
			ScreenName    string `json:"screen_name"`
			UserID        string `json:"user_id"`
			UserName      string `json:"user_name"`
			BannerURL     string `json:"profile_banner_url"`
			ImageURL      string `json:"profile_image_url"`
			FriendsCount  int64  `json:"friends_count"`
			Description   string `json:"description"`
			TotalMentions int64  `json:"total_mentions"`
		} `json:"dataquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		log.Printf("error in unmarshalling result :: %v\n", err)
		return err
	}

	// verification
	prevValue := int64(-1)
	for _, t := range r.QueryData {
		if prevValue != -1 && prevValue < t.TotalMentions {
			log.Printf("the mentions are not sorted, resp: %v\n", t)
		}

		if t.UID == "" || t.UserID == "" || t.UserName == "" || t.ImageURL == "" {
			log.Printf("response is empty :: %+v\n", t)
			return errInvalidResponse
		}
	}

	return nil
}

// Query Type 4
type queryFour struct{}

func (q *queryFour) getParams(dgr *dgo.Dgraph) error {
	return nil
}

func (q *queryFour) runQuery(dgr *dgo.Dgraph) error {
	query := fmt.Sprintf(`
{
  var(func: has(user_id)) {
    a as count(~author)
  }

  q(func: uid(a), orderdesc: val(a), first: 100, offset: %v) {
    uid
    screen_name
    user_id
    user_name
    profile_banner_url
    profile_image_url
    friends_count
    description
    total_tweets : val(a)
  }
}
`, rand.Intn(1000))

	txn := dgr.NewReadOnlyTxn()
	resp, err := txn.Query(context.Background(), query)
	if err != nil {
		log.Printf("error in querying dgraph :: %v\n", err)
		return err
	}

	var r struct {
		QueryData []struct {
			UID          string `json:"uid"`
			ScreenName   string `json:"screen_name"`
			UserID       string `json:"user_id"`
			UserName     string `json:"user_name"`
			BannerURL    string `json:"profile_banner_url"`
			ImageURL     string `json:"profile_image_url"`
			FriendsCount int64  `json:"friends_count"`
			Description  string `json:"description"`
			TotalTweets  int64  `json:"total_tweets"`
		} `json:"dataquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		log.Printf("error in unmarshalling result :: %v\n", err)
		return err
	}

	// verification
	prevValue := int64(-1)
	for _, t := range r.QueryData {
		if prevValue != -1 && prevValue < t.TotalTweets {
			log.Printf("the users are not sorted, resp: %v\n", t)
		}

		if t.UID == "" || t.UserID == "" || t.UserName == "" || t.ImageURL == "" {
			log.Printf("response is empty :: %+v\n", t)
			return errInvalidResponse
		}
	}

	return nil
}

func main() {
	allQueries := []dgraphQuery{
		&queryOne{},
		&queryOne{},
		&queryOne{},
		&queryOne{},
		&queryOne{},
		&queryOne{},
		&queryOne{},
		&queryOne{},
		&queryOne{},
		&queryOne{},
		&queryTwo{},
		&queryTwo{},
		&queryTwo{},
		&queryTwo{},
		&queryTwo{},
		&queryTwo{},
		&queryTwo{},
		&queryTwo{},
		&queryTwo{},
		&queryTwo{},
		&queryThree{},
		&queryThree{},
		&queryThree{},
		&queryThree{},
		&queryThree{},
		&queryFour{},
		&queryFour{},
		&queryFour{},
		&queryFour{},
		&queryFour{},
		&queryFour{},
		&queryFour{},
	}

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
	for _, query := range allQueries {
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

		for i := 0; i < 100; i++ {
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
