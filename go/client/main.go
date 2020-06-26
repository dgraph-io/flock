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
	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
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

type twitterUser struct {
	UID              string         `json:"uid,omitempty"`
	DgraphType       string         `json:"dgraph.type,omitempty"`
	UserID           string         `json:"user_id,omitempty"`
	UserName         string         `json:"user_name,omitempty"`
	ScreenName       string         `json:"screen_name,omitempty"`
	Description      string         `json:"description,omitempty"`
	FriendsCount     int            `json:"friends_count,omitempty"`
	FollowersCount   int            `json:"followers_count,omitempty"`
	Verified         bool           `json:"verified,omitempty"`
	ProfileBannerURL string         `json:"profile_banner_url,omitempty"`
	ProfileImageURL  string         `json:"profile_image_url,omitempty"`
	TotalMentions    int64          `json:"total_mentions,omitempty"`
	TotalTweets      int64          `json:"total_tweets,omitempty"`
	Tweet            []twitterTweet `json:"~author,omitempty"`
}

type twitterTweet struct {
	UID        string        `json:"uid,omitempty"`
	DgraphType string        `json:"dgraph.type,omitempty"`
	IDStr      string        `json:"id_str,omitempty"`
	CreatedAt  string        `json:"created_at,omitempty"`
	Message    string        `json:"message,omitempty"`
	URLs       []string      `json:"urls,omitempty"`
	Hashtags   []string      `json:"hashtags,omitempty"`
	Author     twitterUser   `json:"author,omitempty"`
	Mention    []twitterUser `json:"mention,omitempty"`
	Retweet    bool          `json:"retweet,omitempty"`
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
		log.Printf("error in querying dgraph :: %v", err)
		return err
	}

	var r struct {
		QueryData []twitterTweet `json:"dataquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		log.Printf("error in unmarshaling result :: %v", err)
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

	if len(q.hashtags) <= 0 {
		log.Printf("not enough data to run query: %v", query)
		return errInvalidResponse
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
		log.Printf("error in quering dgraph :: %v", err)
		return err
	}

	var r struct {
		QueryData []twitterTweet `json:"dataquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		log.Printf("error in unmarshalling result :: %v", err)
		return err
	}

	// verification
	if len(r.QueryData) <= 0 {
		log.Printf("empty response returned from Dgraph for query: %v", query)
		return errInvalidResponse
	}
	for _, t := range r.QueryData {
		if !strings.Contains(t.Message, hashtag) {
			log.Printf("message doesn't contain hashtag, hashtag: %v, message: %v",
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
			log.Printf("response doesn't contain hashtag, expected: %v, actual: %v",
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
		log.Printf("error in querying dgraph :: %v", err)
		return err
	}

	var r struct {
		QueryData []twitterUser `json:"dataquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		log.Printf("error in unmarshalling result :: %v", err)
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

	if len(q.screenNames) <= 0 {
		log.Printf("not enough data to run query: %v", query)
		return errInvalidResponse
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
    followers_count
    description
  }
}
`
	screenName := q.screenNames[rand.Intn(len(q.screenNames))]
	txn := dgr.NewReadOnlyTxn()
	resp, err := txn.QueryWithVars(context.Background(), query,
		map[string]string{"$screenName": screenName})
	if err != nil {
		log.Printf("error in querying dgraph :: %v", err)
		return err
	}

	var r struct {
		QueryData []twitterUser `json:"dataquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		log.Printf("error in unmarshalling result :: %v", err)
		return err
	}

	// verification
	if len(r.QueryData) <= 0 {
		log.Printf("empty response returned from Dgraph for query: %v", query)
		return errInvalidResponse
	}
	for _, t := range r.QueryData {
		if !strings.Contains(t.ScreenName, screenName) {
			log.Printf("screen name doesn't match, expected: %v, actual: %v",
				screenName, t.ScreenName)
			return errInvalidResponse
		}

		if t.UID == "" || t.UserID == "" {
			log.Printf("response is empty :: %+v", t)
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
    followers_count
    description
    total_mentions : val(a)
  }
}
`, rand.Intn(10))

	txn := dgr.NewReadOnlyTxn()
	resp, err := txn.Query(context.Background(), query)
	if err != nil {
		log.Printf("error in querying dgraph :: %v", err)
		return err
	}

	var r struct {
		QueryData []twitterUser `json:"dataquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		log.Printf("error in unmarshalling result :: %v", err)
		return err
	}

	// verification
	if len(r.QueryData) <= 0 {
		log.Printf("empty response returned from Dgraph for query: %v", query)
		return errInvalidResponse
	}
	prevValue := int64(-1)
	for _, t := range r.QueryData {
		if prevValue != -1 && prevValue < t.TotalMentions {
			log.Printf("the mentions are not sorted, resp: %v", t)
		}

		if t.UID == "" || t.UserID == "" {
			log.Printf("response is empty :: %+v", t)
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

  dataquery(func: uid(a), orderdesc: val(a), first: 100, offset: %v) {
    uid
    screen_name
    user_id
    user_name
    profile_banner_url
    profile_image_url
    friends_count
    followers_count
    description
    total_tweets : val(a)
  }
}
`, rand.Intn(1000))

	txn := dgr.NewReadOnlyTxn()
	resp, err := txn.Query(context.Background(), query)
	if err != nil {
		log.Printf("error in querying dgraph :: %v", err)
		return err
	}

	var r struct {
		QueryData []twitterUser `json:"dataquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		log.Printf("error in unmarshalling result :: %v", err)
		return err
	}

	// verification
	if len(r.QueryData) <= 0 {
		log.Printf("empty response returned from Dgraph for query: %v", query)
		return errInvalidResponse
	}
	prevValue := int64(-1)
	for _, t := range r.QueryData {
		if prevValue != -1 && prevValue < t.TotalTweets {
			log.Printf("the users are not sorted, resp: %v", t)
		}

		if t.UID == "" || t.UserID == "" {
			log.Printf("response is empty :: %+v", t)
			return errInvalidResponse
		}
	}

	return nil
}

// Query Type 5
type queryFive struct {
	userIDs []string
}

func (q *queryFive) getParams(dgr *dgo.Dgraph) error {
	query := fmt.Sprintf(`
{
  dataquery(func: has(user_id), first: 100, offset: %v) {
    user_id
  }
}
`, rand.Intn(1000))

	txn := dgr.NewReadOnlyTxn()
	resp, err := txn.Query(context.Background(), query)
	if err != nil {
		log.Printf("error in querying dgraph :: %v", err)
		return err
	}

	var r struct {
		QueryData []twitterUser `json:"dataquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		log.Printf("error in unmarshalling result :: %v", err)
		return err
	}

	userIDs := make(map[string]bool)
	for _, u := range r.QueryData {
		if u.UserID != "" {
			userIDs[u.UserID] = true
		}
	}

	q.userIDs = make([]string, 0, len(userIDs))
	for h := range userIDs {
		q.userIDs = append(q.userIDs, h)
	}

	if len(q.userIDs) <= 0 {
		log.Printf("not enough data to run query: %v", query)
		return errInvalidResponse
	}

	return nil
}

func (q *queryFive) runQuery(dgr *dgo.Dgraph) error {
	const query = `
query all($userID: string) {
  dataquery(func: eq(user_id, $userID)) {
    uid
    screen_name
    user_id
    user_name
    profile_banner_url
    profile_image_url
    friends_count
    followers_count
    description
  }
}
`
	userID := q.userIDs[rand.Intn(len(q.userIDs))]
	txn := dgr.NewReadOnlyTxn()
	resp, err := txn.QueryWithVars(context.Background(), query,
		map[string]string{"$userID": userID})
	if err != nil {
		log.Printf("error in querying dgraph :: %v", err)
		return err
	}

	var r struct {
		QueryData []twitterUser `json:"dataquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		log.Printf("error in unmarshalling result :: %v", err)
		return err
	}

	// verification
	if len(r.QueryData) <= 0 {
		log.Printf("empty response returned from Dgraph for query: %v", query)
		return errInvalidResponse
	}
	for _, t := range r.QueryData {
		if !strings.Contains(t.UserID, userID) {
			log.Printf("screen name doesn't match, expected: %v, actual: %v",
				userID, t.UserID)
			return errInvalidResponse
		}

		if t.UID == "" || t.ScreenName == "" {
			log.Printf("response is empty :: %+v", t)
			return errInvalidResponse
		}
	}

	return nil
}

// Query Type 6
type querySix struct {
	queryOne
}

func (q *querySix) getParams(dgr *dgo.Dgraph) error {
	// we subtract 41 hours because that's the latest data we get from twitter
	curTime := time.Now().Add(-41 * time.Hour)

	query := fmt.Sprintf(`
{
  dataquery(func:has(hashtags), first: 100, offset: %v) @filter(ge(created_at, "%v")) {
    hashtags
    created_at
  }
}
`, rand.Intn(1000), curTime.Format(time.RFC3339))

	txn := dgr.NewReadOnlyTxn()
	resp, err := txn.Query(context.Background(), query)
	if err != nil {
		log.Printf("error in quering dgraph :: %v", err)
		return err
	}

	var r struct {
		QueryData []twitterTweet `json:"dataquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		log.Printf("error in unmarshaling result :: %v", err)
		return err
	}

	// verify that our query returned tweets with newer timestamps
	for _, t := range r.QueryData {
		c, err := time.Parse(time.RFC3339, t.CreatedAt)
		if err != nil {
			log.Printf("dgraph returned unparse-able timestamp: %v :: %v", t.CreatedAt, err)
			return err
		}

		if !c.After(curTime) {
			log.Printf("dgraph returned old ts, query: %v, ret: %v, cur: %v", query, c, curTime)
			return errInvalidResponse
		}
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

	if len(q.hashtags) <= 0 {
		log.Printf("not enough data to run query: %v", query)
		return errInvalidResponse
	}

	return nil
}

// Query Type 7
type querySeven struct {
	queryTwo
}

func (q *querySeven) getParams(dgr *dgo.Dgraph) error {
	// we subtract 41 hours because that's the latest data we get from twitter
	curTime := time.Now().Add(-41 * time.Hour)

	query := fmt.Sprintf(`
{
  dataquery(func: has(screen_name), first: 100, offset: %v) @cascade {
    screen_name
    ~author @filter(ge(created_at, "%v")) {
      created_at
    }
  }
}
`, rand.Intn(1000), curTime.Format(time.RFC3339))

	txn := dgr.NewReadOnlyTxn()
	resp, err := txn.Query(context.Background(), query)
	if err != nil {
		log.Printf("error in querying dgraph :: %v", err)
		return err
	}

	var r struct {
		QueryData []twitterUser `json:"dataquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		log.Printf("error in unmarshalling result :: %v", err)
		return err
	}

	// verify that our query returned tweets with newer timestamps
	for _, t := range r.QueryData {
		c, err := time.Parse(time.RFC3339, t.Tweet[0].CreatedAt)
		if err != nil {
			log.Printf("dgraph returned unparse-able timestamp: %v :: %v", t.Tweet[0].CreatedAt, err)
			return err
		}

		if !c.After(curTime) {
			log.Printf("dgraph returned old ts, query: %v, ret: %v, cur: %v", query, c, curTime)
			return errInvalidResponse
		}
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

	if len(q.screenNames) <= 0 {
		log.Printf("not enough data to run query: %v", query)
		return errInvalidResponse
	}

	return nil
}

// Query Type 8
type queryEight struct {
	queryFour
}

func (q *queryEight) runQuery(dgr *dgo.Dgraph) error {
	// we subtract 41 hours because that's the latest data we get from twitter
	curTime := time.Now().Add(-41 * time.Hour)

	query := fmt.Sprintf(`
{
  var(func: has(user_id)) {
    a as count(~author) @filter(ge(created_at, "%v"))
  }

  dataquery(func: uid(a), orderdesc: val(a), first: 100, offset: %v) @cascade {
    uid
    screen_name
    user_id
    user_name
    profile_banner_url
    profile_image_url
    friends_count
    followers_count
    description
    total_tweets : val(a)
    ~author @filter(ge(created_at, "%v")) {
      created_at
    }
  }
}
`, curTime.Format(time.RFC3339), rand.Intn(1000), curTime.Format(time.RFC3339))

	txn := dgr.NewReadOnlyTxn()
	resp, err := txn.Query(context.Background(), query)
	if err != nil {
		log.Printf("error in querying dgraph, query: %v :: %v", query, err)
		return err
	}

	var r struct {
		QueryData []twitterUser `json:"dataquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		log.Printf("error in unmarshalling result :: %v", err)
		return err
	}

	// verify that our query returned tweets with newer timestamps
	for _, t := range r.QueryData {
		c, err := time.Parse(time.RFC3339, t.Tweet[0].CreatedAt)
		if err != nil {
			log.Printf("dgraph returned unparse-able timestamp: %v :: %v", t.Tweet[0].CreatedAt, err)
			return err
		}

		if !c.After(curTime) {
			log.Printf("dgraph returned old ts, query: %v, ret: %v, cur: %v", query, c, curTime)
			return errInvalidResponse
		}
	}

	// verification
	if len(r.QueryData) <= 0 {
		log.Printf("empty response returned from Dgraph for query: %v", query)
		return errInvalidResponse
	}
	prevValue := int64(-1)
	for _, t := range r.QueryData {
		if prevValue != -1 && prevValue < t.TotalTweets {
			log.Printf("the users are not sorted, resp: %v", t)
		}

		if t.UID == "" || t.UserID == "" {
			log.Printf("response is empty :: %+v", t)
			return errInvalidResponse
		}
	}

	return nil
}

// Query Type 9
type queryNine struct {
	queryFive
}

func (q *queryNine) getParams(dgr *dgo.Dgraph) error {
	// we subtract 41 hours because that's the latest data we get from twitter
	curTime := time.Now().Add(-41 * time.Hour)

	query := fmt.Sprintf(`
{
  dataquery(func: has(user_id), first: 100, offset: %v) @cascade {
		user_id
		~author @filter(ge(created_at, "%v")) {
      created_at
    }
  }
}
`, rand.Intn(1000), curTime.Format(time.RFC3339))

	txn := dgr.NewReadOnlyTxn()
	resp, err := txn.Query(context.Background(), query)
	if err != nil {
		log.Printf("error in querying dgraph :: %v", err)
		return err
	}

	var r struct {
		QueryData []twitterUser `json:"dataquery"`
	}
	if err := json.Unmarshal(resp.Json, &r); err != nil {
		log.Printf("error in unmarshalling result :: %v", err)
		return err
	}

	// verify that our query returned tweets with newer timestamps
	for _, t := range r.QueryData {
		c, err := time.Parse(time.RFC3339, t.Tweet[0].CreatedAt)
		if err != nil {
			log.Printf("dgraph returned unparse-able timestamp: %v :: %v", t.Tweet[0].CreatedAt, err)
			return err
		}

		if !c.After(curTime) {
			log.Printf("dgraph returned old ts, query: %v, ret: %v, cur: %v", query, c, curTime)
			return errInvalidResponse
		}
	}

	userIDs := make(map[string]bool)
	for _, u := range r.QueryData {
		if u.UserID != "" {
			userIDs[u.UserID] = true
		}
	}

	q.userIDs = make([]string, 0, len(userIDs))
	for h := range userIDs {
		q.userIDs = append(q.userIDs, h)
	}

	if len(q.userIDs) <= 0 {
		log.Printf("not enough data to run query: %v", query)
		return errInvalidResponse
	}

	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	allQueries := []dgraphQuery{
		&queryOne{}, &queryOne{}, &queryOne{}, &queryOne{}, &queryOne{}, &queryOne{},
		&queryTwo{}, &queryTwo{}, &queryTwo{}, &queryTwo{}, &queryTwo{}, &queryTwo{}, &queryTwo{},
		&queryThree{}, &queryThree{}, &queryThree{}, &queryThree{}, &queryThree{},
		&queryFour{}, &queryFour{}, &queryFour{}, &queryFour{}, &queryFour{},
		&queryFive{}, &queryFive{}, &queryFive{},
		&querySix{}, &querySix{}, &querySix{},
		&querySeven{}, &querySeven{},
		&queryEight{}, &queryEight{}, &queryEight{}, &queryEight{},
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
	log.Printf("Using %v dgraph clients on %v alphas",
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
		// run parameter query
		th.Do()
		err := query.getParams(dgr)
		th.Done(nil)

		if err != nil {
			atomic.AddUint32(&stats.Failures, 1)
			log.Printf("error in running parameter query :: %v", err)
			continue
		}

		atomic.AddUint32(&stats.Success, 1)

		// run actual queries
		for i := 0; i < 100; i++ {
			th.Do()
			err := query.runQuery(dgr)
			th.Done(nil)

			if err != nil {
				atomic.AddUint32(&stats.Failures, 1)
				log.Printf("error in running query :: %v", err)
				continue
			}

			atomic.AddUint32(&stats.Success, 1)
		}
	}
}

// TODO: fix the race condition here
func reportStats() {
	var oldStats, newStats progStats
	log.Printf("Reporting stats every %v seconds", opts.ReportPeriodSecs)
	for {
		time.Sleep(time.Second * time.Duration(opts.ReportPeriodSecs))

		oldStats = newStats
		newStats = stats
		log.Printf("STATS success: %d, failures: %d, query_rate: %d/sec",
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
