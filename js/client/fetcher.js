// Fetch a random integer
function getRandomInt(max) {
    return Math.floor(Math.random() * max);
}

// Fetch date-time difference from the current one in ISO format
function getISODateTime(diff) {
    const currentDate = new Date();
    currentDate.setHours( currentDate.getHours() - diff );
    return currentDate.toISOString();
}

// Fetch the query needed for the getParams function of Query class
function paramsQuery(index) {
    const queries = [
        `{ dataquery(func: has(hashtags), first: 100, offset: ${getRandomInt(1000)}) { hashtags } }`,
        `{ dataquery(func: has(screen_name), first: 100, offset: ${getRandomInt(1000)}) { screen_name } }`,
        null,
        null,
        `{ dataquery(func: has(user_id), first: 100, offset: ${getRandomInt(1000)}) { user_id } }`,
        `{ dataquery(func: has(hashtags), first: 100, offset: ${getRandomInt(1000)}) @filter(ge(created_at, "${getISODateTime(41)}")) { hashtags created_at } }`,
        `{ dataquery(func: has(screen_name), first: 100, offset: ${getRandomInt(1000)}) @cascade { screen_name ~author @filter(ge(created_at, "${getISODateTime(41)}")) { created_at } } }`,
        null,
        `{ dataquery(func: has(user_id), first: 100, offset: ${getRandomInt(1000)}) @cascade { user_id ~author @filter(ge(created_at, "${getISODateTime(41)}")) { created_at } } }`
    ];
    return queries[index-1];
}

// Fetch the query needed for the runQuery function of Query class
function runQuery(index) {
    const queries = [
        "query all($var: string) { dataquery(func: eq(hashtags, $var)) { uid id_str retweet message hashtags } }",
        "query all($var: string) { dataquery(func: eq(screen_name, $var)) { uid screen_name user_id user_name profile_banner_url profile_image_url friends_count followers_count description } }",
        `{ var(func: has(<~mention>)) { ~mention @groupby(mention) { a as count(uid) } } dataquery(func: uid(a), orderdesc: val(a), first: 100, offset: ${getRandomInt(1000)}) { uid screen_name user_id user_name profile_banner_url profile_image_url friends_count followers_count description total_mentions : val(a) } }`,
        `{ var(func: has(user_id)) { a as count(~author) } dataquery(func: uid(a), orderdesc: val(a), first: 100, offset: ${getRandomInt(1000)}) { uid screen_name user_id user_name profile_banner_url profile_image_url friends_count followers_count description total_tweets : val(a) } }`,
        "query all($var: string) { dataquery(func: eq(user_id, $var)) { uid screen_name user_id user_name profile_banner_url profile_image_url friends_count followers_count description } }",
        null,
        null,
        `{ var(func: has(user_id)) { a as count(~author) @filter(ge(created_at, "${getISODateTime(41)}")) } dataquery(func: uid(a), orderdesc: val(a), first: 100, offset: ${getRandomInt(1000)}) @cascade { uid screen_name user_id user_name profile_banner_url profile_image_url friends_count followers_count description total_tweets : val(a) ~author @filter(ge(created_at, "${getISODateTime(41)}")) { created_at } } }`,
        null
    ];
    return queries[index-1];
}

// Fetch the reference needed for extracting data from getParam function
function reference(index) {
    const references = [
        "hashtags",
        "screen_name",
        null,
        null,
        "user_id",
        "hashtags",
        "screen_name",
        null,
        "user_id"
    ];
    return references[index-1];
}

module.exports = {
    paramsQuery,
    runQuery,
    reference
};