#!/usr/bin/env python
# coding: utf-8

import pymongo
import tweepy
import time
import yaml
import gspread
import dateutil.parser as parser

from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

# ### Notes from the Twitter's API
# https://developer.twitter.com/en/docs/tweets/timelines/api-reference/get-statuses-user_timeline
# 
# This method can only return up to **3,200 of a user's most recent Tweets**.
# Native retweets of other statuses by the user is included in this total,
# regardless of whether include_rts is set to false when requesting this resource.
# 
# - ** Maximum number of tweets by request : 200 **
# - ** Requests / 15-min window (app auth): 1500 **

# General config
NB_ACCOUNTS_TO_CHECK = 10

# GSheet config
GSHEET_ACCOUNTS_COLUMN_NB = 6
GSHEET_ACCOUNTS_COLUMN_NAME = 'Compte Twitter'
GSHEET_CONTROL_COLUMN_NAME = 'Last check'
GSHEET_CREDENTIALS_FILE = './private/covid19-b40a0237c297.json'
GSHEET_NAME = 'twitter_accounts'

# Witter API and tweepy config
SLEEP_TIME = 900  # 900 seconds = 15 minutes
COUNT_MAX = 200  # max tweets by request
INCLUDE_RETWEETS = True
API_CREDENTIALS = yaml.safe_load(open("./private/config.yml"))

# MongoDB config config
MONGO_BD_NAME = 'covid19'


# Get Google sheet object
def get_sheet_object():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(GSHEET_CREDENTIALS_FILE, scope)
    g_spread_client = gspread.authorize(credentials)
    return g_spread_client.open(GSHEET_NAME).sheet1


def tweepy_api_init():
    auth = tweepy.OAuthHandler(API_CREDENTIALS['consumer_key'], API_CREDENTIALS['consumer_secret'])
    auth.set_access_token(API_CREDENTIALS['access_token'], API_CREDENTIALS['access_token_secret'])
    return tweepy.API(auth)


def get_accounts_from_google_sheets():
    data = sheet.get_all_records()

    # index + 2 is the row index in google sheets
    # don't take empty cells -> len(tweet[GSHEET_ACCOUNTS_COLUMN_NAME]) > 0
    accounts = [(index + 2, tweet[GSHEET_ACCOUNTS_COLUMN_NAME], tweet[GSHEET_CONTROL_COLUMN_NAME])
                for index, tweet in enumerate(data) if len(tweet[GSHEET_ACCOUNTS_COLUMN_NAME]) > 0]

    # returns a list of tuples such as : 
    # (7, '@USTreasury', '2020-04-20 16:17:32') if the date exists -> update mode
    # (2, '@realDonaldTrump', '') if the date doesn't exist -> insert mode 

    today = datetime.now().strftime('%Y-%m-%d')

    # select if the control date is empty or older than today
    accounts = [account for account in accounts if len(account[2]) == 0 or account[2].split()[0] < today]

    # return up to NB_ACCOUNTS_TO_CHECK

    if len(accounts) >= NB_ACCOUNTS_TO_CHECK:
        return accounts[:NB_ACCOUNTS_TO_CHECK]
    else:
        return accounts


def max_tweets_limit_notice(screen_name, error):
    print('Limit reached for ' + screen_name)
    print(error.message)  # just to check the exact nature of the error
    print('Wait 15 minutes ...')
    time.sleep(SLEEP_TIME)
    print('New request for ' + screen_name)


def get_last_minus_one(screen_name, sort_type='oldest'):
    sort_parameter = pymongo.ASCENDING
    if sort_type != 'oldest':
        sort_parameter = pymongo.DESCENDING
    last_two = tweets_db.find({'screen_name': screen_name}).sort('date_iso', sort_parameter).limit(2)
    try:
        last_minus_one: object = last_two[1]['id']
        return last_minus_one
    except IndexError as exception:
        print(exception)
        return False


def get_newest_id(screen_name): return get_last_minus_one(screen_name, 'newest')


def get_oldest_id(screen_name): return get_last_minus_one(screen_name, 'oldest')


def get_initial_tweets(screen_name):
    api = tweepy_api_init()

    print('Get initial request with most recent tweets for ' + screen_name)

    try:

        new_tweets = api.user_timeline(screen_name=screen_name,
                                       count=COUNT_MAX,
                                       tweet_mode='extended',
                                       include_rts=INCLUDE_RETWEETS)

    except tweepy.TweepError as error:

        max_tweets_limit_notice(screen_name, error)

        print('Try again to get the initial tweets list for ' + screen_name)

        new_tweets = api.user_timeline(screen_name=screen_name,
                                       count=COUNT_MAX,
                                       tweet_mode='extended',
                                       include_rts=INCLUDE_RETWEETS)

    print(f"{len(new_tweets)} tweets scraped")

    return new_tweets


def get_oldest_tweets(screen_name):
    api = tweepy_api_init()
    all_tweets = []
    new_tweets = ['ok']  # to initialize : len(new_tweets) should be > 0

    oldest_id = get_oldest_id(screen_name)

    # continue the procedure gets new tweets
    while len(new_tweets) > 0:

        try:

            print(f"Oldest tweet {oldest_id} by {screen_name}")

            new_tweets = api.user_timeline(screen_name=screen_name,
                                           count=COUNT_MAX,
                                           tweet_mode='extended',
                                           include_rts=INCLUDE_RETWEETS,
                                           max_id=oldest_id)  # IMPORTANT

        except tweepy.TweepError as error:

            max_tweets_limit_notice(screen_name, error)

            print(f"Oldest tweet {oldest_id} by {screen_name}")

            # we try again after 15 minutes
            new_tweets = api.user_timeline(screen_name=screen_name,
                                           count=COUNT_MAX,
                                           tweet_mode='extended',
                                           include_rts=INCLUDE_RETWEETS,
                                           max_id=oldest_id)  # IMPORTANT
            continue

        all_tweets.extend(new_tweets)

        # update the id of the oldest tweet less one
        # see : https://gist.github.com/seankross/9338551
        oldest_id = all_tweets[-1].id - 1

        print(f"{len(all_tweets)} tweets scraped")
        time.sleep(5)

    return all_tweets


def get_newest_tweets(screen_name):
    api = tweepy_api_init()
    all_tweets = []
    new_tweets = ['ok']  # to initialize : len(new_tweets) should be > 0

    newest_id = get_newest_id(screen_name)

    # continue the procedure gets new tweets
    while len(new_tweets) > 0:

        try:

            print(f"Newest tweet {newest_id} by {screen_name}")

            new_tweets = api.user_timeline(screen_name=screen_name,
                                           count=COUNT_MAX,
                                           tweet_mode='extended',
                                           include_rts=INCLUDE_RETWEETS,
                                           since_id=newest_id)  # IMPORTANT

        except tweepy.TweepError as error:

            max_tweets_limit_notice(screen_name, error)

            print(f"Newest tweet {newest_id} by {screen_name}")

            # we try again after 15 minutes
            new_tweets = api.user_timeline(screen_name=screen_name,
                                           count=COUNT_MAX,
                                           tweet_mode='extended',
                                           include_rts=INCLUDE_RETWEETS,
                                           since_id=newest_id)  # IMPORTANT
            continue

        all_tweets.extend(new_tweets)

        newest_id = all_tweets[-1].id

        print(f"{len(all_tweets)} tweets scraped")
        time.sleep(5)

    return all_tweets


def insert_tweets_to_mongo(tweepy_tweets, screen_name):
    for tweet in tweepy_tweets:
        tweet_json = tweet._json
        date = parser.parse(tweet_json['created_at'])
        tweet_json['date_iso'] = date.isoformat().split('T')[0]
        tweet_json['screen_name'] = screen_name
        tweet_json['scraped_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        try:
            tweets_db.insert_one(tweet_json)
        except pymongo.errors.DuplicateKeyError:
            print("Tweet n°{} already in the database".format(tweet.id))
            continue


def get_tweets(accounts):
    for account in accounts:

        index, screen_name, checked_date = account

        if len(checked_date) == 0:  # no date in google sheet -> insert mode

            # get initial tweets
            new_tweepy_tweets = get_initial_tweets(screen_name)
            insert_tweets_to_mongo(new_tweepy_tweets, screen_name)

            # get oldest tweets
            old_tweepy_tweets = get_oldest_tweets(screen_name)
            insert_tweets_to_mongo(old_tweepy_tweets, screen_name)

            # update control sheet
            sheet.update_cell(index, GSHEET_ACCOUNTS_COLUMN_NB, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        else:  # date exists in google sheet -> update mode

            # get newest tweets
            new_tweepy_tweets = get_newest_tweets(screen_name)
            insert_tweets_to_mongo(new_tweepy_tweets, screen_name)

            # update control sheet
            sheet.update_cell(index, GSHEET_ACCOUNTS_COLUMN_NB, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


# Initialize MongoDb database
client = pymongo.MongoClient()
db = client[MONGO_BD_NAME]
tweets_db = db.tweets

# Get Google sheet
sheet = get_sheet_object()

# Get tweets
get_tweets(get_accounts_from_google_sheets())