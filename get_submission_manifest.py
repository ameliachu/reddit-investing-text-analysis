# -*- coding: utf -*-
"""
This script retreives the submission ids of
the WSB subreddit based on time period and keyword.
"""
import sys
import datetime as dt
import json
import pandas as pd
import praw

target_subreddit = 'wallstreetbets'
queries = {
    'daily': '"Daily Discussion Thread"',
    'weekend': '"Weekend Discussion Thread"',
    'gme': '"GME" and "Megathread"'}


def getSubmissionManifest(query, period='year'):
    # use 'week' for prod
    subreddit = reddit.subreddit(target_subreddit)
    wsb_results = subreddit.search(query, time_filter=period)
    sub_list = list(map(lambda s: (s.created_utc, s.id, s.title), wsb_results))
    submission_ids = [row[1] for row in sub_list]
    submission = pd.DataFrame(sub_list, columns=['sub_created_utc', 'sub_id', 'sub_title'])
    return submission, submission_ids

# Parsing Inputs
query_type = sys.argv[1]
if len(sys.argv) > 2:
    period = sys.argv[2]

if query_type == 'custom':
    query = sys.argv[3]
else:
    query = queries[query_type]

# main
with open('creds.json') as json_file:
    creds = json.load(json_file)

reddit = praw.Reddit(**creds)
reddit.config.ratelimit_seconds = 60
submission, submission_ids = getSubmissionManifest(query)
today_date = dt.datetime.today().strftime("%Y-%m-%d")
submission_path = f'data/reddit/manifests/submission_manifest_{today_date}.csv'
submission.to_csv(submission_path, index=False)
print(f'manifest written to: {submission_path}')
