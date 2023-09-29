# -*- coding: utf -*-
"""
This script retreives the GME stock prices
for the past day (15-min intervals).

$ python3 get_daily_prices.py CLIENT_ID CLIENT_SECRET USER PW
/usr/bin/sh /home/petiteai519/run_job.sh
On Google VM:

$ mkdir data/alpha_vantage
$ mkdir jobs
$ mkdir jobs/logs
$ sudo apt install python3-pip
$ sudo crontab -e
* * * * * /usr/bin/sh /home/petiteai519/run_job.sh
"""
import datetime as dt
import pandas as pd
import praw
from praw.models import Redditor

reddit.config.ratelimit_seconds = 60

target_subreddit = 'wallstreetbets'

queries = {
    'daily': '"Daily Discussion Thread"',
    'weekend': '"Weekend Discussion Thread"',
    'gme': '"GME" and "Megathread"'}


submission_ids = []

def getSubmissionManifest(query, period='week'):
    subreddit = reddit.subreddit(target_subreddit)
    wsb_results = subreddit.search(query, time_filter='year')
    sub_list = list(map(lambda s: (s.created_utc, s.id, s.title), wsb_results))
    submission_ids = [row[1] for row in sub_list]
    submission = pd.DataFrame(sub_list, columns=['sub_created_utc','sub_id', 'sub_title'])
    return submission, submission_ids

submission = getSubmissionManifest(query, 'week')
submission.to_csv(f'submission_manifest_{today_date}.csv', index=False)

comment_df = pd.DataFrame(columns=['sub_id', 'body', 'score', 'author','created_utc'])
for sub_id in submission_ids:
    print(f'Working on submission: {sub_id}')
    comment_list = []
    submission = reddit.submission(id=sub_id)
    num_comments = submission.num_comments
    print(f'Total Comments: {num_comments:,}')

    submission.comments.replace_more(limit=None)
    for comment in submission.comments.list():
        comment_list.append([sub_id, comment.body, comment.score, comment.author, comment.created_utc])
#        time.sleep(5)
    comments = pd.DataFrame(comment_list, columns=['sub_id','body','score','author', 'created_utc'])
    comments.to_csv(f'comments_{sub_id}.csv', index=True)
