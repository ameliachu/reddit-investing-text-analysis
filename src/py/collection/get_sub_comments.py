# -*- coding: utf -*-
"""
This script writes out a csv file
of all the comments in a subreddit
submission based on submission id (sub_id).

$ nohup python3 get_sub_comments.py lm7n51 &
"""
import sys
import json
import praw
import pandas as pd

manifest_directory = "data/reddit"

file_list = {
    "weekend": "curated_submission_manifest_weekend_04-15-2021.csv",
    "daily": "curated_submission_manifest_daily_04-15-2021.csv",
    "gme": "curated_submission_manifest_gme_04-15-2021.csv"}


def get_manifest(query_type):

    """
    Reads in a manifest file of submission ids as a Pandas DataFrame.
    """
    manifest_path = f"{manifest_directory}/{file_list[query_type]}"
    manifest = pd.read_csv(manifest_path)
    return manifest


def get_submission_comments(sub_id, reddit):
    comments_schema = ['sub_id', 'body', 'score', 'author', 'created_utc']
    comment_list = []
    submission = reddit.submission(id=sub_id)
    num_comments = submission.num_comments
    print(f'Total Comments: {num_comments:,}')
    submission.comments.replace_more(limit=None)
    for comment in submission.comments.list():
        metadata = [sub_id, comment.body, comment.score, comment.author, comment.created_utc]
        comment_list.append(metadata)
    #        time.sleep(5)
    comments = pd.DataFrame(comment_list, columns=comments_schema)
    return comments


with open('creds.json') as json_file:
    creds = json.load(json_file)

query_type = sys.argv[1]
manifest = get_manifest(query_type)
submission_ids = manifest['sub_id'].to_list()

for sub_id in submission_ids:
    reddit = praw.Reddit(**creds)
    print(f'Working on {sub_id}...')
    comments = get_submission_comments(sub_id, reddit)
    # Index required for Quanteda doc_id variable
    comments.to_csv(f'{manifest_directory}/comments_{query_type}_{sub_id}.csv',
                    index=True)
