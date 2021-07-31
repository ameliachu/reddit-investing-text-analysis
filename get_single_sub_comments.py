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
    if num_comments > 40000:
        if upper_bound:
            print(f"Too many. sigh... we'll come back to this: {sub_id}")
            comments = None
        else:
            print(f"Woah, that's a lot of comments...")
    if upper_bound:
        pass
    else:
        retries = 0
        while retries < 100:
            retries += 1
            try:
                submission.comments.replace_more(limit=None)
                break
            except Exception as e:
                print('Attempting to handle replace_more issue')
                print(e)
                time.sleep(1)

        for comment in submission.comments.list():
            metadata = [sub_id, comment.body, comment.score, comment.author, comment.created_utc]
            comment_list.append(metadata)
    #        time.sleep(5)
        comments = pd.DataFrame(comment_list, columns=comments_schema)
    return comments


with open('creds.json') as json_file:
    creds = json.load(json_file)

# query_type = sys.argv[1]
query_type = 'gme'
# manifest = get_manifest(query_type)
# submission_ids = manifest['sub_id'].to_list()
sub_id = sys.argv[1]
upper_bound = False

# for sub_id in submission_ids[36:]:
reddit = praw.Reddit(**creds)
print(f'Working on {sub_id}...')
comments = get_submission_comments(sub_id, reddit)
if comments is not None:
# Index required for Quanteda doc_id variable
    print(f'Writing out: {sub_id}')
    comments.to_csv(f'{manifest_directory}/comments_{query_type}_limited_{sub_id}.csv',
                    index=True)
