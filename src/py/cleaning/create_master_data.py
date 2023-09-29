# -*- coding: utf -*-
"""
This script writes out tables for Analysis in R.
"""
import os
import pandas as pd


class RedditCSVComments:
    def __init__(self, raw_data):
        self.raw_data = raw_data
        self.default_author_agg = {
            "sub_id": ["nunique"],
            "score": ["sum", "count"],
            "created_utc": ["min", "max"]
            }


class QAComments(RedditCSVComments):
    def __init__(self, raw_data):
        super().__init__(raw_data)
        self.log = []

    def init_count(self):
        raw_data = self.raw_data
        return raw_data.count()[0]

    def n_unique_authors(self, cleaned=True):
        if cleaned:
            data = self.cleaned_data
        else:
            data = self.raw_data
        return data['author'].nunique()

    def post_count(self):
        cleaned_data = self.cleaned_data
        return cleaned_data.count()[0]


class CleanRedditCSVComments(RedditCSVComments):
    def __init__(self, raw_data):
        super().__init__(raw_data)


    def remove_uninformative_comments(self, comment_exclusion=['[deleted]', '[removed]']):
        raw_data = self.raw_data
        cleaned_data = raw_data[~raw_data['body'].isin(comment_exclusion)]
        self.cleaned_data = cleaned_data
        return cleaned_data

# home_directory = '/Users/ameliachu/Google Drive/Spring 2021/Text as Data/final_project'

home_directory = "/Volumes/GoogleDrive/My Drive/Spring 2021/Text as Data/final_project/"
data_directory = "data/reddit" # should never change

# Should be custom or inferred by date
manifest_fname = "curated_submission_manifest_gme_04-15-2021.csv"

manifest_path = f"{home_directory}/{data_directory}/{manifest_fname}"
manifest = pd.read_csv(manifest_path)

# Determining what we have collected thus far
gme_raw_data_directory = f"{home_directory}/{data_directory}/gme/"
gme_fnames = [f for f in os.listdir(gme_raw_data_directory) if os.path.isfile(os.path.join(gme_raw_data_directory, f))]

existing_sub_ids = [fname.split('.')[0].split('_')[-1] for fname in gme_fnames if fname.startswith('comments_gme_')]

existing_sub_ids = [i for i in existing_sub_ids if i not in ['m16emz']]
clean_log = []

for sub_id in existing_sub_ids:
    gme_raw_data_path = f"{gme_raw_data_directory}comments_gme_{sub_id}.csv"
    gme_raw_data = pd.read_csv(gme_raw_data_path, index_col=0)

    comments = RedditCSVComments(gme_raw_data)
    init_count = QAComments.init_count(comments)
    cleaned_gme_data = CleanRedditCSVComments.remove_uninformative_comments(comments)
    unique_authors = QAComments.n_unique_authors(comments, cleaned=True)
    post_process_count = QAComments.post_count(comments)

    new_entry = (sub_id, init_count, post_process_count, unique_authors)
    cleaned_gme_data_path =  f"{gme_raw_data_directory}cleaned_comments_gme_{sub_id}.csv"
    cleaned_gme_data.to_csv(cleaned_gme_data_path, index=True)
    clean_log.append(new_entry)

print(clean_log)

gme_master_data_list = []

for sub_id in existing_sub_ids:
    cleaned_gme_data_path = f"{gme_raw_data_directory}cleaned_comments_gme_{sub_id}.csv"
    gme_cleaned_data = pd.read_csv(cleaned_gme_data_path, index_col=0)
    gme_master_data_list.append(gme_cleaned_data)

gme_master_data = pd.concat(gme_master_data_list).reset_index(drop=True)

start_time = int(gme_master_data['created_utc'].min())
end_time = int(gme_master_data['created_utc'].max())

gme_master_data_path = f"{gme_raw_data_directory}gme_master_data_{start_time}_{end_time}.csv"
gme_master_data.to_csv(gme_master_data_path, index=True)
print(gme_master_data_path)
