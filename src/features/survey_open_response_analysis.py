import os
import pandas as pd
import numpy as np
import nltk
from nltk.corpus import stopwords
# nltk.download('stopwords')
# nltk.download('punkt')

from sklearn.feature_extraction.text import CountVectorizer

import datetime

from config import *


# Bag of words
# - bigrams, trigrams
# - num occurrences total
# - num occurrence as proportion of total words
# - avg num occurrences per response
# - num of responses which include the word/phrase
# num of responses that include the word/phrase, how many times is it included?

# punctuation: exclamation marks, question marks
# how many times do they occur
# how many times do they occur consecutively

# specific words: 'mayor', 'Mayor Hogsett', 'Joe Hogsett', 'police', 'black lives matter', 'all lives matter', 'crime'

# profanity, slurs

survey_data_cleaned_loc = os.path.join(os.path.join(INTERIM_DATA_DIR, 'survey'), 'survey_data_cleaned.csv')
survey_data_cleaned = pd.read_csv(survey_data_cleaned_loc)

survey_data_cleaned = survey_data_cleaned.rename(columns={
    "What community-level resources and supports exist to keep Indianapolis community members safe? What is working?": "Q1 - What exists/works",
    "What conditions must be present for the community to be safe and equitable?": "Q2 - Conditions for safety/equity",
    "What changes must occur to make an equitable and accountable public safety system?": "Q3 - Changes for equity/accountability",
    "What else should be considered (perspectives, knowledge, data, resources) as we begin this work?": "Q4 - Addt'l considerations"
})

# Q1: What community-level resources and supports exist to keep Indianapolis community members safe? What is working?
q1_what_exists_working_df = survey_data_cleaned[["Respondent ID", "Q1 - What exists/works"]].dropna(subset=["Q1 - What exists/works"])

# Q2: What conditions must be present for the community to be safe and equitable?
q2_safe_equitable_df = survey_data_cleaned[["Respondent ID", "Q2 - Conditions for safety/equity"]].dropna(subset=["Q2 - Conditions for safety/equity"])

# Q3: What changes must occur to make an equitable and accountable public safety system?
q3_changes_df = survey_data_cleaned[["Respondent ID", "Q3 - Changes for equity/accountability"]].dropna(subset=["Q3 - Changes for equity/accountability"])

# Q4: What else should be considered (perspectives, knowledge, data, resources) as we begin this work?
q4_addtl_considerations_df = survey_data_cleaned[["Respondent ID", "Q4 - Addt'l considerations"]].dropna(subset=["Q4 - Addt'l considerations"])


def bag_of_words(df, col, csv_filename):
    # TODO: Tokenize

    count = CountVectorizer()
    bag_of_words = count.fit_transform(df[col])
    bag_of_words.toarray()
    feature_names = count.get_feature_names()
    bag_of_words_df = pd.DataFrame(bag_of_words.toarray(), columns=feature_names)

    # Remove stopwords
    stopwords_list = stopwords.words('english')

    non_stopwords_list = list(set(bag_of_words_df.columns) - set(stopwords_list))
    # TODO: Print/export stopwords, words excluded from survey columns

    bag_of_words_no_stopwords_df = bag_of_words_df[non_stopwords_list]

    # print(bag_of_words_no_stopwords_df)
    bag_of_words_df_sums = bag_of_words_no_stopwords_df.sum(axis=0).reset_index().rename(columns={"index": "word", 0: "count"})
    bag_of_words_df_sums.sort_values(by="count", ascending=False, inplace=True)
    bag_of_words_df_sums["pctile"] = bag_of_words_df_sums["count"].rank(method="average", pct=True, ascending=True)
    print(bag_of_words_df_sums)

    bag_of_words_csv_loc = os.path.join(os.path.join(INTERIM_DATA_DIR, "survey"), f"bag_of_words_survey_{csv_filename}.csv")
    bag_of_words_df_sums.to_csv(bag_of_words_csv_loc, index=False)

    return bag_of_words_no_stopwords_df, bag_of_words_df_sums


def main():
    q1_bag_of_words_sparse_df, q1_bag_of_words_sums_df = bag_of_words(q1_what_exists_working_df, "Q1 - What exists/works", "Q1")



main()


# from nltk.tokenize import word_tokenize
# # q1_what_exists_working_df["tokens"] = q1_what_exists_working_df["What community-level resources and supports exist to keep Indianapolis community members safe? What is working?"].apply(word_tokenize)
#
# from collections import Counter
# # bag_of_words_series = pd.Series([word for sentence in q1_what_exists_working_df["Q1 - What exists/works"].flatten() for word in sentence.split()]).value_counts()
# # bag_of_words_df = bag_of_words_series.to_frame(name="count")
# # bag_of_words_df = bag_of_words_df.sort_values(by="count")
#
# q1_what_exists_working_df['BoW'] = q1_what_exists_working_df['Q1 - What exists/works'].str.split().apply(Counter)
# # q1_what_exists_working_df = q1_what_exists_working_df.sort_values(by="BoW")
# print(q1_what_exists_working_df)
