import os
import glob
import pandas as pd
import numpy as np

import datetime

from fuzzywuzzy import fuzz

from config import ROOT_DIR

# download all CSVs from data/raw
# deduplicate?
# clean


def append_all_csv():
    raw_data_path = os.path.join(os.path.join(ROOT_DIR, "data"), "raw")
    os.chdir(raw_data_path)

    df_list = []
    for i in glob.glob('*.csv'):
        df = pd.read_csv(i)
        df_list.append(df)

    all_raw_df = pd.concat(df_list)

    all_raw_df_dedupe = all_raw_df.drop_duplicates()

    return all_raw_df_dedupe


def cast_datatypes(df):
    df = df.applymap(str)
    df['Start Date'] = pd.to_datetime(df['Start Date'])
    df['End Date'] = pd.to_datetime(df['End Date'])

    return df


def clean_gender_field(df):
    # Get fuzzy match score between 'male' and 'female':
    print(fuzz.token_set_ratio('male', 'female'))

    # TODO: Best way to use fuzzy matching in this case? Even use it at all? Try to capture spelling errors like "femake", "mail", etc
    # http://jonathansoma.com/lede/algorithms-2017/classes/fuzziness-matplotlib/fuzzing-matching-in-pandas-with-fuzzywuzzy/

    # First, convert everything to uppercase
    df['What is your gender?'] = df['What is your gender?'].str.upper()

    def fuzzy_match_gender(row):
        if row['What is your gender?'] == 'FEMALE':
            return 'FEMALE'
        elif row['What is your gender?'] == 'F':
            return 'FEMALE'
        elif row['What is your gender?'] == 'MALE':
            return 'MALE'
        elif row['What is your gender?'] == 'M':
            return 'MALE'
        # elif fuzz.token_set_ratio(row['What is your gender?'], "FEMALE") > 0.8:
        #     return 'FEMALE'
        # elif fuzz.token_set_ratio(row['What is your gender?'], "MALE") > 0.8:
        #     return 'MALE'
        else:
            return 'OTHER'

    df['What is your gender? (cleaned)'] = df.apply(lambda row: fuzzy_match_gender(row), axis=1)

    # Finally, re-convert to title case
    df['What is your gender?'] = df['What is your gender?'].str.title()
    df['What is your gender? (cleaned)'] = df['What is your gender? (cleaned)'].str.title()

    return df


def replace_with_other(col_name, df):
    """
    Replaces all non-null values in the specified column with "Other".

    :param col_name: (str) the name of the column in the dataframe you want to change.
    :param df: (pandas DataFrame)
    :return:
    """

    df.loc[df[col_name].notnull(), col_name] = 'Other'

    return df


def grouping_aggregation(df):

    # df_grouped = pd.DataFrame.groupby(by=[
    #     "What is your zip code?",
    #     "Racial or ethnic background: African-American/Black",
    #     "Racial or ethnic background: American Indian/Alaskan Native",
    #     "Racial or ethnic background: Asian",
    #     "Racial or ethnic background: Caucasian/White",
    #     "Racial or ethnic background: Hispanic/Latinx",
    #     "Racial or ethnic background: Native Hawaiian/Pacific Islander",
    #     "Racial or ethnic background: Other",
    #     "What is your gender?",
    #     "What is your age?"
    # ], as_index=False).apply(lambda x: pd.Series({
    #     'total_respondents': x['Respondent ID'].shape[0],
    #     'pct_homicide': x['Top 3 public safety problems: Homicide']
    # }))

    df_grouped = df.groupby(by=[
        "What is your zip code?",
        # "Racial or ethnic background: African-American/Black",
        # "Racial or ethnic background: American Indian/Alaskan Native",
        # "Racial or ethnic background: Asian",
        # "Racial or ethnic background: Caucasian/White",
        # "Racial or ethnic background: Hispanic/Latinx",
        # "Racial or ethnic background: Native Hawaiian/Pacific Islander",
        # "Racial or ethnic background: Other",
        "What is your gender? (cleaned)",
        "What is your age?"
    ], as_index=False).agg(
        total_respondents=('Respondent ID', np.size),
        # pct_homicide=("Top 3 public safety problems: Homicide", 'count'),
        # pct_gun_violence=("Top 3 public safety problems: Gun violence", 'count'),
        # pct_physical_assault=("Top 3 public safety problems: Physical assault", 'count'),
        # pct_gang_activity=("Top 3 public safety problems: Gang activity", 'count'),
        # pct_drug_sales=("Top 3 public safety problems: Drug sales", 'count'),
        # pct_drug_abuse=("Top 3 public safety problems: Drug abuse", 'count'),
        # pct_robbery=("Top 3 public safety problems: Robbery (e.g., mugging)", 'count'),
        # pct_sexual_assault=("Top 3 public safety problems: Sexual assault", 'count'),
        # pct_theft=("Top 3 public safety problems: Theft", 'count'),
        # pct_burglary_theft_auto=("Top 3 public safety problems: Burglary/theft (auto)", 'count'),
        # pct_burglary_residence=("Top 3 public safety problems: Burglary (residence)", 'count'),
        # pct_underage_drinking=("Top 3 public safety problems: Underage drinking", 'count'),
        # pct_domestic_violence=("Top 3 public safety problems: Domestic violence", 'count'),
        # pct_disorderly_conduct=("Top 3 public safety problems: Disorderly conduct/noise", 'count'),
        # pct_vandalism_graffiti=("Top 3 public safety problems: Vandalism/graffiti", 'count'),
        # pct_prostitution=("Top 3 public safety problems: Prostitution", 'count'),
        # pct_disorderly_youth=("Top 3 public safety problems: Disorderly youth", 'count'),
        # pct_homelessness_related_problems=("Top 3 public safety problems: Homelessness-related problems", 'count'),
        # pct_traffic_issues=("Top 3 public safety problems: Traffic issues", 'count'),
        # pct_lack_of_police_presence=("Top 3 public safety problems: Lack of police presence", 'count'),
        # pct_slow_police_response=("Top 3 public safety problems: Slow police response", 'count'),
        # pct_dont_want_to_answer=("Top 3 public safety problems: Don't want to answer", 'count'),
        # pct_other=("Top 3 public safety problems: Other", 'count'),
    )

    # TODO: More manual aggregation? Slicing & filtering to get percentages and ranks?

    return df_grouped


def main():
    all_df_raw = append_all_csv()
    all_df_raw_casted = cast_datatypes(all_df_raw)
    df_gender_cleaned = clean_gender_field(all_df_raw_casted)
    # print(df_gender_cleaned[['What is your gender?', 'What is your gender? (imputed)']].sample(n=25))
    all_df_raw_race_other = replace_with_other('Racial or ethnic background: Other', df_gender_cleaned)

    print(all_df_raw_race_other['What is your gender? (cleaned)'].unique().tolist())
    print(all_df_raw_race_other['Racial or ethnic background: Other'].unique().tolist())

    # TODO: Saved cleaned data to data/interim?
    # TODO: Make new file to do grouping/aggregations?

    df_grouped = grouping_aggregation(all_df_raw_race_other)

    utc_datetime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H_%M_%S')  # TODO: Better formatting
    processed_data_path = os.path.join(os.path.join(ROOT_DIR, "data"), "processed")
    os.chdir(processed_data_path)
    try:
        df_grouped.to_csv(f"survey_processed_{utc_datetime}.csv", index=False)
        print(f"Success! Saved grouped & aggregated survey data ({df_grouped.shape[1]} columns x {df_grouped.shape[0]} rows) as 'survey_raw_{utc_datetime}.csv'")
    except Exception as e:
        print(f"Error saving grouped & aggregated survey data as CSV: {e}")

    print(df_grouped)


main()
