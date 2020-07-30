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

    # TODO: Cast these fields to datetime
    # df['Start Date'] = pd.to_datetime(df['Start Date'])
    # df['End Date'] = pd.to_datetime(df['End Date'])

    return df


def clean_gender_field(df):
    # Get fuzzy match score between 'male' and 'female':
    # print(fuzz.token_set_ratio('male', 'female'))

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

    df = df[[
        "What is your zip code?",
        "What is your gender? (cleaned)",
        "What is your age?",
        "Racial or ethnic background: African-American/Black",
        "Racial or ethnic background: American Indian/Alaskan Native",
        "Racial or ethnic background: Asian",
        "Racial or ethnic background: Caucasian/White",
        "Racial or ethnic background: Hispanic/Latinx",
        "Racial or ethnic background: Native Hawaiian/Pacific Islander",
        "Racial or ethnic background: Other",
        "Top 3 public safety problems: Homicide",
        "Top 3 public safety problems: Gun violence",
        "Top 3 public safety problems: Physical assault",
        "Top 3 public safety problems: Gang activity",
        "Top 3 public safety problems: Drug sales",
        "Top 3 public safety problems: Drug abuse",
        "Top 3 public safety problems: Robbery (e.g., mugging)",
        "Top 3 public safety problems: Sexual assault",
        "Top 3 public safety problems: Theft",
        "Top 3 public safety problems: Burglary/theft (auto)",
        "Top 3 public safety problems: Burglary (residence)",
        "Top 3 public safety problems: Underage drinking",
        "Top 3 public safety problems: Domestic violence",
        "Top 3 public safety problems: Disorderly conduct/noise",
        "Top 3 public safety problems: Vandalism/graffiti",
        "Top 3 public safety problems: Prostitution",
        "Top 3 public safety problems: Disorderly youth",
        "Top 3 public safety problems: Homelessness-related problems",
        "Top 3 public safety problems: Traffic issues",
        "Top 3 public safety problems: Lack of police presence",
        "Top 3 public safety problems: Slow police response",
        "Top 3 public safety problems: Don't want to answer",
        "Top 3 public safety problems: Other",
    ]]

    df_grouped = df.drop_duplicates(subset=[
            "What is your zip code?",
            "What is your gender? (cleaned)",
            "What is your age?",
            "Racial or ethnic background: African-American/Black",
            "Racial or ethnic background: American Indian/Alaskan Native",
            "Racial or ethnic background: Asian",
            "Racial or ethnic background: Caucasian/White",
            "Racial or ethnic background: Hispanic/Latinx",
            "Racial or ethnic background: Native Hawaiian/Pacific Islander",
            "Racial or ethnic background: Other",
    ])

    def aggregations(row):
        df_sliced = df.loc[
            (df['What is your zip code?'] == row['What is your zip code?']) &
            (df['Racial or ethnic background: African-American/Black'] == row['Racial or ethnic background: African-American/Black']) &
            (df['Racial or ethnic background: American Indian/Alaskan Native'] == row['Racial or ethnic background: American Indian/Alaskan Native']) &
            (df['Racial or ethnic background: Asian'] == row['Racial or ethnic background: Asian']) &
            (df['Racial or ethnic background: Caucasian/White'] == row['Racial or ethnic background: Caucasian/White']) &
            (df['Racial or ethnic background: Hispanic/Latinx'] == row['Racial or ethnic background: Hispanic/Latinx']) &
            (df['Racial or ethnic background: Native Hawaiian/Pacific Islander'] == row['Racial or ethnic background: Native Hawaiian/Pacific Islander']) &
            (df['Racial or ethnic background: Other'] == row['Racial or ethnic background: Other']) &
            (df['What is your gender? (cleaned)'] == row['What is your gender? (cleaned)']) &
            (df['What is your gender? (cleaned)'] == row['What is your gender? (cleaned)'])
        ]

        total_respondents = float(df_sliced.shape[0])

        pct_homicide = float(df_sliced[df_sliced["Top 3 public safety problems: Homicide"] == 'Homicide'].shape[0]) / total_respondents
        pct_gun_violence = float(df_sliced[df_sliced["Top 3 public safety problems: Gun violence"] == 'Gun violence'].shape[0]) / total_respondents
        pct_physical_assault = float(df_sliced[df_sliced["Top 3 public safety problems: Physical assault"] == 'Physical assault'].shape[0]) / total_respondents
        pct_gang_activity = float(df_sliced[df_sliced["Top 3 public safety problems: Gang activity"] == 'Gang activity'].shape[0]) / total_respondents
        pct_drug_sales = float(df_sliced[df_sliced["Top 3 public safety problems: Drug sales"] == 'Drug sales'].shape[0]) / total_respondents
        pct_drug_abuse = float(df_sliced[df_sliced["Top 3 public safety problems: Drug abuse"] == 'Drug abuse'].shape[0]) / total_respondents
        pct_robbery = float(df_sliced[df_sliced["Top 3 public safety problems: Robbery (e.g., mugging)"] == 'Robbery (e.g., mugging)'].shape[0]) / total_respondents
        pct_sexual_assault = float(df_sliced[df_sliced["Top 3 public safety problems: Sexual assault"] == 'Sexual assault'].shape[0]) / total_respondents
        pct_theft = float(df_sliced[df_sliced["Top 3 public safety problems: Theft"] == 'Theft'].shape[0]) / total_respondents
        pct_burglary_theft_auto = float(df_sliced[df_sliced["Top 3 public safety problems: Burglary/theft (auto)"] == 'Burglary/theft (auto)'].shape[0]) / total_respondents
        pct_burglary_residence = float(df_sliced[df_sliced["Top 3 public safety problems: Burglary (residence)"] == 'Burglary (residence)'].shape[0]) / total_respondents
        pct_underage_drinking = float(df_sliced[df_sliced["Top 3 public safety problems: Underage drinking"] == 'Underage drinking'].shape[0]) / total_respondents
        pct_domestic_violence = float(df_sliced[df_sliced["Top 3 public safety problems: Domestic violence"] == 'Domestic violence'].shape[0]) / total_respondents
        pct_disorderly_conduct = float(df_sliced[df_sliced["Top 3 public safety problems: Disorderly conduct/noise"] == 'Disorderly conduct/noise'].shape[0]) / total_respondents
        pct_vandalism_graffiti = float(df_sliced[df_sliced["Top 3 public safety problems: Vandalism/graffiti"] == 'Vandalism/graffiti'].shape[0]) / total_respondents
        pct_prostitution = float(df_sliced[df_sliced["Top 3 public safety problems: Prostitution"] == 'Prostitution'].shape[0]) / total_respondents
        pct_disorderly_youth = float(df_sliced[df_sliced["Top 3 public safety problems: Disorderly youth"] == 'Disorderly youth'].shape[0]) / total_respondents
        pct_homelessness_related_problems = float(df_sliced[df_sliced["Top 3 public safety problems: Homelessness-related problems"] == 'Homelessness-related problems'].shape[0]) / total_respondents
        pct_traffic_issues = float(df_sliced[df_sliced["Top 3 public safety problems: Traffic issues"] == 'Traffic issues'].shape[0]) / total_respondents
        pct_lack_of_police_presence = float(df_sliced[df_sliced["Top 3 public safety problems: Lack of police presence"] == 'Lack of police presence'].shape[0]) / total_respondents
        pct_slow_police_response = float(df_sliced[df_sliced["Top 3 public safety problems: Slow police response"] == 'Slow police response'].shape[0]) / total_respondents
        pct_dont_want_to_answer = float(df_sliced[df_sliced["Top 3 public safety problems: Don't want to answer"] == "Don't want to answer"].shape[0]) / total_respondents
        pct_other = float(df_sliced[df_sliced["Top 3 public safety problems: Other"] == 'Other'].shape[0]) / total_respondents

        return total_respondents, pct_homicide, pct_gun_violence, pct_physical_assault, pct_gang_activity, \
               pct_drug_sales, pct_drug_abuse, pct_robbery, pct_sexual_assault, pct_theft, pct_burglary_theft_auto, \
               pct_burglary_residence, pct_underage_drinking, pct_domestic_violence, pct_disorderly_conduct, \
               pct_vandalism_graffiti, pct_prostitution, pct_disorderly_youth, pct_homelessness_related_problems, \
               pct_traffic_issues, pct_lack_of_police_presence, pct_slow_police_response, pct_dont_want_to_answer, pct_other

    df_grouped['total_respondents'], df_grouped['pct_homicide'], df_grouped['pct_gun_violence'], \
    df_grouped['pct_physical_assault'], df_grouped['pct_gang_activity'], df_grouped['pct_drug_sales'], \
    df_grouped['pct_drug_abuse'], df_grouped['pct_robbery'], df_grouped['pct_sexual_assault'], df_grouped['pct_theft'], \
    df_grouped['pct_burglary_theft_auto'], df_grouped['pct_burglary_residence'], df_grouped['pct_underage_drinking'], \
    df_grouped['pct_domestic_violence'], df_grouped['pct_disorderly_conduct'], df_grouped['pct_vandalism_graffiti'], \
    df_grouped['pct_prostitution'], df_grouped['pct_disorderly_youth'], df_grouped['pct_homelessness_related_problems'], \
    df_grouped['pct_traffic_issues'], df_grouped['pct_lack_of_police_presence'], df_grouped['pct_slow_police_response'], \
    df_grouped['pct_dont_want_to_answer'], df_grouped['pct_other'] = zip(*df_grouped.apply(lambda row: aggregations(row), axis=1))

    df_grouped_subset_cols = df_grouped[[
        "What is your zip code?",
        "What is your gender? (cleaned)",
        "What is your age?",
        "Racial or ethnic background: African-American/Black",
        "Racial or ethnic background: American Indian/Alaskan Native",
        "Racial or ethnic background: Asian",
        "Racial or ethnic background: Caucasian/White",
        "Racial or ethnic background: Hispanic/Latinx",
        "Racial or ethnic background: Native Hawaiian/Pacific Islander",
        "Racial or ethnic background: Other",
        'total_respondents',
        'pct_homicide',
        'pct_gun_violence',
        'pct_physical_assault',
        'pct_gang_activity',
        'pct_drug_sales',
        'pct_drug_abuse',
        'pct_robbery',
        'pct_sexual_assault',
        'pct_theft',
        'pct_burglary_theft_auto',
        'pct_burglary_residence',
        'pct_underage_drinking',
        'pct_domestic_violence',
        'pct_disorderly_conduct',
        'pct_vandalism_graffiti',
        'pct_prostitution',
        'pct_disorderly_youth',
        'pct_homelessness_related_problems',
        'pct_traffic_issues',
        'pct_lack_of_police_presence',
        'pct_slow_police_response',
        'pct_dont_want_to_answer',
        'pct_other',
    ]]

    return df_grouped_subset_cols


def main():
    all_df_raw = append_all_csv()
    all_df_raw_casted = cast_datatypes(all_df_raw)
    df_gender_cleaned = clean_gender_field(all_df_raw_casted)
    # print(df_gender_cleaned[['What is your gender?', 'What is your gender? (imputed)']].sample(n=25))
    all_df_raw_race_other = replace_with_other('Racial or ethnic background: Other', df_gender_cleaned)
    all_df_raw_problems_other = replace_with_other('Top 3 public safety problems: Other', all_df_raw_race_other)

    # print(all_df_raw_race_other['What is your gender? (cleaned)'].unique().tolist())
    # print(all_df_raw_race_other['Racial or ethnic background: Other'].unique().tolist())

    # TODO: Saved cleaned data to data/interim?
    # TODO: Make new file to do grouping/aggregations?

    df_grouped = grouping_aggregation(all_df_raw_problems_other)

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
