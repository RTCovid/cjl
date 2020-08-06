import os
import glob
import pandas as pd
import numpy as np

import datetime

import string
from fuzzywuzzy import fuzz

from config import *

# download all CSVs from data/raw
# deduplicate?
# clean


def append_all_csv():
    raw_data_path = os.path.join(RAW_DATA_DIR, "survey")
    os.chdir(raw_data_path)

    df_list = []
    for i in glob.glob('*.csv'):
        df = pd.read_csv(i)
        df_list.append(df)

    all_raw_df = pd.concat(df_list)

    all_raw_df_dedupe = all_raw_df.drop_duplicates()

    return all_raw_df_dedupe


def fill_non_indiana_zips(df):
    all_indiana_zip_codes_loc = os.path.join(RAW_DATA_DIR, "Indiana Zip Codes - IN Zip Codes.csv")
    all_indiana_zip_codes = pd.read_csv(all_indiana_zip_codes_loc)

    all_indiana_zip_codes['Indiana Zip Codes'] = all_indiana_zip_codes['Indiana Zip Codes'].apply(str)
    indiana_zip_code_list = all_indiana_zip_codes['Indiana Zip Codes'].unique().tolist()
    indiana_zip_code_list += ['nan']

    # Cast Zip Code field as string, truncate to eliminate decimals
    df['Zip Code'] = df['Zip Code'].apply(str)
    df['Zip Code'] = df['Zip Code'].str[:5]  # TODO: Could also use regex, might be safer?

    # Replace non-Indiana zip codes with 'Other'
    df.loc[~df['Zip Code'].isin(indiana_zip_code_list), 'Zip Code'] = 'Other'

    # Replace 'nan' with np.nan
    df['Zip Code'] = df['Zip Code'].replace(to_replace='nan', value=np.nan)

    return df


def drop_under_18(df):
    df = df[df["Age"] != "Under 18"]

    return df


def fill_blanks(df, col, replace_val="Prefer not to answer."):
    df[col] = df[col].fillna(value=replace_val)

    return df


def cast_datatypes(df):
    df = df.applymap(str)

    # TODO: Cast these fields to datetime -- but doing so seems to interfere with the grouping & aggregation? Revisit this
    # df['Start Date'] = pd.to_datetime(df['Start Date'])
    # df['End Date'] = pd.to_datetime(df['End Date'])

    return df


def clean_gender_field(df):
    # Get fuzzy match score between 'male' and 'female':
    # print(fuzz.token_set_ratio('male', 'female'))

    # TODO: Best way to use fuzzy matching in this case? Even use it at all? Try to capture spelling errors like "femake", "mail", etc
    # http://jonathansoma.com/lede/algorithms-2017/classes/fuzziness-matplotlib/fuzzing-matching-in-pandas-with-fuzzywuzzy/

    # TODO: Spellchecker? https://pypi.org/project/pyspellchecker/

    def fuzzy_match_gender(row):
        # Convert string to uppercase
        string_upper = row['What is your gender?'].upper()

        # Remove punctuation/symbols
        string_upper_strip_punc = string_upper.translate(str.maketrans('', '', string.punctuation))

        # Manual inference/typo correction
        if string_upper_strip_punc in ['FEMALE', 'F', 'WOMAN', 'WOMEN', 'FEMALES', 'FEMAIL', 'FEMAILE', 'FEMAL', 'FEMAKE', 'SHE', 'HER', 'SHEHER', 'SHE/HER', 'SHE-HER', 'TRANS FEMALE', 'TRANSFEMALE', 'TRANS WOMAN', 'TRANSWOMAN', 'CISGENDER WOMAN', 'CIS GENDER WOMAN', 'CISGENDERED WOMAN', 'CIS GENDERED WOMAN', 'CISGENDER FEMALE', 'CIS GENDER FEMALE', 'CISGENDERED FEMALE', 'CIS GENDERED FEMALE',]:
            return 'Female'
        elif string_upper_strip_punc in ['MALE', 'M', 'MAN', 'MEN', 'MALES', 'MAIL', 'MAILE', 'MAL', 'MAKE', 'HE', 'HIM', 'HEHIM', 'HE/HIM', 'HE-HIM', 'TRANS MALE', 'TRANSMALE', 'TRANS MAN', 'TRANSMAN', 'CISGENDER MAN', 'CIS GENDER MAN', 'CISGENDERED MAN', 'CIS GENDERED MAN', 'CISGENDER MAN', 'CIS GENDER MAN', 'CISGENDERED MAN', 'CIS GENDERED MAN',]:
            return 'Male'
        elif string_upper_strip_punc in ['NON-BINARY', 'NON BINARY', 'NONBINARY', 'NONE BINARY']:
            return 'Non-Binary'
        # elif fuzz.token_set_ratio(row['What is your gender?'], "FEMALE") > 0.8:
        #     return 'FEMALE'
        # elif fuzz.token_set_ratio(row['What is your gender?'], "MALE") > 0.8:
        #     return 'MALE'
        else:
            return 'Other'

    df['What is your gender? (cleaned)'] = df.apply(lambda row: fuzzy_match_gender(row), axis=1)

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


def concat_race_ethnicity(df):
    df["Race/Ethnicity Concat"] = df[[
        "Racial or ethnic background: African-American/Black",
        "Racial or ethnic background: American Indian/Alaskan Native",
        "Racial or ethnic background: Asian",
        "Racial or ethnic background: Caucasian/White",
        "Racial or ethnic background: Hispanic/Latinx",
        "Racial or ethnic background: Native Hawaiian/Pacific Islander",
        "Racial or ethnic background: Other"
    ]].agg('|'.join, axis=1)

    df["Race/Ethnicity Concat"] = df["Race/Ethnicity Concat"].str.replace("nan", "")

    return df


def grouping_aggregation(df):
    df_grouped = df.groupby(by=["Zip Code", "Gender (Inferred)", "Age"], as_index=False)['Respondent ID'].count().drop(columns=['Respondent ID'])

    unique_race_ethnicity_df = pd.DataFrame({"Race/Ethnicity": ["African-American/Black",
                                                                "American Indian/Alaskan Native",
                                                                "Asian",
                                                                "Caucasian/White",
                                                                "Hispanic/Latinx",
                                                                "Native Hawaiian/Pacific Islander",
                                                                "Other",
                                                                "Prefer not to answer."]})

    # Cross join
    df_grouped = df_grouped.assign(key=1).merge(unique_race_ethnicity_df.assign(key=1), on='key').drop('key', 1)

    df_data = df[[
        "Zip Code",
        "Gender (Inferred)",
        "Age",
        "Race/Ethnicity Concat",
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

    def aggregations(row):
        df_sliced = df_data.loc[
            (df_data['Zip Code'] == row['Zip Code']) &
            (df_data["Race/Ethnicity Concat"].str.contains(row["Race/Ethnicity"])) &
            (df_data['Gender (Inferred)'] == row['Gender (Inferred)']) &
            (df_data['Age'] == row['Age'])
        ]

        total_respondents = float(df_sliced.shape[0])

        if total_respondents < 1:
            pct_homicide = 0
            pct_gun_violence = 0
            pct_physical_assault = 0
            pct_gang_activity = 0
            pct_drug_sales = 0
            pct_drug_abuse = 0
            pct_robbery = 0
            pct_sexual_assault = 0
            pct_theft = 0
            pct_burglary_theft_auto = 0
            pct_burglary_residence = 0
            pct_underage_drinking = 0
            pct_domestic_violence = 0
            pct_disorderly_conduct = 0
            pct_vandalism_graffiti = 0
            pct_prostitution = 0
            pct_disorderly_youth = 0
            pct_homelessness_related_problems = 0
            pct_traffic_issues = 0
            pct_lack_of_police_presence = 0
            pct_slow_police_response = 0
            pct_dont_want_to_answer = 0
            pct_other = 0

        else:
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
        "Zip Code",
        "Gender (Inferred)",
        "Age",
        "Race/Ethnicity",
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


def main(verbose=False):
    if verbose:
        print("Finding all *.csv in data/raw/survey, concatenating to 1 DF...")
    all_df_raw = append_all_csv()

    if verbose:
        print("Filling non-Indiana zip codes with 'Other'...")
    df_cleaned_zips = fill_non_indiana_zips(all_df_raw)

    if verbose:
        print("Dropping rows where Age == 'Under 18'...")
    df_drop_18 = drop_under_18(df_cleaned_zips)

    if verbose:
        print("Filling <blank> Age values with 'Prefer not to answer.'...")
    df_fill_age = fill_blanks(df_drop_18, "Age")

    if verbose:
        print("Filling <blank> Gender (Inferred) values with 'Prefer not to answer.'...")
    df_fill_gender = fill_blanks(df_fill_age, "Gender (Inferred)")

    if verbose:
        print("Casting columns to correct datatypes...")
    df_cast = cast_datatypes(df_fill_gender)

    if verbose:
        print("Filling other/nonstandard Race/Ethnicity values with 'Other'...")
    df_race_other = replace_with_other('Racial or ethnic background: Other', df_cast)

    if verbose:
        print("Filling other/nonstandard 'Other public safety problem' values with 'Other'...")
    df_problems_other = replace_with_other('Top 3 public safety problems: Other', df_race_other)

    df_problems_other["Gender (Inferred)"] = df_problems_other["Gender (Inferred)"].str.title()

    if verbose:
        print("Adding field of all concatenated race/ethnicity selections...")
    df_race_concat = concat_race_ethnicity(df_problems_other)

    if verbose:
        print("Grouping and aggregating...")
    df_grouped = grouping_aggregation(df_race_concat)

    utc_datetime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H_%M_%S')  # TODO: Better formatting
    processed_data_path = os.path.join(PROCESSED_DATA_DIR, 'survey')
    os.chdir(processed_data_path)

    if verbose:
        print("Saving processed survey data to data/processed/survey")
    try:
        df_grouped.to_csv(f"survey_processed_{utc_datetime}.csv", index=False)
        print(f"Success! Saved grouped & aggregated survey data ({df_grouped.shape[1]} columns x {df_grouped.shape[0]} rows) as 'survey_raw_{utc_datetime}.csv'")
    except Exception as e:
        print(f"Error saving grouped & aggregated survey data as CSV: {e}")

    print(df_grouped)


main(verbose=True)
