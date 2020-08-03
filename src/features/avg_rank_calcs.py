import pandas as pd
import os
import sys

import statistics

sys.path.append("../")

from config import *

# TODO: Modularize the below into function(s)
zip_data_loc = os.path.join(INTERIM_DATA_DIR, "zip_level_metrics.csv")
zip_df = pd.read_csv(zip_data_loc)

PRIMARY_KEY_COLS = ['Zip Code']
raw_metric_cols = [
    col for col in zip_df.columns \
    if col not in PRIMARY_KEY_COLS \
        and 'Rank' not in col
]


# TODO: create Metric class that has all of these mappings baked into the class.
HEALTH_METRICS = [
    'Food Access',
    'Binge drinking',
    'Diabetes',
    'Frequent mental distress',
    'Frequent physical distress',
    'Obesity',
    'Physical inactivity',
    'Preventive services',
    'Smoking',
    'Medicaid Claims Per Capita',
    'Medicaid Recipients Per Capita',
    'Medicaid Dollar Amt. Per Capita',
    'ER Claims Per Capita',
    'ER Recipients Per Capita',
    'ER Dollar Amt. Per Capita',
    'Mental Health Claims Per Capita',
    'Mental Health ER Claims Per Capita',
    'Mental Health Patients Per Capita',
    'Mental Health Dollar Amt. Per Capita',
    'Life expectancy',
    'Limited access to healthy foods',
    'Uninsured'
]

CRIME_METRICS = [
    'All Arrests Per Capita',
    'Drug Arrests Per Capita',
    'Violent Arrests Per Capita',
    'Theft/Trespassing Arrests Per Capita',
    'Weapon Arrests Per Capita',
    'UCR Index Crime Per Capita',
    'UCR Homicide Per Capita',
    'UCR Rape Per Capita',
    'UCR Agg. Assault Per Capita',
    'UCR Robbery Per Capita',
    'UCR Burglary Per Capita'
]

HOUSING_ENVIRONMENT_METRICS = [
    'Eviction Rate',
    '% Renter Occupied',
    'Lead exposure risk index',
    'Park access',
    'Median Household Income',
    'Income Inequality',
    'Poverty Rate',
    'Racial/ethnic diversity',
    'Unemployment',
    'Children in Poverty',
]

EDUCATION_METRICS = [
    'Access to Quality PreK',
    'Quality Schools',
    'High School Graduation Rate'
]

health_df = zip_df[[*PRIMARY_KEY_COLS, *HEALTH_METRICS]]
crime_df = zip_df[[*PRIMARY_KEY_COLS, *CRIME_METRICS]]
housing_env_df = zip_df[[*PRIMARY_KEY_COLS, *HOUSING_ENVIRONMENT_METRICS]]
educ_df = zip_df[[*PRIMARY_KEY_COLS, *EDUCATION_METRICS]]

# educ_df.describe()
# housing_env_df.describe()
# health_df.describe()


def clean_zip_codes(df):
    all_indiana_zip_codes_loc = os.path.join(RAW_DATA_DIR, "Indiana Zip Codes - IN Zip Codes.csv")
    all_indiana_zip_codes = pd.read_csv(all_indiana_zip_codes_loc)

    indiana_zip_code_list = all_indiana_zip_codes['Indiana Zip Codes'].unique().tolist()

    df['Zip Code'] = df['Zip Code'].apply(str)

    df.loc[~df['Zip Code'].isin(indiana_zip_code_list), 'Zip Code'] = 'Other'

    return df


def avg_rank_pctile(df, metric_category):

    # TODO: Drop records (zip codes) which are missing ALL (or some?) metrics

    for c in df.columns:
        if c != 'Zip Code':
            df[f"{c}_dense_rank"] = df[c].rank(method="dense", na_option="bottom")  # TODO: What direction (asc/desc) should each metric be ranked? E.g. are values "better" if higher or lower
            df[f"{c}_dense_rank_pctile"] = df[c].rank(method="dense", pct=True, na_option="bottom")
            df[f"{c}_avg_rank"] = df[c].rank(method="average", na_option="bottom")
            df[f"{c}_avg_rank_pctile"] = df[c].rank(method="average", pct=True, na_option="bottom")

    def avg_vals_row(row):
        dense_rank_vals = [row[c] for c in df.columns if c.endswith('_dense_rank')]
        dense_rank_pctile_vals = [row[c] for c in df.columns if c.endswith('_dense_rank_pctile')]
        avg_rank_vals = [row[c] for c in df.columns if c.endswith('_avg_rank')]
        avg_rank_pctile_vals = [row[c] for c in df.columns if c.endswith('_avg_rank_pctile')]

        return statistics.mean(dense_rank_vals), statistics.mean(dense_rank_pctile_vals), \
               statistics.mean(avg_rank_vals), statistics.mean(avg_rank_pctile_vals)

    df[f'overall_avg_dense_rank_{metric_category}'], df[f'overall_avg_dense_rank_pctile_{metric_category}'], \
    df[f'overall_avg_avg_rank_{metric_category}'], df[f'overall_avg_avg_rank_pctile_{metric_category}'] = zip(*df.apply(lambda row: avg_vals_row(row), axis=1))

    # TODO: Re-merge (outer join) ranked records with all original zip codes (before dropping those missing all metrics)
    # TODO: Fill unranked zip codes with NaN

    return df


def re_rank_overall_avgs(df, metric_category):
    df[f"overall_avg_dense_rank_RANK_{metric_category}"] = df[f'overall_avg_dense_rank_{metric_category}'].rank(method="dense", na_option="bottom")
    df[f"overall_avg_dense_rank_pctile_RANK_{metric_category}"] = df[f'overall_avg_dense_rank_pctile_{metric_category}'].rank(method="dense", na_option="bottom")
    df[f"overall_avg_avg_rank_RANK_{metric_category}"] = df[f'overall_avg_avg_rank_{metric_category}'].rank(method="dense", na_option="bottom")
    df[f"overall_avg_avg_rank_pctile_RANK_{metric_category}"] = df[f'overall_avg_avg_rank_pctile_{metric_category}'].rank(method="dense", na_option="bottom")

    return df


def main():
    health_df_cleaned = clean_zip_codes(health_df)
    crime_df_cleaned = clean_zip_codes(crime_df)
    housing_env_df_cleaned = clean_zip_codes(housing_env_df)
    educ_df_cleaned = clean_zip_codes(educ_df)

    health_df_cleaned_ranked = avg_rank_pctile(health_df_cleaned, 'health')
    crime_df_cleaned_ranked = avg_rank_pctile(crime_df_cleaned, 'crime')
    housing_env_df_cleaned_ranked = avg_rank_pctile(housing_env_df_cleaned, 'housing_env')
    educ_df_cleaned_ranked = avg_rank_pctile(educ_df_cleaned, 'edu')

    health_df_cleaned_re_ranked = re_rank_overall_avgs(health_df_cleaned_ranked, 'health')
    crime_df_cleaned_re_ranked = re_rank_overall_avgs(crime_df_cleaned_ranked, 'crime')
    housing_env_df_cleaned_re_ranked = re_rank_overall_avgs(housing_env_df_cleaned_ranked, 'housing_env')
    educ_df_cleaned_re_ranked = re_rank_overall_avgs(educ_df_cleaned_ranked, 'edu')

    # health_df_cleaned_re_ranked.to_csv(os.path.join(PROCESSED_DATA_DIR, 'health_df_cleaned_ranked.csv'), index=False)
    # crime_df_cleaned_re_ranked.to_csv(os.path.join(PROCESSED_DATA_DIR, 'crime_df_cleaned_ranked.csv'), index=False)
    # housing_env_df_cleaned_re_ranked.to_csv(os.path.join(PROCESSED_DATA_DIR, 'housing_env_df_cleaned_ranked.csv'), index=False)
    # educ_df_cleaned_re_ranked.to_csv(os.path.join(PROCESSED_DATA_DIR, 'educ_df_cleaned_ranked.csv'), index=False)

    # dfs = [health_df_cleaned_re_ranked, crime_df_cleaned_re_ranked, housing_env_df_cleaned_re_ranked, educ_df_cleaned_re_ranked]
    # final_joined_df = pd.concat(dfs, axis=1, join='outer')

    merge1 = pd.merge(left=health_df_cleaned_re_ranked, right=crime_df_cleaned_re_ranked, how='outer', on='Zip Code')
    merge2 = pd.merge(left=housing_env_df_cleaned_re_ranked, right=educ_df_cleaned_re_ranked, how='outer', on='Zip Code')

    final_joined_df = pd.merge(left=merge1, right=merge2, how='outer', on='Zip Code')
    final_joined_df.to_csv(os.path.join(PROCESSED_DATA_DIR, 'all_ranked_metrics.csv'), index=False)


main()
