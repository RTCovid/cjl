from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

import os
import pickle
import datetime
import pandas as pd

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

SURVEY_SPREADSHEET_ID = '10HcqHVwEoqc5l0GybdI0O8WWd7-iDT9NbxFycnmPA3g'
SURVEY_RANGE_NAME = 'Sheet!A3:AT'


def pull_sheet_data():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SURVEY_SPREADSHEET_ID,
                                range=SURVEY_RANGE_NAME).execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')

    return values


def sheet_headers():
    headers = [
        "Respondent ID",
        "Collector ID",
        "Start Date",
        "End Date",
        "IP Address",
        "Email Address",
        "First Name",
        "Last Name",
        "Custom Data 1",
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
        "What community-level resources and supports exist to keep Indianapolis community members safe? What is working?",
        "What condtions must be present for the community to be safe and equitable?",
        "What changes must occur to make an equitable and accountable public safety system?",
        "What else should be considered (perspectives, knowledge, data, resources) as we begin this work?",
        "What is your zip code?",
        "What is your age?",
        "What is your gender?",
        "Racial or ethnic background: African-American/Black",
        "Racial or ethnic background: American Indian/Alaskan Native",
        "Racial or ethnic background: Asian",
        "Racial or ethnic background: Caucasian/White",
        "Racial or ethnic background: Hispanic/Latinx",
        "Racial or ethnic background: Native Hawaiian/Pacific Islander",
        "Racial or ethnic background: Other"
    ]

    return headers


def main():
    survey_data = pull_sheet_data()
    survey_df = pd.DataFrame(survey_data[0:], columns=sheet_headers())

    utc_datetime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    os.chdir("..")
    os.chdir("..")
    os.chdir('data/raw')
    survey_df.to_csv(f"survey_raw_{utc_datetime}.csv")


main()
