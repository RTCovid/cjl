import os
import glob
import pandas as pd

from config import ROOT_DIR

# download all CSVs from data/raw
# deduplicate?
# clean


def append_all_csv():
    raw_data_path = os.path.join(os.path.join(ROOT_DIR, "data"), "raw")
    os.chdir(raw_data_path)
    all_raw_df = pd.concat(glob.glob('*.csv'))

    return all_raw_df


all_raw_df = append_all_csv()
