#!/usr/bin/env python3
# stage 3: sorting and merging data

import numpy as np
import pandas as pd

from glob import glob

STAGE2_GLOB = 'stage2.*.csv'

SCHEMA = {
    'congressional_district': np.float64,
    'state_house_district': np.float64,
    'state_senate_district': np.float64,
    'n_guns_involved': np.float64,
}

def load_csv(csv_fname):
    return pd.read_csv(csv_fname,
                       dtype=SCHEMA,
                       parse_dates=['date'],
                       encoding='utf-8')

def inner_sort(dfs):
    for df in dfs:
        assert all(~df['date'].isna())
        df.sort_values('date', inplace=True)

def outer_sort(dfs):
    # If the first incident in one file took place earlier than the first incident in another,
    # we assume all incidents in the former took place earlier than all incidents in the latter.
    dfs.sort(key=lambda df: df.loc[0].date)

def main():
    # Sort the dataframes by ascending date, then sort by ascending date *within* each dataframe,
    # then merge into 1 giant CSV.
    dfs = [load_csv(fname) for fname in glob(STAGE2_GLOB)]
    inner_sort(dfs)
    outer_sort(dfs)

    giant_df = pd.concat(dfs, ignore_index=True)
    giant_df.to_csv('stage3.csv',
                    index=False,
                    float_format='%g',
                    encoding='utf-8')

if __name__ == '__main__':
    main()
