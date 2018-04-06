#!/usr/bin/env python3
# stage 3: augmenting each incident with additional fields, again using scraping

import asyncio
import logging as log
import pandas as pd

from argparse import ArgumentParser
from stage3_session import Stage3Session

STAGE2_OUTPUT = 'stage2.csv'

def parse_args():
    parser = ArgumentParser()
    parser.add_argument(
        '-d', '--debug',
        help="show debug information",
        action='store_const',
        dest='log_level',
        const=log.DEBUG,
        default=log.WARNING,
    )
    parser.add_argument(
        '-m', '--mock',
        help="read in mock csv file for easier debugging",
        action='store',
        dest='csv_fname',
        default=STAGE2_OUTPUT,
    )
    parser.add_argument(
        '--sequential',
        help="run http requests sequentially instead of asynchronously for deterministic behavior (-> easier debugging)",
        action='store_true',
        dest='sequential',
    )
    return parser.parse_args()

def load_stage2(args):
    return pd.read_csv(args.csv_fname,
                       parse_dates=['date'],
                       encoding='utf-8')

def add_incident_id(df):
    def extract_id(incident_url):
        PREFIX = 'http://www.gunviolencearchive.org/incident/'
        assert incident_url.startswith(PREFIX)
        return int(incident_url[len(PREFIX):])

    df['incident_id'] = df['incident_url'].apply(extract_id)
    return df

async def add_incident_url_fields(df, args):
    async with Stage3Session() as session:
        tasks = df['incident_url'].apply(session.get_fields)
        if args.sequential:
            # Note: This is suuuuuuper slow
            fields = [await task for task in tasks]
        else:
            fields = await asyncio.gather(*tasks)

    for field_name, field_values in zip(*fields):
        assert df.shape[0] == len(field_values)
        df[field_name] = field_values

    return df

async def main():
    args = parse_args()
    log.basicConfig(level=args.log_level)

    df = load_stage2(args)
    df = add_incident_id(df)
    df = await add_incident_url_fields(df, args)

    df.to_csv('stage3.csv',
              index=False,
              encoding='utf-8')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
