#!/usr/bin/env python3
# stage 2: augmenting each incident with additional fields, again using scraping

import asyncio
import logging as log
import pandas as pd

from argparse import ArgumentParser

from log_utils import log_first_call
from stage2_extractor import NIL_FIELDS
from stage2_session import Stage2Session

def parse_args():
    parser = ArgumentParser()

    parser.add_argument(
        'input_fname',
        metavar='INPUT',
        help="path to input file",
    )
    parser.add_argument(
        'output_fname',
        metavar='OUTPUT',
        help="path to output file"
    )

    parser.add_argument(
        '-d', '--debug',
        help="show debug information",
        action='store_const',
        dest='log_level',
        const=log.DEBUG,
        default=log.WARNING,
    )
    parser.add_argument(
        '-l', '--limit',
        metavar='NUM',
        help="limit the number of simultaneous connections aiohttp makes to gunviolencearchive.org",
        action='store',
        dest='conn_limit',
        type=int,
        default=0, # represents unlimited number of connections
    )

    return parser.parse_args()

def load_stage1(args):
    log_first_call()
    return pd.read_csv(args.input_fname,
                       parse_dates=['date'],
                       encoding='utf-8')

def add_incident_id(df):
    log_first_call()
    def extract_id(incident_url):
        PREFIX = 'http://www.gunviolencearchive.org/incident/'
        assert incident_url.startswith(PREFIX)
        return int(incident_url[len(PREFIX):])

    df.insert(0, 'incident_id', df['incident_url'].apply(extract_id))
    return df

async def add_fields_from_incident_url(df, args):
    log_first_call()
    def field_name(lst):
        assert len(set([field.name for field in lst])) == 1
        return lst[0].name

    def field_values(lst):
        return [field.value for field in lst]

    async with Stage2Session(limit_per_host=args.conn_limit) as session:
        # list of coros of tuples of Fields
        tasks = df.apply(session.get_fields_from_incident_url, axis=1)
        # list of (tuples of Fields) and (exceptions)
        fields = await asyncio.gather(*tasks, return_exceptions=True)

    incident_url_fields_missing = [isinstance(x, Exception) for x in fields]
    df['incident_url_fields_missing'] = incident_url_fields_missing

    # list of tuples of Fields
    fields = [NIL_FIELDS if isinstance(x, Exception) else x for x in fields]

    # tuple of lists of Fields, where each list's Fields should have the same name
    # if the extractor did its job correctly
    fields = zip(*fields)
    fields = [(field_name(lst), field_values(lst)) for lst in fields]

    for field_name, field_values in fields:
        assert df.shape[0] == len(field_values)
        df[field_name] = field_values

    return df

async def main():
    args = parse_args()
    log.basicConfig(level=args.log_level)

    df = load_stage1(args)
    df = add_incident_id(df)
    df = await add_fields_from_incident_url(df, args)

    df.to_csv(args.output_fname,
              index=False,
              float_format='%g',
              encoding='utf-8')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
