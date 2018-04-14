#!/usr/bin/env python3
# stage 2: augmenting each incident with additional fields, again using scraping

import asyncio
import logging as log
import numpy as np
import pandas as pd
import sys

from aiohttp.client_exceptions import ClientResponseError
from argparse import ArgumentParser

from log_utils import log_first_call
from stage2_extractor import NIL_FIELDS
from stage2_session import Stage2Session

SCHEMA = {
    'congressional_district': np.float64,
    'state_house_district': np.float64,
    'state_senate_district': np.float64,
    'n_guns_involved': np.float64,
}

def parse_args():
    targets_specific_month = False
    if len(sys.argv) > 1:
        parts = sys.argv[1].split('-')
        if len(parts) == 2: # e.g. '02-2014'
            targets_specific_month = True
            del sys.argv[1]

    parser = ArgumentParser()
    if not targets_specific_month:
        parser.add_argument(
            'input_fname',
            metavar='INPUT',
            help="path to input file",
        )
        parser.add_argument(
            'output_fname',
            metavar='OUTPUT',
            help="path to output file. " \
                 "if --amend is specified, this is interpreted as a suffix and output is written to the path (INPUT + OUTPUT)."
        )

    parser.add_argument(
        '-a', '--amend',
        help="amend existing stage2 file by populating missing values",
        action='store_true',
        dest='amend',
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
        default=20,
    )

    args = parser.parse_args()
    if targets_specific_month:
        month, year = map(int, parts)
        args.input_fname = 'stage1.{:02d}.{:04d}.csv'.format(month, year)
        args.output_fname = 'stage2.{:02d}.{:04d}.csv'.format(month, year)
    return args

def load_input(args):
    log_first_call()
    return pd.read_csv(args.input_fname,
                       dtype=SCHEMA,
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

async def add_fields_from_incident_url(df, args, predicate=None):
    log_first_call()
    def field_name(lst):
        assert len(set([field.name for field in lst])) == 1
        return lst[0].name

    def field_values(lst):
        return [field.value for field in lst]

    subset = df if predicate is None else df.loc[predicate]
    if len(subset) == 0:
        # No work to do
        return df

    async with Stage2Session(limit_per_host=args.conn_limit) as session:
        # list of coros of tuples of Fields
        tasks = subset.apply(session.get_fields_from_incident_url, axis=1)
        # list of (tuples of Fields) and (exceptions)
        fields = await asyncio.gather(*tasks, return_exceptions=True)

    # Temporarily suppress Pandas' SettingWithCopyWarning
    pd.options.mode.chained_assignment = None
    try:
        incident_url_fields_missing = [isinstance(x, Exception) for x in fields]
        subset['incident_url_fields_missing'] = incident_url_fields_missing
        
        not_found = [isinstance(x, ClientResponseError) and x.code == 404 for x in fields]

        # list of tuples of Fields
        fields = [NIL_FIELDS if isinstance(x, Exception) else x for x in fields]

        # tuple of lists of Fields, where each list's Fields should have the same name
        # if the extractor did its job correctly
        fields = zip(*fields)
        fields = [(field_name(lst), field_values(lst)) for lst in fields]

        for field_name, field_values in fields:
            assert subset.shape[0] == len(field_values)
            subset[field_name] = field_values

        subset = subset.astype(SCHEMA)
    finally:
        pd.options.mode.chained_assignment = 'warn'

    if predicate is not None:
        df.loc[subset.index] = subset
        df.drop(index=subset.index[not_found], inplace=True)

    return df

async def main():
    args = parse_args()
    log.basicConfig(level=args.log_level)

    df = load_input(args)

    if args.amend:
        output_fname = args.input_fname + args.output_fname
        df = await add_fields_from_incident_url(df, args, predicate=df['incident_url_fields_missing'])
    else:
        output_fname = args.output_fname
        df = add_incident_id(df)
        df = await add_fields_from_incident_url(df, args)

    df.to_csv(output_fname,
              index=False,
              float_format='%g',
              encoding='utf-8')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
