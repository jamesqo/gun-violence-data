#!/usr/bin/env python3
# stage 3: augmenting each incident with additional fields, again using scraping

import asyncio
import pandas as pd

from stage3_session import Stage3Session

STAGE2_OUTPUT = 'stage2.csv'

def load_stage2():
    return pd.read_csv(STAGE2_OUTPUT,
                       parse_dates=['date'],
                       encoding='utf-8')

def add_incident_id(df):
    def extract_id(incident_url):
        PREFIX = 'http://www.gunviolencearchive.org/incident/'
        assert incident_url.startswith(PREFIX)
        return int(incident_url[len(PREFIX):])

    df['incident_id'] = df['incident_url'].apply(extract_id)
    return df

async def add_incident_url_fields(df):
    async with Stage3Session() as session:
        tasks = df['incident_url'].apply(session.get_fields)
        fields = await asyncio.gather(*tasks)
    for field_name, field_values in zip(*fields):
        assert df.shape[0] == len(field_values)
        df[field_name] = field_values
    return df

async def main():
    df = load_stage2()
    df = add_incident_id(df)
    df = await add_incident_url_fields(df)

    df.to_csv('stage3.csv',
              index=False,
              encoding='utf-8')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
