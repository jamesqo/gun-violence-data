import asyncio
import math
import numpy as np
import sys
import traceback as tb

from aiohttp import ClientSession, TCPConnector
from aiohttp.hdrs import CONTENT_TYPE
from collections import namedtuple

from stage3_extractor import Stage3Extractor

Context = namedtuple('Context', ['address', 'city_or_county', 'state'])

def _compute_wait(wait_mean, wait_factor):
    log_wait_mean = math.log(wait_mean, wait_factor)
    fuzz = np.random.standard_normal(size=1)[0]
    return int(np.ceil(wait_factor ** (log_wait_mean + fuzz)))

class Stage3Session(object):
    def __init__(self, **kwargs):
        self._extractor = Stage3Extractor()
        self._conn_options = kwargs

    async def __aenter__(self):
        conn = TCPConnector(**self._conn_options)
        self._sess = await ClientSession(connector=conn).__aenter__()
        return self

    async def __aexit__(self, type, value, tb):
        await self._sess.__aexit__(type, value, tb)

    def _log_failed_request(self, url):
        print("ERROR! Failed GET request to {}".format(url), file=sys.stderr)

    def _log_retry(self, url, status, retry_wait):
        print("GET request to {} failed with status {}. Trying again in {}s...".format(url, status, retry_wait), file=sys.stderr)

    def _log_extraction_failed(self, url):
        print("ERROR! Extractor failed for the following webpage: {}".format(url), file=sys.stderr)

    # Note: retry_limit=0 means no limit.
    async def _get(self, url, retry_limit=0, retry_wait_mean=5, wait_factor=2):
        resp = await self._sess.get(url)
        status = resp.status
        if retry_limit == 1 or status < 400:
            return resp
        elif 400 <= status < 500:
            self._log_failed_request(url)
            return resp

        # Server error, try again.
        retry_wait = _compute_wait(retry_wait_mean, wait_factor)
        self._log_retry(url, resp.status, retry_wait)
        async with resp: # Dispose of the response immediately.
            pass
        await asyncio.sleep(retry_wait)

        assert retry_limit != 1
        new_retry_limit = 0 if retry_limit == 0 else retry_limit - 1
        new_retry_wait_mean = retry_wait_mean * wait_factor
        return await self._get(url, new_retry_limit, new_retry_wait_mean, wait_factor)

    async def _get_fields_from_incident_url(self, row):
        incident_url = row['incident_url']
        resp = await self._get(incident_url)
        async with resp:
            if resp.status >= 400:
                resp.raise_for_status()

            ctype = resp.headers.get(CONTENT_TYPE, '').lower()
            mimetype = ctype[:ctype.find(';')]
            if mimetype in ('text/htm', 'text/html'):
                text = await resp.text()
            else:
                raise NotImplementedError("Encountered unknown mime type {}".format(mimetype))

        ctx = Context(address=row['address'],
                      city_or_county=row['city_or_county'],
                      state=row['state'])
        try:
            return self._extractor.extract_fields(text, ctx)
        except:
            self._log_extraction_failed(incident_url)
            raise

    async def get_fields_from_incident_url(self, row):
        try:
            await self._get_fields_from_incident_url(row)
        except:
            # Passing return_exceptions=True to asyncio.gather() destroys the ability
            # to print them once they're caught, so do that manually here.
            tb.print_exc()
            raise
