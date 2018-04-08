import asyncio
import math
import numpy as np
import platform
import sys
import traceback as tb

from aiohttp import ClientSession, TCPConnector
from aiohttp.client_exceptions import ClientOSError
from aiohttp.hdrs import CONTENT_TYPE
from collections import namedtuple

from log_utils import log_first_call
from stage2_extractor import Stage2Extractor

Context = namedtuple('Context', ['address', 'city_or_county', 'state'])

def _compute_wait(average_wait, rng_base):
    log_first_call()
    log_average_wait = math.log(average_wait, rng_base)
    fuzz = np.random.standard_normal(size=1)[0]
    return int(np.ceil(rng_base ** (log_average_wait + fuzz)))

class Stage2Session(object):
    def __init__(self, **kwargs):
        self._extractor = Stage2Extractor()
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
    async def _get(self, url, retry_limit=0, average_wait=10, rng_base=2):
        try:
            resp = await self._sess.get(url)
            status = resp.status
            if retry_limit == 1 or status < 400:
                return resp
            elif 400 <= status < 500:
                self._log_failed_request(url)
                return resp
        except asyncio.TimeoutError:
            if retry_limit == 1:
                raise
            resp, status = None, '<timed out>'
        except ClientOSError as exc:
            if platform.system() == 'Windows' and exc.errno == 10054:
                # WinError: An existing connection was forcibly closed by the remote host
                resp, status = None, '<conn closed>'
            else:
                raise

        # Server error, try again.
        if resp is not None:
            async with resp: # Dispose of the response immediately.
                pass
        wait = _compute_wait(average_wait, rng_base)
        self._log_retry(url, status, wait)
        await asyncio.sleep(wait)

        assert retry_limit != 1
        new_retry_limit = 0 if retry_limit == 0 else retry_limit - 1
        return await self._get(url, new_retry_limit, average_wait, rng_base)

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
        log_first_call()
        try:
            return await self._get_fields_from_incident_url(row)
        except:
            # Passing return_exceptions=True to asyncio.gather() destroys the ability
            # to print them once they're caught, so do that manually here.
            tb.print_exc()
            raise
