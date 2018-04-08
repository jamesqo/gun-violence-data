import asyncio
import math
import numpy as np
import platform
import sys
import traceback as tb

from aiohttp import ClientResponse, ClientSession, TCPConnector
from aiohttp.client_exceptions import ClientOSError
from aiohttp.hdrs import CONTENT_TYPE
from asyncio import CancelledError
from collections import namedtuple

from log_utils import log_first_call
from stage2_extractor import Stage2Extractor

Context = namedtuple('Context', ['address', 'city_or_county', 'state'])

def _compute_wait(average_wait, rng_base):
    log_first_call()
    log_average_wait = math.log(average_wait, rng_base)
    fuzz = np.random.standard_normal(size=1)[0]
    return int(np.ceil(rng_base ** (log_average_wait + fuzz)))

def _status_from_exception(exc):
    if isinstance(exc, CancelledError):
        return '<canceled>'
    if isinstance(exc, ClientOSError) and platform.system() == 'Windows' and exc.errno == 10054:
        # WinError: An existing connection was forcibly closed by the remote host
        return '<conn closed>'
    if isinstance(exc, asyncio.TimeoutError):
        return '<timed out>'

    return ''

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

    async def _get(self, url, average_wait=20, rng_base=2):
        while True:
            try:
                resp = await self._sess.get(url)
            except Exception as exc:
                status = _status_from_exception(exc)
                if not status:
                    raise
            else:
                status = resp.status
                if status < 400: # Suceeded
                    return resp
                elif 400 <= status < 500: # Client error
                    self._log_failed_request(url)
                    return resp
                # It's a server error. Dispose the response and retry.
                await resp.release()

            wait = _compute_wait(average_wait, rng_base)
            self._log_retry(url, status, wait)
            await asyncio.sleep(wait)

    async def _get_fields_from_incident_url(self, row):
        incident_url = row['incident_url']
        resp = await self._get(incident_url)
        async with resp:
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
