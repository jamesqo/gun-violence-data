import asyncio
import sys

from aiohttp import ClientSession, TCPConnector
from aiohttp.hdrs import CONTENT_TYPE
from collections import namedtuple

from stage3_extractor import Stage3Extractor

Context = namedtuple('Context', ['address', 'city_or_county', 'state'])

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

    async def _get(self, url, retry_limit=5, retry_wait=10):
        resp = await self._sess.get(url)
        status = resp.status
        if retry_limit == 1 or status < 400:
            return resp
        elif 400 <= status < 500:
            self._log_failed_request(url)
            return resp

        # Server error, try again.
        async with resp: # Dispose of the response immediately.
            pass
        self._log_retry(url, resp.status, retry_wait)
        await asyncio.sleep(retry_wait)
        return await self._get(url, retry_limit - 1, retry_wait)

    async def get_fields_from_incident_url(self, row):
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
        except Exception:
            self._log_extraction_failed(incident_url)
            raise
