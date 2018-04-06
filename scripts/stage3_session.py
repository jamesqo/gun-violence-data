import sys

from aiohttp import ClientSession
from aiohttp.hdrs import CONTENT_TYPE
from collections import namedtuple

from stage3_extractor import Stage3Extractor

Context = namedtuple('Context', ['address', 'city_or_county', 'state'])

class Stage3Session(object):
    def __init__(self):
        self._extractor = Stage3Extractor()

    async def __aenter__(self):
        self._sess = await ClientSession().__aenter__()
        return self

    async def __aexit__(self, type, value, tb):
        await self._sess.__aexit__(type, value, tb)

    async def get_fields_from_incident_url(self, row):
        incident_url = row['incident_url']
        async with self._sess.get(incident_url) as resp:
            if resp.status >= 400:
                print("ERROR! Failed GET request to {}".format(incident_url), file=sys.stderr)
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
            print("ERROR! Extractor failed for the following webpage: {}".format(incident_url), file=sys.stderr)
            raise
