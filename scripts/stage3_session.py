from aiohttp import ClientSession
from aiohttp.hdrs import CONTENT_TYPE

from stage3_extractor import Stage3Extractor

class Stage3Session(object):
    def __init__(self):
        self._extractor = Stage3Extractor()

    async def __aenter__(self):
        self._sess = await ClientSession().__aenter__()
        return self

    async def __aexit__(self, type, value, tb):
        await self._sess.__aexit__(type, value, tb)

    async def get_fields(self, incident_url):
        async with self._sess.get(incident_url) as resp:
            ctype = resp.headers.get(CONTENT_TYPE, '').lower()
            mimetype = ctype[:ctype.find(';')]
            if mimetype in ('text/htm', 'text/html'):
                text = await resp.text()
            else:
                raise NotImplementedError("Encountered unknown mime type {}".format(mimetype))
        return self._extractor.extract_fields(text)
