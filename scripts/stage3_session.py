from aiohttp import ClientSession

from stage3_extractor import Stage3Extractor

class Stage3Session(object):
    def __init__(self):
        self._extractor = Stage3Extractor()

    async def __aenter__(self):
        self._sess = await ClientSession().__aenter__()
        return self

    async def __aexit__(self, type, value, tb):
        await self._sess.__aexit__(type, value, tb)

    async def _gettext(self, url):
        async with self._sess.get(url) as resp:
            return await resp.text()

    async def get_fields(self, incident_url):
        text = await self._gettext(incident_url)
        return self._extractor.extract_fields(text)
