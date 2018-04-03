import asyncio
import csv

from aiohttp import ClientSession
from bs4 import BeautifulSoup

GVA_DOMAIN = 'http://www.gunviolencearchive.org'

def _get_info(tr):
    tds = tr.select('td')
    assert len(tds) == 7

    date, state, city_or_county, address, n_killed, n_injured = [td.contents[0] for td in tds[:6]]
    print(locals())
    n_killed, n_injured = map(int, [n_killed, n_injured])

    incident_a = tds[6].find('a', string='View Incident')
    incident_url = GVA_DOMAIN + incident_a['href']

    source_a = tds[6].find('a', string='View Source')
    source_url = GVA_DOMAIN + (source_a['href'] if source_a else '')

    return date, state, city_or_county, address, n_killed, n_injured, incident_url, source_url

class Stage1Serializer(object):
    def __init__(self, output_fname, encoding='utf-8'):
        self._output_fname = output_fname
        self._encoding = encoding

    async def __aenter__(self):
        self._output_file = open(self._output_fname, 'w', encoding=self._encoding)
        self._writer = csv.writer(self._output_file)
        self._sess = await ClientSession().__aenter__()
        return self

    async def __aexit__(self, type, value, tb):
        self._output_file.__exit__(type, value, tb)
        await self._sess.__aexit__(type, value, tb)

    async def _gettext(self, url):
        async with self._sess.get(url) as resp:
            return await resp.text()

    def write_header(self):
        self._writer.writerow([
            'date',
            'state',
            'city_or_county',
            'address',
            'n_killed',
            'n_injured',
            'incident_url',
            'source_url'
        ])

    async def write_batch(self, query_url, n_pages):
        urls = [query_url] + ['{}?page={}'.format(query_url, pageno) for pageno in range(1, n_pages)]
        tasks = [self.write_page(url) for url in urls]
        return await asyncio.gather(*tasks)

    async def write_page(self, page_url):
        text = await self._gettext(page_url)
        soup = BeautifulSoup(text, features='html5lib')
        trs = soup.select('.responsive .odd, .responsive .even')
        trs = reversed(trs) # Order by ascending date instead of descending
        infos = map(_get_info, trs)
        for info in infos:
            self._writer.writerow([*info])

