import csv
import shutil
import requests
import warnings

from datetime import date, timedelta
from functools import partial
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver import Chrome
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib.parse import parse_qs, urlencode, urlparse
from uuid import uuid4

GLOBAL_START = date(year=2013, month=1, day=1)
#GLOBAL_END = date(year=2018, month=4, day=1)
GLOBAL_END = date(year=2013, month=4, day=1)

MESSAGE_NO_INCIDENTS_AVAILABLE = 'There are currently no incidents available.'

def _uuid_is_present(driver, _):
    form_wrapper = driver.find_element_by_css_selector('.filter-outer.form-wrapper')
    # TODO: It's possible that the element could become stale in between these calls.
    form_wrapper_id = form_wrapper.get_attribute('id')
    return 'new' not in form_wrapper_id

def _get_value(driver, element):
    # HACK HACK HACK
    script = 'return arguments[0].innerText;'
    return driver.execute_script(script, element)

def _get_info(driver, tr):
    tds = tr.find_elements_by_css_selector('td')
    assert len(tds) == 7
    date, state, city_or_county, address, \
        n_killed, n_injured = map(partial(_get_value, driver), tds[:6])
    n_killed, n_injured = map(int, [n_killed, n_injured])

    incident_url, source_url = tds[6].find_element_by_link_text('View Incident').get_attribute('href'), \
                               tds[6].find_element_by_link_text('View Source').get_attribute('href')

    return date, state, city_or_county, address, \
        n_killed, n_injured, \
        incident_url, source_url

def main():
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', UserWarning)
        driver = Chrome()

    step = timedelta(days=7)
    start, end = GLOBAL_START, GLOBAL_START + step - timedelta(days=1)
    with open('stage1.csv', 'w', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['date', 'state', 'city_or_county', 'address', 'n_killed', 'n_injured', 'incident_url', 'source_url'])
        while start <= GLOBAL_END:
            query(driver, start, end)
            process_batch(driver, writer)
            start, end = end + timedelta(days=1), min(GLOBAL_END, end + step)

def query(driver, start_date, end_date):
    url = 'http://www.gunviolencearchive.org/query'
    print('GET', url)
    driver.get(url)

    filter_dropdown_trigger = driver.find_element_by_css_selector('.filter-dropdown-trigger')
    filter_dropdown_trigger.click()

    date_link = driver.find_element_by_link_text('Date')
    date_link.click()

    predicate = partial(_uuid_is_present, driver)
    WebDriverWait(driver, timeout=15).until(predicate)

    form_wrapper = driver.find_element_by_css_selector('.filter-outer.form-wrapper')
    form_wrapper_id = form_wrapper.get_attribute('id')
    print(form_wrapper_id)
    start, end = len('edit-query-filters-'), form_wrapper_id.find('-outer-filter')
    uuid = form_wrapper_id[start:end]

    input_date_from_id = f'edit-query-filters-{uuid}-outer-filter-filter-field-date-from'
    input_date_to_id = f'edit-query-filters-{uuid}-outer-filter-filter-field-date-to'
    # TODO: This format string will not work on Unix. Use - instead of #
    start_date_str = start_date.strftime('%#m/%#d/%Y')
    end_date_str = end_date.strftime('%#m/%#d/%Y')

    # HACK HACK HACK
    driver.execute_script(f'''
    document.getElementById("{input_date_from_id}").value = "{start_date_str}";
    document.getElementById("{input_date_to_id}").value = "{end_date_str}";
    ''')

    form_submit = driver.find_element_by_id('edit-actions-execute')
    form_submit.click()

def process_batch(driver, writer):
    scrollable = driver.find_element_by_css_selector('.table-wrapper .scrollable')
    tds = scrollable.find_elements_by_css_selector('tr.odd td')
    if len(tds) == 1 and _get_value(driver, tds[0]) == MESSAGE_NO_INCIDENTS_AVAILABLE:
        # Nil query results.
        return

    # Since we want to write out incidents by ascending date, process pages that come later first.
    try:
        last_li = driver.find_element_by_css_selector('.pager-last.last')
        last_li.click()
    except NoSuchElementException:
        # A single page of results was returned.
        process_page(driver, writer)
        return

    # Now we're on the last page. Process each page and navigate forwards.
    last_url = driver.current_url
    base_url = last_url[:last_url.find('?')]
    last_url_query = urlparse(last_url).query
    last_pageno = parse_qs(last_url_query)['page']

    # NOTE: In true programmer fashion, the nth page is labeled '?page={n - 1}'
    process_page(driver, writer)
    for pageno in range(last_pageno - 1, 0, -1):
        url = f'{base_url}?page={pageno}'
        driver.get(url)
        process_page(driver, writer)

    # First page has no '?page=' query parameter
    url = base_url
    driver.get(url)
    process_page(driver, writer)

def process_page(driver, writer):
    scrollable = driver.find_element_by_css_selector('.table-wrapper .scrollable')
    trs = scrollable.find_elements_by_css_selector('tr.odd, tr.even')

    trs = reversed(trs) # Order by ascending date instead of descending
    infos = map(partial(_get_info, driver), trs)
    for info in infos:
        writer.writerow([*info])

if __name__ == '__main__':
    main()
