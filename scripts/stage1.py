#!/usr/bin/env python3

import csv
import dateutil.parser as dateparser
import logging as log
import platform
import sys
import warnings

import selenium_utils

from argparse import ArgumentParser
from calendar import monthrange
from datetime import date, timedelta
from functools import partial
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from urllib.parse import parse_qs, urlparse

MESSAGE_NO_INCIDENTS_AVAILABLE = 'There are currently no incidents available.'

def _uuid_is_present(driver, _):
    form_wrapper = driver.find_element_or_wait(By.CSS_SELECTOR, '.filter-outer.form-wrapper')
    # TODO: It's possible that the element could become stale in between these calls.
    form_wrapper_id = None
    while form_wrapper_id is None:
        try:
            form_wrapper_id = form_wrapper.get_attribute('id')
        except StaleElementReferenceException:
            # Remember, this code is being run while we're waiting for the form wrapper to be updated.
            # There's a real chance that update could happen in between the find_element_or_wait and get_attribute calls.
            continue
    return 'new' not in form_wrapper_id

def _fmt_date(date):
    # Format as %m/%d/%Y, but do not leave leading zeroes on the month or day.
    # Surprisingly, the syntax for this is different across platforms: https://stackoverflow.com/a/2073189/4077294
    fmt_string = '%#m/%#d/%Y' if platform.system() == 'Windows' else '%-m/%-d/%Y'
    return date.strftime(fmt_string)

def _get_info(driver, tr):
    tds = driver.find_elements_or_wait(By.CSS_SELECTOR, 'td', ancestor=tr)
    assert len(tds) == 7
    date, state, city_or_county, address, \
        n_killed, n_injured = map(driver.get_value, tds[:6])
    n_killed, n_injured = map(int, [n_killed, n_injured])

    incident_a = driver.find_element_or_wait(By.LINK_TEXT, 'View Incident', ancestor=tds[6])
    incident_url = incident_a.get_attribute('href')
    try:
        source_a = driver.find_element_or_wait(By.LINK_TEXT, 'View Source', ancestor=tds[6])
        source_url = source_a.get_attribute('href')
    except NoSuchElementException:
        source_url = ''

    return date, state, city_or_county, address, \
        n_killed, n_injured, \
        incident_url, source_url

def main():
    args = parse_args()
    log.basicConfig(level=args.log_level)
    driver = Chrome()

    step = timedelta(days=1)
    global_start, global_end = dateparser.parse(args.start_date), dateparser.parse(args.end_date)
    start, end = global_start, global_start + step - timedelta(days=1)

    with open(args.output_file, 'w', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['date', 'state', 'city_or_county', 'address', 'n_killed', 'n_injured', 'incident_url', 'source_url'])

        while start <= global_end:
            query(driver, start, end)
            process_batch(driver, writer)
            start, end = end + timedelta(days=1), min(global_end, end + step)

def parse_args():
    parts = sys.argv[1].split('-')
    if len(parts) == 2:
        del sys.argv[1]
    
    parser = ArgumentParser()
    if len(parts) != 2:
        parser.add_argument(
            'start_date',
            metavar='START',
            help="set start date",
            action='store',
        )
        parser.add_argument(
            'end_date',
            metavar='END',
            help="set end date",
            action='store',
        )
        parser.add_argument(
            'output_file',
            metavar='OUTFILE',
            help="set output file",
            action='store',
        )

    parser.add_argument(
        '-d', '--debug',
        help="show debug information",
        action='store_const',
        dest='log_level',
        const=log.DEBUG,
        default=log.WARNING,
    )

    args = parser.parse_args()
    if len(parts) == 2: # e.g. '02-2014'
        month, year = map(int, parts)
        end_day = monthrange(year, month)[1]
        
        args.start_date = '{}-01-{}'.format(month, year)
        args.end_date = '{}-{}-{}'.format(month, end_day, year)
        args.output_file = 'stage1.{:02d}.{:04d}.csv'.format(month, year)
    return args


def query(driver, start_date, end_date):
    log.debug("Querying incidents between {:%m/%d/%Y} and {:%m/%d/%Y}".format(start_date, end_date))

    url = 'http://www.gunviolencearchive.org/query'
    print('GET', url)
    driver.get(url)

    filter_dropdown_trigger = driver.find_element_or_wait(By.CSS_SELECTOR, '.filter-dropdown-trigger')
    driver.click(filter_dropdown_trigger)

    date_link = driver.find_element_or_wait(By.LINK_TEXT, 'Date')
    driver.click(date_link)

    wait = WebDriverWait(driver, timeout=15)
    predicate = partial(_uuid_is_present, driver)
    wait.until(predicate)

    form_wrapper = driver.find_element_or_wait(By.CSS_SELECTOR, '.filter-outer.form-wrapper')
    form_wrapper_id = form_wrapper.get_attribute('id')
    start, end = len('edit-query-filters-'), form_wrapper_id.find('-outer-filter')
    uuid = form_wrapper_id[start:end]

    input_date_from_id = 'edit-query-filters-{}-outer-filter-filter-field-date-from'.format(uuid)
    input_date_to_id = 'edit-query-filters-{}-outer-filter-filter-field-date-to'.format(uuid)
    # TODO: This format string will not work on Unix. Use - instead of #
    start_date_str = _fmt_date(start_date)
    end_date_str = _fmt_date(end_date)

    # HACK HACK HACK
    script = '''
    document.getElementById("{input_date_from_id}").value = "{start_date_str}";
    document.getElementById("{input_date_to_id}").value = "{end_date_str}";
    '''.format(input_date_from_id=input_date_from_id,
               input_date_to_id=input_date_to_id,
               start_date_str=start_date_str,
               end_date_str=end_date_str)
    driver.execute_script(script)

    print('GET', '{results_url}')
    form_submit = driver.find_element_or_wait(By.ID, 'edit-actions-execute')
    driver.click(form_submit)

def process_batch(driver, writer):
    tds = driver.find_elements_or_wait(By.CSS_SELECTOR, '.responsive .odd td')
    if len(tds) == 1 and driver.get_value(tds[0]) == MESSAGE_NO_INCIDENTS_AVAILABLE:
        # Nil query results.
        return

    base_url = driver.current_url
    # Since we want to write out incidents by ascending date, process pages that come later first.
    try:
        print('GET', '{}?page={{last_pageno}}'.format(base_url))
        last_li = driver.find_element_or_wait(By.CSS_SELECTOR, '.pager-last.last')
        driver.click(last_li)
    except NoSuchElementException:
        # A single page of results was returned.
        process_page(driver, writer)
        return

    # Now we're on the last page. Process each page and navigate forwards.
    last_url = driver.current_url
    last_url_query = urlparse(last_url).query
    last_pageno = int(parse_qs(last_url_query)['page'][0])

    # NOTE: In true programmer fashion, the nth page is labeled '?page={n - 1}'
    process_page(driver, writer)
    for pageno in range(last_pageno - 1, 0, -1):
        url = '{}?page={}'.format(base_url, pageno)
        print('GET', url)
        driver.get(url)
        process_page(driver, writer)

    # First page has no '?page=' query parameter
    url = base_url
    print('GET', url)
    driver.get(url)
    process_page(driver, writer)

def process_page(driver, writer):
    trs = driver.find_elements_or_wait(By.CSS_SELECTOR, '.responsive .odd, .responsive .even')
    trs = reversed(trs) # Order by ascending date instead of descending
    infos = map(partial(_get_info, driver), trs)
    for info in infos:
        writer.writerow([*info])

if __name__ == '__main__':
    main()
