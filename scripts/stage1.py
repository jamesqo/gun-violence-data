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
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from urllib.parse import parse_qs, urlparse

# Formats as %m/%d/%Y, but does not leave leading zeroes on the month or day.
# Surprisingly, the syntax for this is different across platforms: https://stackoverflow.com/a/2073189/4077294
DATE_FORMAT = '%#m/%#d/%Y' if platform.system() == 'Windows' else '%-m/%-d/%Y'

MESSAGE_NO_INCIDENTS_AVAILABLE = 'There are currently no incidents available.'

def parse_args():
    targets_specific_month = False
    if len(sys.argv) > 1:
        parts = sys.argv[1].split('-')
        if len(parts) == 2: # e.g. '02-2014'
            targets_specific_month = True
            del sys.argv[1]

    parser = ArgumentParser()
    if not targets_specific_month:
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
    if targets_specific_month:
        month, year = map(int, parts)
        end_day = monthrange(year, month)[1]
        
        args.start_date = '{}-01-{}'.format(month, year)
        args.end_date = '{}-{}-{}'.format(month, end_day, year)
        args.output_file = 'stage1.{:02d}.{:04d}.csv'.format(month, year)
    return args

def query(driver, start_date, end_date):
    print("Querying incidents between {:%m/%d/%Y} and {:%m/%d/%Y}".format(start_date, end_date))

    driver.get_verbose('http://www.gunviolencearchive.org/query')

    # Click "Add a rule"
    filter_dropdown_trigger = driver.find_element_or_wait(By.CSS_SELECTOR, '.filter-dropdown-trigger')
    driver.click(filter_dropdown_trigger)

    # Click "Date"
    date_link = driver.find_element_or_wait(By.LINK_TEXT, 'Date')
    driver.click(date_link)

    input_date_from = driver.find_element_or_wait(By.CSS_SELECTOR, 'input[id$="filter-field-date-from"]')
    input_date_to = driver.find_element_or_wait(By.CSS_SELECTOR, 'input[id$="filter-field-date-to"]')
    start_date_str = start_date.strftime(DATE_FORMAT)
    end_date_str = end_date.strftime(DATE_FORMAT)

    # HACK HACK HACK
    script = '''
    arguments[0].value = "{}";
    arguments[1].value = "{}";
    '''.format(start_date_str, end_date_str)
    driver.execute_script(script, input_date_from, input_date_to)

    form_submit = driver.find_element_or_wait(By.ID, 'edit-actions-execute')
    driver.click(form_submit)

    return driver.current_url

def process_batch(driver, writer):
    tds = driver.find_elements_or_wait(By.CSS_SELECTOR, '.responsive .odd td')
    if len(tds) == 1 and driver.get_value(tds[0]) == MESSAGE_NO_INCIDENTS_AVAILABLE:
        # Nil query results.
        return

    base_url = driver.current_url
    # Since we want to write out incidents by ascending date, process pages that come later first.
    try:
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
        driver.get_verbose(url='{}?page={}'.format(base_url, pageno))
        process_page(driver, writer)

    # First page has no '?page=' query parameter
    driver.get_verbose(url=base_url)
    process_page(driver, writer)

def process_page(driver, writer):
    trs = driver.find_elements_or_wait(By.CSS_SELECTOR, '.responsive .odd, .responsive .even')
    trs = reversed(trs) # Order by ascending date instead of descending
    infos = map(partial(get_info, driver), trs)
    for info in infos:
        writer.writerow([*info])

def get_info(driver, tr):
    tds = driver.find_elements_or_wait(By.CSS_SELECTOR, 'td', ancestor=tr)
    assert len(tds) == 7
    date, state, city_or_county, address, n_killed, n_injured = map(driver.get_value, tds[:6])
    n_killed, n_injured = map(int, [n_killed, n_injured])

    incident_a = driver.find_element_or_wait(By.LINK_TEXT, 'View Incident', ancestor=tds[6])
    incident_url = incident_a.get_attribute('href')

    try:
        source_a = driver.find_element_or_wait(By.LINK_TEXT, 'View Source', ancestor=tds[6])
        source_url = source_a.get_attribute('href')
    except NoSuchElementException:
        source_url = ''

    return date, state, city_or_county, address, n_killed, n_injured, incident_url, source_url

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
            query_url = query(driver, start, end)
            process_batch(driver, writer)
            start, end = end + timedelta(days=1), min(global_end, end + step)

if __name__ == '__main__':
    main()
