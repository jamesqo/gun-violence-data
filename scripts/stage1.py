#!/usr/bin/env python3
# stage 1: initial import of data fom gunviolencearchive.org using web scraping techniques

import asyncio
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
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib.parse import parse_qs, urlparse

from stage1_serializer import Stage1Serializer

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

    driver.get('http://www.gunviolencearchive.org/query')

    # Click "Add a rule"
    filter_dropdown_trigger = driver.find_element_or_wait(By.CSS_SELECTOR, '.filter-dropdown-trigger')
    driver.click(filter_dropdown_trigger)

    # Click "Date"
    date_link = driver.find_element_or_wait(By.LINK_TEXT, 'Date')
    driver.click(date_link)

    # Fill in the date fields
    input_date_from = driver.find_element_or_wait(By.CSS_SELECTOR, 'input[id$="filter-field-date-from"]')
    input_date_to = driver.find_element_or_wait(By.CSS_SELECTOR, 'input[id$="filter-field-date-to"]')
    start_date_str = start_date.strftime(DATE_FORMAT)
    end_date_str = end_date.strftime(DATE_FORMAT)
    script = '''
    arguments[0].value = "{}";
    arguments[1].value = "{}";
    '''.format(start_date_str, end_date_str)
    driver.execute_script(script, input_date_from, input_date_to)

    # Click submit button
    form_submit = driver.find_element_or_wait(By.ID, 'edit-actions-execute')
    driver.click(form_submit)

    # Extract the number of pages from the pager
    return driver.current_url, get_n_pages(driver)

def get_n_pages(driver):
    try:
        last_a = driver.find_element_or_wait(By.CSS_SELECTOR, 'a[title="Go to last page"]', timeout=1)
        last_url = last_a.get_attribute('href')
        form_data = urlparse(last_url).query
        n_pages = int(parse_qs(form_data)['page'][0]) + 1

        return n_pages
    except NoSuchElementException:
        tds = driver.find_elements_or_wait(By.CSS_SELECTOR, '.responsive tbody tr td')
        if len(tds) == 1 and driver.get_value(tds[0]) == MESSAGE_NO_INCIDENTS_AVAILABLE:
            # Nil query results.
            return 0

        # A single page of results was returned.
        return 1

async def main():
    args = parse_args()
    log.basicConfig(level=args.log_level)
    driver = Chrome()

    step = timedelta(days=1)
    global_start, global_end = dateparser.parse(args.start_date), dateparser.parse(args.end_date)
    start, end = global_start, global_start + step - timedelta(days=1)

    async with Stage1Serializer(output_fname=args.output_file) as serializer:
        serializer.write_header()
        while start <= global_end:
            query_url, n_pages = query(driver, start, end)
            if n_pages > 0:
                serializer.write_batch(query_url, n_pages)
            start, end = end + timedelta(days=1), min(global_end, end + step)
        await serializer.flush_writes()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
