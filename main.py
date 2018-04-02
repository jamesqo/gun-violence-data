import shutil
import requests
import warnings

from functools import partial
#from selenium.common.exceptions import TimeoutException
from selenium.webdriver import Chrome
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib.parse import urlencode
from uuid import uuid4

def uuid_is_present(driver, _):
    form_wrapper = driver.find_element_by_css_selector('.filter-outer.form-wrapper')
    form_wrapper_id = form_wrapper.get_attribute('id')
    return 'new' not in form_wrapper_id

def main():
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', UserWarning)
        driver = Chrome()

    url = 'http://www.gunviolencearchive.org/query'
    print('GET', url)
    driver.get(url)

    filter_dropdown_trigger = driver.find_element_by_css_selector('.filter-dropdown-trigger')
    filter_dropdown_trigger.click()

    date_link = driver.find_element_by_link_text('Date')
    date_link.click()

    predicate = partial(uuid_is_present, driver)
    WebDriverWait(driver, timeout=15).until(predicate)

    form_wrapper = driver.find_element_by_css_selector('.filter-outer.form-wrapper')
    form_wrapper_id = form_wrapper.get_attribute('id')
    print(form_wrapper_id)
    start, end = len('edit-query-filters-'), form_wrapper_id.find('-outer-filter')
    uuid = form_wrapper_id[start:end]

    input_date_from_id = f'edit-query-filters-{uuid}-outer-filter-filter-field-date-from'
    input_date_to_id = f'edit-query-filters-{uuid}-outer-filter-filter-field-date-to'
    driver.execute_script(f'''
    document.getElementById("{input_date_from_id}").value = "4/1/2018";
    document.getElementById("{input_date_to_id}").value = "4/2/2018";
    ''')

    raise Exception()

if __name__ == '__main__':
    main()
