import shutil
import requests
import warnings

from selenium.webdriver import PhantomJS
from urllib.parse import urlencode
from uuid import uuid4

def get_uuids(driver):
    print('GET', 'http://www.gunviolencearchive.org/query')
    driver.get('http://www.gunviolencearchive.org/query')
    query_id = driver.find_element_by_name('query[query_id]').get_property('value')
    form_build_id = driver.find_element_by_name('form_build_id').get_property('value')
    return query_id, form_build_id

def main():
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', UserWarning)
        driver = PhantomJS()

    query_id, form_build_id = get_uuids(driver)
    uuid = uuid4()

    form_data = {
        'query[base_group][base_group_select]': 'And',
        f'query[filters][{uuid}][type]': 'IncidentDate',
        f'query[filters][{uuid}][outer_filter][weight]': '001',
        f'query[filters][{uuid}][outer_filter][comparator]': 'is in',
        f'query[filters][{uuid}][outer_filter][filter][field][date-from]': '4/1/2018',
        f'query[filters][{uuid}][outer_filter][filter][field][date-to]': '4/2/2018',
        'query[filters][new][type]': '',
        'query[filters][outer_filter][weight]': '0.002',
        'query[query_id]': query_id,
        'form_build_id': form_build_id,
        'form_id': 'gva_entry_query',
        'op': 'Search',
    }
    params = urlencode(form_data)
    url = f'http://www.gunviolencearchive.org/query?{params}'

    print('POST', url)
    headers = '''
    Host: www.gunviolencearchive.org
    Connection: keep-alive
    Cache-Control: max-age=0
    Origin: http://www.gunviolencearchive.org
    Upgrade-Insecure-Requests: 1
    Content-Type: application/x-www-form-urlencoded
    Save-Data: on
    User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.162 Safari/537.36
    Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
    Referer: http://www.gunviolencearchive.org/query
    Accept-Encoding: gzip, deflate
    Accept-Language: en-US,en;q=0.9
    Cookie: __cfduid=d0402f9d65212b4cca2ae4534effa46b31522690623; has_js=1; SESS193bf903d09f76bb11992108b9bcc263=zQN3CieZrm-7Ix9kOPZKq3QYNjXoSzgxgeYqU34n5hk
    '''
    headers = {line[:line.find(':')].strip(): line[line.find(':')+2:].strip() for line in headers.strip().split('\n')}
    print(headers)

    response = requests.post(url, headers)
    print(f"POST status code: {response.status_code}")

    print('GET', f'http://www.gunviolencearchive.org/query/{query_id}')
    driver.get(f'http://www.gunviolencearchive.org/query/{query_id}')
    print([x.get_attribute('id') for x in driver.find_elements_by_css_selector('*')])
    table_wrapper = driver.find_element_by_id('table-wrapper')
    rows = table_wrapper.find_elements_by_class_name('odd') + \
           table_wrapper.find_elements_by_class_name('even')

    print(len(rows))

if __name__ == '__main__':
    main()
