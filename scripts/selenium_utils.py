from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

def click(self, element):
    script = 'arguments[0].scrollIntoView();'
    self.execute_script(script, element)
    element.click()

def find_element_or_wait(self, by, value, ancestor=None, timeout=10):
    ancestor = ancestor or self
    try:
        # Common case: the element is already loaded and we don't need to wait.
        return ancestor.find_element(by, value)
    except NoSuchElementException:
        try:
            wait = WebDriverWait(self, timeout)
            wait.until(EC.visibility_of_element_located((by, value)))
        except TimeoutException:
            # Let the next find_element() call throw a NoSuchElementException.
            pass
        return ancestor.find_element(by, value)

def find_elements_or_wait(self, by, value, ancestor=None, timeout=10):
    ancestor = ancestor or self
    try:
        # Common case: the elements are already loaded and we don't need to wait.
        return ancestor.find_elements(by, value)
    except NoSuchElementException:
        try:
            wait = WebDriverWait(self, timeout)
            wait.until(EC.visibility_of_element_located((by, value)))
        except TimeoutException:
            # Let the next find_elements() call throw a NoSuchElementException.
            pass
        return ancestor.find_elements(by, value)

def get_value(self, element, decode=True):
    property = 'innerText' if decode else 'innerHTML'
    script = 'return arguments[0].{};'.format(property)
    return self.execute_script(script, element)

def has_page_loaded(self):
    script = 'return document.readyState;'
    return self.execute_script(script) == 'complete'

# TODO: Use getattr/setattr and __all__ instead of writing things out by hand.
WebDriver.click = click
WebDriver.find_element_or_wait = find_element_or_wait
WebDriver.find_elements_or_wait = find_elements_or_wait
WebDriver.get_value = get_value
WebDriver.has_page_loaded = has_page_loaded
