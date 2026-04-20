from datetime import datetime

from helpers.selenium_helper import *
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

TTF_SOURCE = 'https://www.theice.com/products/27996665/Dutch-TTF-Gas-Futures/data?marketId=5285049&span=1'

def getTTFFutures():
    browser = getWebDriver()
    browser.get(TTF_SOURCE)

    results = []
    table_xpath = '/html/body/div[2]/div[4]/div[1]/div/div/div/div/div/div/div/div/div/table'
    timeout = 6 # seconds
    try:
        myElem = WebDriverWait(browser, timeout).until(EC.presence_of_element_located((By.XPATH, table_xpath)))

        soup = BeautifulSoup(browser.page_source, 'html.parser')
        table = soup.find_all('table')[0]
        for row in table.contents:
            if row.name != 'tbody':
                continue
            data = row.contents[0]

            dateText = data.contents[0].text
            valueText = data.contents[1].text
            versionText = ' '.join(list(data.contents[2].strings))

            try:
                results.append({
                    'DispatchDay': datetime.strptime(dateText, '%b%y').date(),
                    'Version': datetime.strptime(versionText, '%d/%m/%Y %H:%M %p'),
                    'Value': float(valueText)
                })
            except:
                # continue on date parse error
                pass

    except TimeoutException:
        return []

    return results