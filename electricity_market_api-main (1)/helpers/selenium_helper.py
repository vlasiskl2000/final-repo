from selenium import webdriver

def getWebDriver():
    op = webdriver.ChromeOptions()
    op.add_argument('headless')
    browser = webdriver.Chrome(executable_path="C:\chromedriver.exe", options=op)
    return browser