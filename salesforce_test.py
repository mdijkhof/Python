from selenium import webdriver

from megatron import chrome

# create a new Chrome session
driver = chrome.chrome_driver()
driver.implicitly_wait(300)
driver.maximize_window()

account_list = ['001C000001T3FQrIAN']

for account_id in account_list:
# Navigate to the application home page
    driver.get(f"https://groupon-dev.my.salesforce.com/{account_id}")
    driver.implicitly_wait(100)

    print(driver)
    
    
    # search_field = driver.find_element_by_xpath("//input[@title='Search Salesforce']")
    # search_field.click();
    # search_field.clear();
    # search_field.send_keys("case number").wait();
    # driver.find_element_by_xpath("//span[@title='case number']");
    # search_field.click();
    # driver.implicitly_wait(10)
    # search_field.submit();
    # search_field.sendKeys(Keys.RETURN);