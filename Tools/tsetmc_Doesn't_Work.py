import time

import pandas
from selenium import webdriver
from selenium.webdriver.common.by import By


#AdminPanel_credentials = ["https://tavazon.ephoenix.ir", "mehdy", "123"]


driver = webdriver.Firefox(executable_path="E:\Moini\WebDrivers\geckodriver.exe")
fp = webdriver.FirefoxProfile()
driver.get('https://www.sahamyab.com/api/proxy/symbol/stockWatch?v=0.1&namad=&market=1&type=&sector=&page=6&sort=&pageSize=20&')
t = driver.page_source
df = pandas.read_html(t, skiprows=0)[0]
print(t)
# def login_to_website(credentials):
#     driver = webdriver.Firefox(executable_path="E:\webdrivers\geckodriver.exe")
#     driver.get(credentials[0])
#     driver.maximize_window()
#     cf.enter_input(driver, "input#keyboard-user", credentials[1])
#     cf.enter_input(driver, "input#keyboard-pass", credentials[2])
#     #cf.find_and_click(driver, "input#Captcha")
#     time.sleep(10)
#     cf.find_and_click(driver, "div.col-xs-6:nth-child(6) > button:nth-child(1)")
#
#     cf.find_and_click(driver,"div.col-sm-6:nth-child(5) > div:nth-child(1) > div:nth-child(2) > div:nth-child(3) > div:nth-child(2) > a:nth-child(1) > div:nth-child(1) > div:nth-child(2)")
#     for i in range(1000):
#         try:
#             try:
#                 cf.error_check_if(driver,"html body.colorMain.body-alert.modal-open div.modal.fade.ui-draggable.modalError.in div.modal-dialog.modal1 div.modal-content div.modal-body form div.form-group.row.noMargin div.pull-left button.btn.btnDef")
#                 cf.find_and_click(driver,"html body.colorMain.body-alert.modal-open div.modal.fade.ui-draggable.modalError.in div.modal-dialog.modal1 div.modal-content div.modal-body form div.form-group.row.noMargin div.pull-left button.btn.btnDef")
#             except:
#                 print("2")
#
#             cf.enter_input(driver,
#                        "body > div.modal.fade.ui-draggable.order-buy.in > div > div > div.modal-body.bgColor2 > form > div:nth-child(2) > input",
#                        150)
#             cf.enter_input(driver,
#                        "body > div.modal.fade.ui-draggable.order-buy.in > div > div > div.modal-body.bgColor2 > form > div:nth-child(3) > input",
#                        50414)
#             cf.find_and_click(driver,
#                           "div.modal:nth-child(123) > div:nth-child(1) > div:nth-child(1) > div:nth-child(2) > form:nth-child(1) > div:nth-child(10) > div:nth-child(2) > div:nth-child(1) > button:nth-child(1)")
#             print(i)
#         except:
#             print(3)
#
#
#
#
#
#     return driver
#
#
# def go_to_rule_page(driver):
#     time.sleep(2)
#     cf.find_and_click(driver, "li:nth-of-type(3) > a > .ng-binding")
#     cf.find_and_click(driver, "li:nth-of-type(3) > .ng-scope.sidebar-submenu  .ng-binding")
#     time.sleep(1)
#     return driver
#
#
# def write_in_new_rule(driver):
#     cf.find_and_click(driver, "a:nth-of-type(1) > .fa.fa-plus")
#     driver.find_element(By.ID, "Title").click()
#     driver.find_element(By.ID, "Title").send_keys("حداگثر سفارش خرید وپاسار")
#     driver.find_element(By.ID, "Check").click()
#     dropdown = driver.find_element(By.ID, "Check")
#     dropdown.find_element(By.XPATH, "//option[. = 'حداکثر حجم سفارش خرید']").click()
#     driver.find_element_by_css_selector("#SelectedUser > div > div > div > div > div > div > div > input").send_keys(
#         "oms")
#     time.sleep(2)
#     cf.find_and_click(driver,
#                       ".k-group.k-list-container.k-popup.k-reset.k-state-border-up  ul[role='listbox'] > li["
#                       "role='option']")
#     driver.find_element_by_css_selector(
#         "#SelectedInstrument > div > div > div > div > div > div > div > input").send_keys("وپاسار")
#     time.sleep(0.2)
#     cf.find_and_click(driver,
#                       ".k-group.k-list-container.k-popup.k-reset.k-state-border-up  ul[role='listbox'] > "
#                       "li:nth-of-type(1)")
#     driver.find_element(By.ID, "FaMessage").click()
#     driver.find_element(By.ID, "FaMessage").send_keys("امکان  خرید بیش از 300 وجود ندارد")
#     driver.find_element(By.CSS_SELECTOR, "[ng-if] .ng-pristine").send_keys("300")
#     driver.find_element(By.CSS_SELECTOR, ".form-group > .col-md-8 .ng-pristine").click()
#     driver.find_element(By.CSS_SELECTOR, ".fa-check").click()
#
#
# driver = login_to_website(AdminPanel_credentials)
# go_to_rule_page(driver)
# write_in_new_rule(driver)
