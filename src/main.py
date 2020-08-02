import logging
import os
import time
import csv
from contextlib import contextmanager
import shutil

import logging_gelf.formatters
import logging_gelf.handlers
from selenium import webdriver
from selenium.webdriver import FirefoxOptions
from selenium.common.exceptions import NoSuchElementException
from keboola import docker


@contextmanager
def browser_handler(browserdriver):
    try:
        yield browserdriver
    finally:
        browserdriver.quit()


def setup_browser_profile(browser_profile, download_files_path):
    browser_profile.set_preference("browser.download.folderList", 2)  # custom location
    browser_profile.set_preference("browser.download.manager.showWhenStarting", False)
    browser_profile.set_preference("browser.download.dir", download_files_path)
    browser_profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv")
    return browser_profile


def login(webbrowser, email, passphrase):
    name_field = webbrowser.find_element_by_id("frm-loginForm-loginForm-email")
    name_field.send_keys(email)
    pass_field = webbrowser.find_element_by_id("frm-loginForm-loginForm-password")
    pass_field.send_keys(passphrase)
    login_button = webbrowser.find_element_by_xpath(
        '//*[@id="frm-loginForm-loginForm"]/fieldset/footer/button'
    )
    login_button.click()


def generate_new_sortiment_report(webbrowser):
    try:
        generate_report_button = webbrowser.find_element_by_xpath(
            '//*[@id="right"]/table[1]/tbody/tr[2]/td[3]/button'
        )
        generate_report_button.click()
    except NoSuchElementException:
        logger.error(
            "Generate report button is not visible. It might have been clicked recently."
        )
    else:
        webbrowser.switch_to.alert.accept()


logging.basicConfig(
    level=logging.DEBUG, handlers=[]
)  # do not create default stdout handler
logger = logging.getLogger()

try:
    logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
        host=os.getenv("KBC_LOGGER_ADDR"), port=int(os.getenv("KBC_LOGGER_PORT"))
    )
except TypeError:
    logging_gelf_handler = logging.StreamHandler()

logging_gelf_handler.setFormatter(
    logging_gelf.formatters.GELFFormatter(null_character=True)
)
logger.addHandler(logging_gelf_handler)
logger.setLevel(logging.INFO)

OUTCOLS = ["material", "country", "distrchan", "source", "cse_id", "category_name"]
datadir = os.getenv("KBC_DATADIR", "/data/")
download_path = f"{datadir}in/files/"
results_path = f'{os.getenv("KBC_DATADIR")}out/tables/results.csv'
conf = docker.Config(datadir)

params = conf.get_parameters()
logger.info("Extracted parameters.")

shops_list = list(params.keys())

for shop in shops_list:
    login_url = params[shop]["login_url"]
    username = params[shop]["username"]
    password = params[shop]["#password"]
    sortiment_report_url = params[shop]["sortiment_report_url"]
    country = params[shop]["country"]
    distrchan = params[shop]["distrchan"]
    colnames_mapping = params[shop]["colnames_mapping"]

    os.makedirs(download_path, exist_ok=True)

    profile = setup_browser_profile(webdriver.FirefoxProfile(), download_path)
    opts = FirefoxOptions()
    opts.add_argument("--headless")
    driver = webdriver.Firefox(firefox_profile=profile, options=opts)

    with browser_handler(driver) as browser:
        logging.info(f"Going to {login_url}")
        browser.get(login_url)

        logger.info("Logging in.")
        login(browser, email=username, passphrase=password)

        logger.info("Going to sortiment report page.")
        browser.get(sortiment_report_url)

        generate_new_sortiment_report(browser)

        latest_csv_download_button = browser.find_element_by_xpath(
            '//*[@id="right"]/table[2]/tbody/tr[2]/td[3]/a[2]'
        )
        logger.info("Clicking download.")
        # without resizing, the button is not accessible in headless mode
        browser.set_window_size(1280, 1024)
        # wait for download button to be accessible
        time.sleep(2)
        latest_csv_download_button.click()

    for file in os.listdir(download_path):
        logger.info(f"Processing file {file}")
        result_file_existed = os.path.isfile(results_path)
        with open(f"{download_path}{file}", "r") as infile, open(
            results_path, "a+"
        ) as outfile:
            dict_reader = csv.DictReader(infile)
            dict_writer = csv.DictWriter(
                outfile, fieldnames=OUTCOLS, extrasaction="ignore"
            )
            if not result_file_existed:
                dict_writer.writeheader()
            for line in dict_reader:
                line_renamed = {
                    colnames_mapping.get(key, key): val for key, val in line.items()
                }
                dict_writer.writerow(
                    {
                        **line_renamed,
                        "country": country,
                        "distrchan": distrchan,
                        "source": "heureka",
                    }
                )

    # delete downloaded file to avoid duplicate processing
    shutil.rmtree(download_path, ignore_errors=True)
