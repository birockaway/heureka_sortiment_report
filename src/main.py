"""
Launches generations of heureka sortiment report if one was not launched in the last hour.
Downloads las available sortiment report and writes to csvs as specified in config.
"""
import html
import logging
import os
import re
from csv import DictReader, DictWriter
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import logging_gelf.formatters
import logging_gelf.handlers
from keboola import docker
from bs4 import BeautifulSoup


def extract_tag(input_string):
    input_name = re.search('<input name="([a-z])"', input_string).group(1)
    value = re.search(' value="((.*?)+)"', input_string).group(1)
    return input_name, value


def extract_report_url_dict(text):
    url_dict = dict()
    for part in text:
        if re.match("<input name", str(part)) is not None:
            k, v = extract_tag(str(part))
            url_dict[k] = v
    return url_dict


def check_report_generation(text):
    se = re.sub("<td>|</td>", "", str(text))
    try:
        last_dt = datetime.strptime(se, "%d.%m.%Y %H:%M:%S").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        logging.info(
            f"Current report started generating at: {last_dt}. Cannot generate new now."
        )
        return True
    except ValueError:
        return False


def get_first_csv_link(sortiment_report_html):
    for link_elem in sortiment_report_html.find_all("a", href=True):
        link = str(html.unescape(link_elem["href"]))
        if link[-3:] == "csv" and "sortiment-report" in link:
            return link


def get_formatted_dicts_from_csv(filename, mapping, country, distrchan):
    add_fields = {
        "country": country,
        "distrchan": distrchan,
        "source": "heureka",
        "timestamp": datetime.utcnow().strftime("%Y%m%d"),
    }
    add_dict = {v: add_fields[k] for k, v in mapping.items() if k in add_fields.keys()}
    with open(filename, "rt") as infile:
        dict_reader = DictReader(infile, delimiter=",")
        for line in dict_reader:
            outrow = {v: line[k] for k, v in mapping.items() if k in line.keys()}
            yield {**outrow, **add_dict}


def write_response_to_csv(in_filename, mapping, out_filename, country, distrchan):
    with open(out_filename, "wt") as outfile:
        dict_writer = DictWriter(
            outfile, fieldnames=list(mapping.values()), extrasaction="ignore"
        )
        dict_writer.writeheader()
        for line in get_formatted_dicts_from_csv(
            in_filename, mapping, country, distrchan
        ):
            dict_writer.writerow(line)


def main():
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

    datadir = os.getenv("KBC_DATADIR", "/data/")
    conf = docker.Config(datadir)

    params = conf.get_parameters()
    logger.info("Extracted parameters.")
    login_url = params.get("login_url")
    login_user = params.get("login_user")
    login_pass = params.get("#login_pass")
    country = params.get("country")
    distrchan = params.get("distrchan")
    output_files_settings = params.get("output_files_settings")

    url_report = f"https://sluzby.heureka.{country.lower()}/obchody/sortiment-report/"

    payload = {
        "_do": "loginForm-loginForm-submit",
        "email": login_user,
        "password": login_pass,
    }

    with requests.Session() as session:
        retries = Retry(
            total=3, backoff_factor=0.3, status_forcelist=(500, 501, 502, 503, 504)
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))
        session.mount("http://", HTTPAdapter(max_retries=retries))
        login_response = session.post(login_url, data=payload, timeout=5)
        if login_response.status_code // 100 != 2:
            logger.error("Failed to log in.")
        report_response = session.get(url_report)
        if report_response.status_code // 100 != 2:
            logger.error("Failed to get to report page.")
        soup = BeautifulSoup(report_response.text, "html.parser")
        text = soup.find_all("td")[3]
        logger.info("Checking if report is generating.")
        current_report_is_generating = check_report_generation(text)
        if current_report_is_generating is False:
            logger.info("Getting url for new report.")
            report_dict = extract_report_url_dict(text)
            current_report_url = f"{url_report}?s={report_dict['s']}&d={report_dict['d']}&l={report_dict['l']}"
            report_generation_request = session.get(current_report_url)
            if report_generation_request.status_code // 100 != 2:
                logger.error("Failed to generate new report.")
        report_page = session.get(url_report)
        if report_page.status_code // 100 != 2:
            logger.error("Failed to get to report page.")
        report_soup = BeautifulSoup(report_page.text, "html.parser")
        logger.info("Getting last available report url.")
        last_report_url = get_first_csv_link(report_soup)
        logger.info("Downloading last available report.")
        tempfile_name = f"{datadir}in/tables/temp.csv"
        with session.get(last_report_url, stream=True) as r:
            r.raise_for_status()
            with open(tempfile_name, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

    logger.info("Writing csvs.")
    for filename, columns_mapping in output_files_settings.items():
        logger.info(f"Processing file: {filename}")
        write_response_to_csv(
            in_filename=tempfile_name,
            mapping=columns_mapping,
            out_filename=f"{datadir}out/tables/{filename}",
            country=country,
            distrchan=distrchan,
        )

    logger.info("Done.")


if __name__ == "__main__":
    main()
