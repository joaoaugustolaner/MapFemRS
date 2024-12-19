"""
Webscrapper for SSP-RS website

How it works:
    1. It connects to the https in the url variable;
    2. Check if it has the certificate needed (e6.pem);
    3. Parse the HTML of the page and get the link to .xlsx files;
    4. Retrieves the files and store them separated by year;

Author: João Augusto Tonial Laner
Github: https://github.com/joaoaugustolaner
"""

__version__ = "0.1.0"

import os
import logging
import certifi
import httpx
import wget
import bs4
from bs4 import BeautifulSoup as bs
from pathlib import Path

url = "https://ssp.rs.gov.br/indicadores-da-violencia-contra-a-mulher"
base_url = "https://ssp.rs.gov.br/"


def get_html_text() -> str:
    """
    Return the html of the webpage in a text format

    Arguments: None
    Return: str
    """
    try:
        response = httpx.get(url, timeout=None)
        return response.text
    except httpx.ConnectError:
        logging.warn("SSL Connection error: Downloading necessary certificates...\n\n")
        os.system("wget https://letsencrypt.org/certs/2024/e6.pem")
        logging.info("\n Downloaded finished. Appending file...\n\n")
        os.system(f"cat e6.pem >> {certifi.where()}")
        logging.debug("File appended. Trying Again... \n\n\n")
        os.system("rm -r e6.pem")

    except httpx.ReadTimeout:
        logging.error("Connection time out. Trying again...\n")

    finally:
        response = httpx.get(url)
        return response.text


def get_anchors(html_page: str):
    """
    Returns all the anchor elements of a section called 'div.artigo__texto'

    Arguments:
    html_page: a string that represents the html of the webpage in text form

    Returns:
    the anchor elements
    """

    soup = bs(html_page)
    anchor_elements = soup.select("div.artigo__texto a")
    return anchor_elements


def assemble_links(anchors: bs4.element.ResultSet) -> dict[int, str]:
    """
    This functions creates a dictionary with the links obtained from the get_anchors function.
    Each anchor element leads to a report. The key is the year and the value is the link obtained.

    Arguments:
    anchors: a set of links

    Returns:
    A dictionary with the year of the report and the link to it
    """

    year = ""
    path_to_file = ""
    links = dict()

    for link in anchors:
        if "https" in link["href"]:
            path_to_file = link["href"]
            year = link.contents[0][-5:]
            links[int(year)] = path_to_file
            continue

        if len(link.contents) >= 3:
            path_to_file = base_url + link["href"]
            year = link.contents[2][-5:]
            links[int(year)] = path_to_file

    return links


def download_files(links: dict[int, str]):
    """
    This funtion download the files from the links obtained in previous steps.

    Arguments:
    links: a dictionary with the year and the link to the report
    """
    # try to create directory
    path_to_files = Path(f"{Path.cwd()}/raw/")

    if Path.is_dir(Path(path_to_files)):
        logging.warn("PATH ALREADY CREATED!\n")
    else:
        Path.mkdir(path_to_files, parents=True)
        logging.info(f"{path_to_files} CREATED!\n")

    print("BEGGINING DONWLOAD.\n")

    for item in links.items():
        file_url = item[1]  # access only links for each item

        if Path(f"{path_to_files}/{item[0]}.xlsx").exists():
            continue

        wget.download(url=file_url, out=f"{path_to_files}/{item[0]}.xlsx")
        logging.warning(f"\n{item[0]}.xlsx saved under {path_to_files}\n")


if __name__ == "__main__":
    page = get_html_text()
    anchors = get_anchors(page)
    links = assemble_links(anchors)
    download_files(links)
