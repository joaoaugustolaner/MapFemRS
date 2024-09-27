import os, certifi, httpx, wget
import bs4

from bs4 import BeautifulSoup as bs
from pathlib import Path

class Scrapper:
    """This class is responsible for accessing the provided URL and scrapping the data within ssp.rs.gov.br"""

    url = "https://ssp.rs.gov.br/indicadores-da-violencia-contra-a-mulher"
    base_url = "https://ssp.rs.gov.br/"

    def get_html(self) -> str:
        try:
            response = httpx.get(self.url)
            return response.text
        except httpx.ConnectError: 
            print("SSL Connection error: Downloading necessary certificates...\n\n")
            os.system("wget https://letsencrypt.org/certs/2024/e6.pem")
            print("\n Downloaded finished. Appending file...\n\n")
            os.system(f'cat e6.pem >> {certifi.where()}')
            print("File appended. Trying Again... \n\n\n")
            os.system("rm -r e6.pem")

        except httpx.ReadTimeout:
            print("Reading error: Time out\n Trying again...\n")
            os.system("sleep 1")

        finally:
            response = httpx.get(self.url)
            return response.text

    
    def get_anchors(self, html_page:str):
        soup = bs(html_page)
        anchor_elements = soup.select("div.artigo__texto a")
        return anchor_elements

    def assemble_links(self, anchors: bs4.element.ResultSet) -> dict[int, str]:
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
                path_to_file = self.base_url + link["href"]
                year = link.contents[2][-5:]
                links[int(year)] = path_to_file

        return links
    

    def download_files(self, links: dict[int, str]):
        """Download files from curated links in previous step. Files will always be saved under $HOME/.mapfem/data/raw"""
    
        #try to create directory
        path_to_files = Path(f'{Path.home()}/.mapfem/data/raw') 
        
        if Path.is_dir(Path(path_to_files)):
            print("PATH ALREADY CREATED!\n")
        else:
            Path.mkdir(path_to_files, parents=True)
            print(f'{path_to_files} CREATED!\n')
            os.system('clear')

        print("BEGGINING DONWLOAD.\n")
        
        for item in links.items():
            file_url = item[1] #access only links for each item

            print(f'\nReport_{item[0]} saved under {path_to_files}\n')
            wget.download(url=file_url, out=f'{path_to_files}/{item[0]}.xlsx')
            

    def start(self):
        scrapper = Scrapper()
        page = scrapper.get_html()
        anchors = scrapper.get_anchors(page)
        links = scrapper.assemble_links(anchors)
        scrapper.download_files(links)

