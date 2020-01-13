import datetime
import logging
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup

logging.basicConfig(filename="./test.log", filemode="a", format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
                    datefmt="%d-%m-%Y %H:%M:%S", level=logging.DEBUG)
logger = logging.getLogger(__name__)


class ContentDownloader:
    def __init__(self, url):
        self.url = url

    def download(self):
        """
        Using requests to download the html content from given URL
        :return: HTML content
        """

        headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'
        }

        s = requests.session()
        r = s.request(method='get', url=self.url, headers=headers)
        if r.status_code == 200:
            logger.info('\nDownload content from Wikipedia:%s' % self.url)
            return r.content
        logger.debug('\nCan not download content from:%s. Maybe check your network.' % self.url)
        return None


class ContentParser:
    def __init__(self, content):
        self.content = content

    def parserHTML(self):
        # Using Beautifulsoup to parse html content and pandas to parse table data
        if content is None:
            logger.warning('\nNothing to parse.')
            return None
        soup = BeautifulSoup(self.content)
        table = soup.find('div', id="mw-content-text").find('div').find_all('table')[3]
        tableData = pd.read_html(str(table))[0]
        tableData.columns = ['date', 'a', 'b', 'c', 'd', 'e', 'result']
        tableData = pd.DataFrame(tableData, columns=['date', 'result'])[2:]
        logger.info('\nContent from Wiki has been parsed to DataFrame.')
        return tableData


def processData(data):
    """
    Filter the valid data(operational/successful/en route)
    :param data: dataframe
    :return: dict which contains date and value. e.g. {"1 March" : 1}
    """
    map = {}
    currentDate = ""
    if data is None:
        logger.warning('\nNothing to process.')
        return map
    for index, row in data.iterrows():
        if 'operational' in str(row["result"]).lower() or 'successful' in str(
                row["result"]).lower() or 'en route' in str(row["result"]).lower():
            date = row["date"]
            if date == currentDate:
                continue
            else:
                currentDate = date
                pattern = r"\d*\s(January|February|March|April|May|June|July|August|September|October|November|December)"
                cleanedDate = re.search(pattern, currentDate)[0]
                map[cleanedDate] = map.setdefault(cleanedDate, 0) + 1
    logger.info('\nData has been cleaned and processed.')
    return map


def outputData(map):
    """
    print all the data to a csv file
    :param map:
    :return: output.csv
    """
    with open('./output.csv', 'w') as output:
        output.write("date, value\n")
        begin = datetime.datetime(2019, 1, 1)
        end = datetime.datetime(2019, 12, 31)
        d = begin
        delta = datetime.timedelta(days=1)
        while d <= end:
            output.write(d.replace(tzinfo=datetime.timezone.utc).isoformat() + ', ')
            output.write(str(map.setdefault(d.strftime("%-d %B"), 0)))
            output.write('\n')

            d += delta
    logger.info('\nThe data has been written into "output.csv".')


if __name__ == "__main__":
    url = "https://en.wikipedia.org/wiki/2019_in_spaceflight#Orbital_launches"
    content = ContentDownloader(url=url).download()
    tableData = ContentParser(content).parserHTML()
    map = processData(tableData)
    outputData(map)
