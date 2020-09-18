import SharesMagScraperCommon as WSC
import requests
from bs4 import BeautifulSoup
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()


def marketNews(URL):
    rarr = []
    keys = ['date', 'newsUrl', 'newsTitle', 'src']
    include_cols = "2,3,4"
    rarr.append(keys)
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")
    mt = soup.find('table', class_='table-1')
    for t in mt.find_all("tr"):
        values = []
        elems = t.children
        i = 0
        for e in elems:
            if (e.name == 'td'):
                i += 1
                if (include_cols.find(str(i))):
                    link = e.find('a', None)
                    if (link is not None):
                        link = link.get('href', None)
                    if (link is not None):
                        # sentiment = marketNewsSentiment(link)
                        values.append(link)
                    values.append(e.text.strip(' \n\t'))
        values[0] = WSC.standardizeDate(values[0])
        rarr.append(values)
    return rarr


def marketNewsSentiment(newsItemURL: str):
    page = requests.get(newsItemURL)
    soup = BeautifulSoup(page.content, "html.parser")
    mt = soup.find('pre')
    text = ''
    if (mt is not None):
        text = mt.contents

    return analyzer.polarity_scores(text)


if __name__ == "__main__":
    # Market News page
    URL = "https://www.sharesmagazine.co.uk/shares/share/7DIG/news/market"
    marketNewsList = marketNews(URL)
    print(marketNewsList)
