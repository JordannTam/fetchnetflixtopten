import re
from bs4 import BeautifulSoup
import requests
import webbrowser



url = "https://www.netflix.com/tudum/top10/"

countriesCode = {
    'South Korea': 'south-korea',
    'Hong Kong': 'hong-kong',
    'Taiwan': 'taiwan',
    'Japan': 'japan',
    'Thailand': 'thailand',
    'Vietnam': 'vietnam',
    'Philippines': 'philippines',
    'Indonesia': 'indonesia',
    'United States': 'united-states',
    'Canada': 'canada',
    'Brazil': 'brazil',
    'Mexico': 'mexico',
    'United Kingdom': 'united-kingdom',
    'Germany': 'germany',
    'France': 'france',
    'Spain': 'spain',
    'Italy': 'italy',
    'Australia': 'australia'
}
# parameter : country name
# type_: leave if blank if you are searching films, 'tv' if you are searching for tv shows
def fetchNetflixTopTenByCountry(country='', type_='', week=''):
    if week != '':
        week = f'?week={week}'
    if type_ != '':
        type_ = f'/{type_}'
    if country != '':
        country = countriesCode[country]
    res = requests.get(url + country + type_ + week)
    if res.status_code != 200:
        print("Failed to retrieve the page")
        return None

    soup = BeautifulSoup(res.text, features="lxml")
    return getDetails(soup)

# parameter : soup
# return : [(rank, name, weeks on Top 10)]
def getDetails(soup):
    tableSoup = soup.find('tbody', class_='css-1rheyty ehtxqvx0')
    rankList = list(map(lambda td: td.span.string, tableSoup.find_all('td', class_='title')))
    nameList = list(map(lambda td: td.button.string, tableSoup.find_all('td', class_='title')))
    weekOnTopList = list(map(lambda tbody: tbody.find_all('td')[1].string, tableSoup))
    zipList = zip(rankList, nameList, weekOnTopList)
    return list(zipList)


if __name__ == '__main__':
    # Searching for global top 10 films
    print(fetchNetflixTopTenByCountry())
    # Searching for this week top 10 films
    print(fetchNetflixTopTenByCountry('Hong Kong'))
    # Searching for week 2024-09-08 top 10 tv
    print(fetchNetflixTopTenByCountry('Taiwan', type_='tv', week='2024-09-08'))
    # webbrowser.open(url)


