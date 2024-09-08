import requests
import asyncio
import aiohttp
import pandas
from bs4 import BeautifulSoup

url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
ts = "2022-02-07 14:03" #No longer exists
ts_used = "2024-01-19 10:35"

def main():
    # your code here
    out = BeautifulSoup(requests.get(url).content, 'html.parser').find('table')
    tableheader = [str(i.text).lower() for i in out('th')]
    tablerows = [
        [str(j.text).strip() for j in m] 
        for m 
        in [i('td') for i in out('tr')]
        if len(m) != 0
    ]
    fn = [i for i in tablerows if i[tableheader.index('Last Modified'.lower())] == ts_used][0][0]
    dl_url = url + fn
    resp = requests.get(dl_url)
    with open(fn, 'wb') as file:
        file.write(resp.content)
    data = pandas.read_csv(fn)
    print(data[data['HourlyDryBulbTemperature'] == data['HourlyDryBulbTemperature'].max()])
    pass


if __name__ == "__main__":
    main()
