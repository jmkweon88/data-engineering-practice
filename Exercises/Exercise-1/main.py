import requests
import os
import zipfile
import asyncio
import aiohttp
import time
from concurrent.futures import ThreadPoolExecutor

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def series_dl(url):
    #Download loop function
    fn = url.split('/')[len(url.split('/')) - 1]
    sc = requests.head(url, allow_redirects=True).status_code
    if sc == 200:
        print(f'{fn} status code 200. Now downloading.')
        resp = requests.get(url)
        with open(fn, 'wb') as file:
            file.write(resp.content)
        print(f'{fn} download complete. Now unzipping.')
        with zipfile.ZipFile(fn,'r') as zf:
            zf.extractall()
        print(f'{fn} unzipped. Now deleting zip file.')
        os.remove(fn)
    else:
        print(f'{fn} status code: {sc}')

async def fileget(url, session):
    async with session.get(url) as aresp:
        fn = url.split('/')[len(url.split('/')) - 1]
        if aresp.status == 200:
            print(f'{fn} status code 200. Now downloading.')
            with open(fn, 'wb') as file:
                file.write(await aresp.read()) #Remember to use await
            print(f'{fn} download complete. Now unzipping.')
            with zipfile.ZipFile(fn,'r') as zf:
                zf.extractall()
            print(f'{fn} unzipped. Now deleting zip file.')
            os.remove(fn)
        else:
            print(f'{fn} status code: {aresp.status}')
            pass

async def amain():
    #asyncio + aiohttp version
    x = os.getcwd()+r"\Exercises\Exercise-1"
    if not os.path.exists(rf"{x}\downloads"): #r for raw string
        os.mkdir(rf"{x}\downloads")
    os.chdir(rf"{x}\downloads")
    async with aiohttp.ClientSession() as session:
        tl = [fileget(i,session) for i in download_uris]
        await asyncio.gather(*tl) #The asterisk is necessary for list unpacking.
        
def main():
    # your code here
    x = os.getcwd()+r"\Exercises\Exercise-1"
    if not os.path.exists(rf"{x}\downloads"):
        os.mkdir(rf"{x}\downloads")
    os.chdir(rf"{x}\downloads")
    with ThreadPoolExecutor(max_workers = min(10,len(download_uris))) as tpe:
        tpe.map(series_dl,download_uris)

if __name__ == "__main__":
    start_time = time.time()
    #main()
    asyncio.run(amain())
    print(f"Total completion time: {time.time() - start_time} secs")