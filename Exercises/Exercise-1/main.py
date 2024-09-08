import requests
import os
import zipfile
import asyncio
import aiohttp

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def main():
    # your code here
    x = os.getcwd()+"\\Exercises\\Exercise-1"
    if not os.path.exists(rf"{x}\downloads"):
        os.mkdir(rf"{x}\downloads")
    os.chdir(rf"{x}\downloads")
    for i in download_uris:
        valid = False
        fn = i.split('/')[len(i.split('/')) - 1]
        sc = requests.head(i, allow_redirects=True).status_code
        if  sc == 200:
            print(f'{fn} status code 200. Now downloading.')
            resp = requests.get(i)
            with open(fn, 'wb') as file:
                file.write(resp.content)
            print(f'{fn} download complete. Now unzipping.')
            with zipfile.ZipFile(fn,'r') as zf:
                zf.extractall()
            print(f'{fn} unzipped. Now deleting zip file.')
            os.remove(fn)
        else:
            print(f'{fn} status code: {sc}')
            pass
    pass

async def amain():
    #asyncio + aiohttp version
    x = os.getcwd()+"\\Exercises\\Exercise-1"
    if not os.path.exists(rf"{x}\downloads"):
        os.mkdir(rf"{x}\downloads")
    os.chdir(rf"{x}\downloads")
    for i in download_uris:
        fn = i.split('/')[len(i.split('/')) - 1]
        async with aiohttp.ClientSession() as session:
            async with session.get(i) as aresp:
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

if __name__ == "__main__":
    #main()
    asyncio.run(amain())
