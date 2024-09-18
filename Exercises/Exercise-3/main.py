import boto3
import io
import gzip
import shutil
import os
import sys
import time



def main():
    # your code here
    #ts = []
    #ts.append(time.time())
    bucket = 'commoncrawl'
    key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
    fn = 'file.gz'
    s3 = boto3.client(
        's3',
        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    )
    s3.download_file(bucket,key,fn)
    #ts.append(time.time())
    o_n = 'output.txt'
    with gzip.open(fn,'rb') as gz:
        with open(o_n,'wb') as f:
            shutil.copyfileobj(gz,f)
    #ts.append(time.time())
    with open(o_n,'rb') as full_text:
        cf = full_text.read()
        uri = cf.split(b'\n')[0].decode('utf-8')
        ffn = uri.split(r'/')[len(uri.split(r'/')) - 1]
    #ts.append(time.time())
    s3.download_file(bucket,uri,ffn)
    #ts.append(time.time())
    fo_n = 'final_output.txt'
    with gzip.open(ffn,'rb') as fgz:
        with open(fo_n, 'wb') as ff:
            shutil.copyfileobj(fgz,ff)
    #ts.append(time.time())
    with open(fo_n,'rb') as next_text:
        while True: 
            l = next_text.readline().decode('utf-8')
            if not l:
                break
            sys.stdout.write(l.strip())
    #ts.append(time.time())
    #for i in range(len(ts) - 1):
        #print(f"{ts[i+1] - ts[i]} secs")

def iomain():
    bucket = 'commoncrawl'
    key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
    fs = io.BytesIO()
    s3 = boto3.client(
        's3',
        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    )
    s3.download_fileobj(bucket,key,fs) #download_file to download as file directly to disk (i.e. the filename to save to must be specified). download_fileobj for file-like object
    fs.seek(0) #You need to seek the beginning of the file-like object before you start the stream
    with gzip.GzipFile(fileobj = fs, mode = 'rb') as igz:
        uri = igz.readline().decode('utf-8').strip()
    fs1 = io.BytesIO()
    s3.download_fileobj(bucket,uri,fs1)
    fs1.seek(0)
    with gzip.GzipFile(fileobj = fs1, mode = 'rb') as ifgz:
        with open('im_output.txt','a', encoding = 'utf-8') as ff1:
            while True:
                l = ifgz.readline().decode('utf-8')
                if not l:
                    break
                sys.stdout.write(l.strip())
                ff1.write(l)

if __name__ == "__main__":
    iomain()

