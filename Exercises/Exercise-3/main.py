import boto3
import botocore
import gzip
import shutil
import os
import sys

def main():
    # your code here
    bucket = 'commoncrawl'
    key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
    fn = 'file.gz'
    s3 = boto3.client(
        's3',
        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    )
    s3.download_file(bucket,key,fn)
    o_n = 'output.txt'
    with gzip.open(fn,'rb') as gz:
        with open(o_n,'wb') as f:
            shutil.copyfileobj(gz,f)
    with open(o_n,'rb') as full_text:
        cf = full_text.read()
        uri = cf.split(b'\n')[0].decode('utf-8')
        ffn = uri.split(r'/')[len(uri.split(r'/')) - 1]
    s3.download_file(bucket,uri,ffn)
    fo_n = 'final_output.txt'
    with gzip.open(ffn,'rb') as fgz:
        with open(fo_n, 'wb') as ff:
            shutil.copyfileobj(fgz,ff)
    with open(fo_n,'rb') as next_text:
        while True: 
            l = next_text.readline().decode('utf-8')
            if not l:
                break
            sys.stdout.write(l.strip())
    pass


if __name__ == "__main__":
    main()
