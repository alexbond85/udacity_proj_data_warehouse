import configparser
import boto3

config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get('AWS', 'key')
SECRET = config.get('AWS', 'secret')


def list_files(bucket_name: str, prefix: str, max_files: int) -> None:
    """
    List files in specific S3 URL

    Parameters
    ----------
    bucket_name : str
        Name of S3 bucket
    prefix : str
        Prefix of S3 bucket
    max_files : int
        Maximum number of files to list
    """
    aws_access_key_id = KEY
    aws_secret_access_key = SECRET

    # Create a session with explicit credentials
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    s3 = session.client('s3')

    # Paginator for handling large lists of objects
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    file_count = 0
    for page in page_iterator:
        for obj in page.get('Contents', []):
            print(obj['Key'])
            file_count += 1
            if file_count >= max_files:
                return


if __name__ == '__main__':
    bucket_name = 'udacity-dend'

    # List 3 files from 'song_data' directory
    print("Listing 3 files from 'song_data':")
    list_files(bucket_name, 'song_data', 3)

    # List 3 files from 'log_data' directory
    print("\nListing 3 files from 'log_data':")
    list_files(bucket_name, 'log_data', 3)
