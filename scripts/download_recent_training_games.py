#!/usr/bin/env python

"""
Download recent training data from storage.lczero.org
"""

import argparse
import requests
import re
import tarfile
import os
from datetime import datetime
from bs4 import BeautifulSoup

BASE_URL = 'https://storage.lczero.org/files/training_data/'


def find_latest_training_run_dir():
    """
    return the dir latest training run by scanning the contents on lczero storage
    """
    r = requests.get(BASE_URL)
    soup = BeautifulSoup(r.content, "html.parser")
    potential_dir_rows = soup.find('pre').prettify().split('\n')
    # find directories by filter rows whose URLs don't end in a /
    # excluding the first row, since that's ../
    valid_dir_rows = [row for row in potential_dir_rows[1:] if re.search(r'href=".*/"', row)]
    most_recent_dir = None
    most_recent_dir_date = None
    for dir_row in valid_dir_rows:
        # parse the dir and the time from strings like: '<a href="run3/">run3/</a>       15-Feb-2020 22:18       -\r'
        row_contents = re.search(r'href="(.*)/".*\s+([a-zA-Z\d-]+\s+\d+:\d+)\s+', dir_row)
        dir_name = row_contents[1]
        dir_date_str = row_contents[2]
        dir_date = datetime.strptime(dir_date_str, '%d-%b-%Y %H:%M')
        if most_recent_dir_date is None or dir_date > most_recent_dir_date:
            most_recent_dir_date = dir_date
            most_recent_dir = dir_name
    return most_recent_dir


def download_file(url, output_dir):
    local_filename = os.path.join(output_dir, url.split('/')[-1])
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)
    return local_filename


def download_latest_files(training_run_dir, output_dir, max_files=None, verbose=False):
    """
    Download the latest <max_files> from the training dir into the local output dir
    """
    r = requests.get(BASE_URL + training_run_dir)
    soup = BeautifulSoup(r.content, "html.parser")
    training_file_links = soup.select('pre a')
    if max_files:
        training_file_links = training_file_links[-max_files:]
    for file_link in training_file_links:
        file_url = BASE_URL + training_run_dir + '/' + file_link.get('href')
        if verbose:
            print(f'downloading {file_url}')
        downloaded_file = download_file(file_url, output_dir)
        if verbose:
            print(f'untarring {downloaded_file}')
            tarred_file = tarfile.open(downloaded_file, 'r:*')
            tarred_file.extractall(output_dir)
            tarred_file.close()
        os.unlink(downloaded_file)

def main(argv):
    latest_dir = find_latest_training_run_dir()
    if not os.path.exists(argv.output):
        os.makedirs(argv.output)
    download_latest_files(latest_dir, argv.output, argv.nfiles, argv.verbose)


if __name__ == "__main__":
    argparser = argparse.ArgumentParser(description=\
            'Download the most recent N training files from lczero storage')
    argparser.add_argument('-o', '--output', type=str, help='output directory')
    argparser.add_argument('-v', '--verbose', action='store_true', help='print extra info as the script runs')
    argparser.add_argument(
        '-n',
        '--nfiles',
        type=int,
        help='the max number of files to download. If not provided, then all files from the most recent version dir will be downloaded'
    )

    main(argparser.parse_args())