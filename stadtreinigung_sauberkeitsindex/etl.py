from datetime import datetime
import os

import pandas as pd

import common
import logging
from stadtreinigung_sauberkeitsindex import credentials
from requests.auth import HTTPBasicAuth
from charset_normalizer import from_path


def main():
    r = common.requests_get(url=credentials.url, auth=HTTPBasicAuth(credentials.user, credentials.pw))
    if len(r.text) == 0:
        logging.error('No data retrieved from API!')
        raise RuntimeError('No data retrieved from API.')
    
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    export_filename = f"{curr_dir}/data/data_{datetime.now().strftime('%Y-%m')}.csv"
    with open(export_filename, 'w') as file:
        file.write(r.text)
    df = add_datenstand(export_filename)
    df.to_csv(export_filename, encoding='cp1252', index=False)
    common.update_ftp_and_odsp(export_filename, 'stadtreinigung/sauberkeitsindex/roh', '100288')


def add_datenstand(path_csv):
    result = from_path(path_csv)
    enc = result.best().encoding
    df = pd.read_csv(path_csv, encoding=enc, sep=';')
    df['datenstand'] = pd.to_datetime(os.path.basename(path_csv).split('_')[1].split('.')[0])
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
