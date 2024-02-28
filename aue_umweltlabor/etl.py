import logging
import pandas as pd
import os
import common
import common.change_tracking as ct
from aue_umweltlabor import credentials

dtypes = {
    'Probentyp': 'category',
    'Probenahmestelle': 'category',
    'X-Coord': 'category',
    'Y-Coord': 'category',
    'Probenahmedauer': 'category',
    'Reihenfolge': 'category',
    'Gruppe': 'category',
    'Auftragnr': 'category',
    'Probennr': 'category',
    'Resultatnummer': 'string',
    'Automatische Auswertung': 'category'
}


def main():
    datafilename = 'OGD-Daten.CSV'
    datafile_with_path = os.path.join(credentials.path_orig, datafilename)
    if True or ct.has_changed(datafile_with_path):
        logging.info('Reading data file from ' + datafile_with_path + '...')
        data = pd.read_csv(datafile_with_path, sep=';', na_filter=False, encoding='cp1252', dtype=dtypes)

        generated_datasets = split_into_datasets(data)
        gew_rhein_rues_wasser = generated_datasets['gew_rhein_rues_wasser']
        generated_datasets = create_truncated_dataset(gew_rhein_rues_wasser, generated_datasets)
        generated_datasets = create_dataset_for_each_year(gew_rhein_rues_wasser, generated_datasets)

        for dataset_name, dataset in generated_datasets.items():
            current_filename = dataset_name + '.csv'
            logging.info("Exporting dataset to " + current_filename + '...')
            dataset.to_csv(os.path.join(credentials.path_work, current_filename), sep=';', encoding='utf-8',
                           index=False)

        files_to_upload = [name + '.csv' for name in generated_datasets.keys()]
        files_to_upload.append(datafilename)
        for filename in files_to_upload:
            file_path = os.path.join(credentials.path_work, filename)
            if True or ct.has_changed(file_path):
                logging.info('Uploading ' + file_path + ' to FTP...')
                remote_path = ''
                # if filename ends with fourdigits.csv (use regex)
                if filename.endswith('gew_rhein_rues_wasser_\\d{4}.csv'):
                    remote_path = 'gew_rhein_rues_wasser_years'
                common.upload_ftp(file_path, credentials.ftp_server, credentials.ftp_user,
                                  credentials.ftp_pass, remote_path)
                ct.update_hash_file(file_path)

        ct.update_hash_file(datafile_with_path)


def split_into_datasets(data):
    logging.info('Calculating new columns...')
    data.columns = [column.replace(" ", "_") for column in data.columns]
    data['Probenahmedatum_date'] = pd.to_datetime(data['Probenahmedatum'], format='%d.%m.%Y', errors='coerce')
    data['Probenahmejahr'] = data['Probenahmedatum_date'].dt.year
    data.Probenahmejahr = data.Probenahmejahr.fillna(0).astype({'Probenahmejahr': int})
    data['Wert_num'] = pd.to_numeric(data['Wert'], errors='coerce')

    logging.info('Create independent datasets:')
    gew_rhein_rues_fest = data.query('Probenahmestelle == "GEW_RHEIN_RUES" and Probentyp == "FESTSTOFF"')
    gew_rhein_rues_wasser = data.query('Probenahmestelle == "GEW_RHEIN_RUES" and Probentyp == "WASSER"')
    oberflaechengew = data.query('Probentyp == "WASSER" and '
                                 'Probenahmestelle != "GEW_RHEIN_RUES" and '
                                 'Probenahmestelle.str.contains("GEW_")')
    grundwasser = data.query('Probenahmestelle.str.contains("F_")')
    return {'oberflaechengew': oberflaechengew, 'grundwasser': grundwasser, 'gew_rhein_rues_fest': gew_rhein_rues_fest, 'gew_rhein_rues_wasser': gew_rhein_rues_wasser}


def create_truncated_dataset(gew_rhein_rues_wasser, generated_datasets):
    current_filename = 'gew_rhein_rues_wasser_truncated'
    logging.info('Creating dataset ' + current_filename + "...")
    latest_year = gew_rhein_rues_wasser['Probenahmejahr'].max()
    years = [latest_year, latest_year - 1]
    gew_rhein_rues_wasser_truncated = gew_rhein_rues_wasser[gew_rhein_rues_wasser.Probenahmejahr.isin(years)]
    generated_datasets[current_filename] = gew_rhein_rues_wasser_truncated
    return generated_datasets


def create_dataset_for_each_year(gew_rhein_rues_wasser, generated_datasets):
    all_years = gew_rhein_rues_wasser['Probenahmejahr'].unique()
    for year in all_years:
        current_filename = 'gew_rhein_rues_wasser_' + str(year)
        logging.info('Creating dataset ' + current_filename + "...")
        dataset = gew_rhein_rues_wasser[gew_rhein_rues_wasser.Probenahmejahr.eq(year)]
        generated_datasets[current_filename] = dataset
    return generated_datasets


if __name__ == "__main__":
    logging.info(f'Executing {__file__}...')
    logging.basicConfig(level=logging.DEBUG)
    main()
    logging.info('Job successful.')