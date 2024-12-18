import logging
from shutil import copy2
import pandas as pd
import common
from common import change_tracking as ct
from mobilitaet_verkehrszaehldaten import credentials
import sys
import os
import platform
import sqlite3
import pytz
import io

print(f'Python running on the following architecture:')
print(f'{platform.architecture()}')


def parse_truncate(path, filename, dest_path, no_file_cp):
    generated_filenames = []
    path_to_orig_file = os.path.join(path, filename)
    path_to_copied_file = os.path.join(dest_path, filename)
    if no_file_cp is False:
        print(f"Copying file {path_to_orig_file} to {path_to_copied_file}...")
        copy2(path_to_orig_file, path_to_copied_file)
    # Parse, process, truncate and write csv file
    print(f"Reading file {filename}...")
    data = pd.read_csv(path_to_copied_file,
                       engine='python',
                       sep=';',
                       # encoding='ANSI',
                       encoding='cp1252',
                       dtype={'SiteCode': 'category', 'SiteName': 'category', 'DirectionName': 'category',
                              'LaneName': 'category', 'TrafficType': 'category'})
    print(f"Processing {path_to_copied_file}...")
    data['DateTimeFrom'] = pd.to_datetime(data['Date'] + ' ' + data['TimeFrom'], format='%d.%m.%Y %H:%M')
    data['DateTimeTo'] = data['DateTimeFrom'] + pd.Timedelta(hours=1)
    data['Year'] = data['DateTimeFrom'].dt.year
    data['Month'] = data['DateTimeFrom'].dt.month
    data['Week'] = data['DateTimeFrom'].dt.isocalendar().week
    data['Day'] = data['DateTimeFrom'].dt.day
    data['Weekday'] = data['DateTimeFrom'].dt.weekday
    data['HourFrom'] = data['DateTimeFrom'].dt.hour
    data['DayOfYear'] = data['DateTimeFrom'].dt.dayofyear
    print(f'Retrieving Zst_id as the first word in SiteName...')
    data['Zst_id'] = data['SiteName'].str.split().str[0]
    current_filename = os.path.join(dest_path, 'converted_' + filename)
    print(f"Saving {current_filename}...")
    data.to_csv(current_filename, sep=';', encoding='utf-8', index=False)
    generated_filenames.append(current_filename)

    db_filename = os.path.join(dest_path, filename.replace('.csv', '.db'))
    print(f'Saving into sqlite db {db_filename}...')
    conn = sqlite3.connect(db_filename)
    data.to_sql(name=db_filename.split(os.sep)[-1].replace('.db', ''), con=conn, if_exists='replace', index=False)
    common.upload_ftp(db_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, '')

    # group by SiteName, get latest rows (data is already sorted by date and time) so that ODS limit
    # of 250K is not exceeded
    # print("Creating dataset truncated_" + filename + "...")
    # grouped_data = data.groupby('SiteName')
    # sliced_data = grouped_data.tail(249900 / grouped_data.ngroups)
    # print("Saving truncated_" + filename + "...")
    # sliced_data.to_csv('truncated_' + filename, sep=';', encoding='utf-8', index=False)
    # return ['converted_' + filename, 'truncated_' + filename]

    # Only keep latest n years of data
    keep_years = 2
    current_filename = os.path.join(dest_path, 'truncated_' + filename)
    print(f'Creating dataset {current_filename}...')
    latest_year = data['Year'].max()
    years = range(latest_year - keep_years, latest_year + 1)
    print(f'Keeping only data for the following years in the truncated file: {list(years)}...')
    truncated_data = data[data.Year.isin(years)]
    print(f"Saving {current_filename}...")
    truncated_data.to_csv(current_filename, sep=';', encoding='utf-8', index=False)
    generated_filenames.append(current_filename)

    # Create a separate dataset per year
    all_years = data.Year.unique()
    for year in all_years:
        year_data = data[data.Year.eq(year)]
        current_filename = os.path.join(dest_path, str(year) + '_' + filename)
        print(f'Saving {current_filename}...')
        year_data.to_csv(current_filename, sep=';', encoding='utf-8', index=False)
        generated_filenames.append(current_filename)

    # Create a separate dataset per site and traffic type
    all_sites = data.Zst_id.unique()
    for site in all_sites:
        for traffic_type in ['MIV', 'Velo', 'Fussgänger']:
            site_data = data[data.Zst_id.eq(site) & data.TrafficType.eq(traffic_type)]

            if site_data.empty:
                continue

            # Save the original site data
            if traffic_type == 'Fussgänger':
                subfolder = 'Fussgaenger'
            elif filename == 'MIV_Speed.csv':
                subfolder = 'MIV_Speed'
            else:
                subfolder = traffic_type

            # Save the original site data
            current_filename = os.path.join(dest_path, 'sites', subfolder, f'{str(site)}.csv')
            print(f'Saving {current_filename}...')
            site_data.to_csv(current_filename, sep=';', encoding='utf-8', index=False)

            if True or ct.has_changed(current_filename):
                common.upload_ftp(current_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                                  f'verkehrszaehl_dashboard/data/{subfolder}')
                categories = {
                    'MIV_Speed.csv': ['Total', '<20', '20-30', '30-40', '40-50', '50-60', '60-70', '70-80', '80-90',
                                      '90-100', '100-110', '110-120', '120-130', '>130'],
                    'MIV_Class_10_1.csv': ['Total', 'MR', 'PW', 'PW+', 'Lief', 'Lief+', 'Lief+Aufl.', 'LW', 'LW+',
                                           'Sattelzug', 'Bus', 'andere'],
                    'Velo_Fuss_Count.csv': ['Total']}
                site_data['Direction_LaneName'] = site_data['DirectionName'].astype(str) + '#' + site_data['LaneName'].astype(str)
                for category in categories[filename]:
                    # Calculate the total counts per hour for each date, direction, and lane
                    df_to_pivot = site_data[['Date', 'Direction_LaneName', 'HourFrom', category]].copy()
                    df_to_merge = site_data[['Date', 'Weekday']].copy().drop_duplicates()
                    df_agg = df_to_pivot.pivot_table(index=['Date', 'Direction_LaneName'],
                                                     values=category,
                                                     columns='HourFrom',
                                                     aggfunc='sum').reset_index()
                    df_agg = df_agg.merge(df_to_merge, on='Date')
                    df_agg[['DirectionName', 'LaneName']] = df_agg['Direction_LaneName'].str.split('#', expand=True)
                    df_agg = df_agg.drop(columns=['Direction_LaneName'])
                    # Save the hourly data
                    current_filename_hourly = os.path.join(dest_path, 'sites', subfolder,
                                                           f'{str(site)}_{category}_hourly.csv')
                    print(f'Saving {current_filename_hourly}...')
                    df_agg.to_csv(current_filename_hourly, sep=';', encoding='utf-8', index=False)
                    common.upload_ftp(current_filename_hourly, credentials.ftp_server, credentials.ftp_user,
                                      credentials.ftp_pass, f'verkehrszaehl_dashboard/data/{subfolder}')
                    os.remove(current_filename_hourly)

                # Calculate the daily counts per weekday for each week, direction, and lane
                df_to_group = site_data[['Date', 'Direction_LaneName'] + categories[filename]].copy()
                df_to_merge = site_data[['Date', 'Week', 'Weekday', 'Year']].copy().drop_duplicates()
                df_agg = df_to_group.groupby(['Date', 'Direction_LaneName'])[categories[filename]].sum().reset_index()
                df_agg = df_agg[df_agg['Total'] > 0]
                df_agg = df_agg.merge(df_to_merge, on='Date')
                df_agg[['DirectionName', 'LaneName']] = df_agg['Direction_LaneName'].str.split('#', expand=True)
                df_agg = df_agg.drop(columns=['Direction_LaneName'])
                # Save the daily data
                current_filename_weekly = os.path.join(dest_path, 'sites', subfolder, f'{str(site)}_daily.csv')
                print(f'Saving {current_filename_weekly}...')
                df_agg.to_csv(current_filename_weekly, sep=';', encoding='utf-8', index=False)
                common.upload_ftp(current_filename_weekly, credentials.ftp_server, credentials.ftp_user,
                                  credentials.ftp_pass, f'verkehrszaehl_dashboard/data/{subfolder}')
                os.remove(current_filename_weekly)

                # Calculate the average per day for each month, direction, and lane
                df_to_group = site_data[['Year', 'Month', 'Direction_LaneName', 'DateTimeFrom'] + categories[filename]].copy()
                df_agg = df_to_group.groupby(['Year', 'Month', 'Direction_LaneName'])[categories[filename]].sum().reset_index()
                df_agg = df_agg[df_agg['Total'] > 0]
                df_measures = df_to_group.groupby(['Year', 'Month', 'Direction_LaneName'])['DateTimeFrom'].nunique().reset_index()
                df_measures.rename(columns={'DateTimeFrom': 'NumMeasures'}, inplace=True)
                df_measures = df_measures[df_measures['NumMeasures'] > 0]
                df_agg = df_agg.merge(df_measures, on=['Year', 'Month', 'Direction_LaneName'])
                for col in categories[filename]:
                    df_agg[col] = df_agg[col] / df_agg['NumMeasures'] * 24
                df_agg[['DirectionName', 'LaneName']] = df_agg['Direction_LaneName'].str.split('#', expand=True)
                df_agg = df_agg.drop(columns=['Direction_LaneName'])
                # Save the monthly data
                current_filename_monthly = os.path.join(dest_path, 'sites', subfolder,
                                                        f'{str(site)}_monthly.csv')
                print(f'Saving {current_filename_monthly}...')
                df_agg.to_csv(current_filename_monthly, sep=';', encoding='utf-8', index=False)
                common.upload_ftp(current_filename_monthly, credentials.ftp_server, credentials.ftp_user,
                                  credentials.ftp_pass, f'verkehrszaehl_dashboard/data/{subfolder}')
                os.remove(current_filename_monthly)

                # Calculate the average per day for each year, direction, and lane
                df_to_group = site_data[['Year', 'Direction_LaneName', 'DateTimeFrom'] + categories[filename]].copy()
                df_agg = df_to_group.groupby(['Year', 'Direction_LaneName'])[categories[filename]].sum().reset_index()
                df_agg = df_agg[df_agg['Total'] > 0]
                df_measures = df_to_group.groupby(['Year', 'Direction_LaneName'])['DateTimeFrom'].nunique().reset_index()
                df_measures.rename(columns={'DateTimeFrom': 'NumMeasures'}, inplace=True)
                df_measures = df_measures[df_measures['NumMeasures'] > 0]
                df_agg = df_agg.merge(df_measures, on=['Year', 'Direction_LaneName'])
                for col in categories[filename]:
                    df_agg[col] = df_agg[col] / df_agg['NumMeasures'] * 24
                df_agg[['DirectionName', 'LaneName']] = df_agg['Direction_LaneName'].str.split('#', expand=True)
                df_agg = df_agg.drop(columns=['Direction_LaneName'])
                # Save the yearly data
                current_filename_yearly = os.path.join(dest_path, 'sites', subfolder,
                                                       f'{str(site)}_yearly.csv')
                print(f'Saving {current_filename_yearly}...')
                df_agg.to_csv(current_filename_yearly, sep=';', encoding='utf-8', index=False)
                common.upload_ftp(current_filename_yearly, credentials.ftp_server, credentials.ftp_user,
                                  credentials.ftp_pass, f'verkehrszaehl_dashboard/data/{subfolder}')
                os.remove(current_filename_yearly)

                ct.update_hash_file(current_filename)

    # Calculate dtv per ZST and traffic type
    df_locations = download_locations()
    if 'MIV' in filename:
        df_dtv = calculate_dtv_zst_miv(data, df_locations, dest_path, filename)
        current_filename = os.path.join(dest_path, 'dtv_' + filename)
        print(f'Saving {current_filename}...')
        df_dtv.to_csv(current_filename, sep=';', encoding='utf-8', index=False)
        generated_filenames.append(current_filename)
    else:
        df_dtv_velo, df_dtv_fuss = calculate_dtv_zst_velo_fuss(data, df_locations, dest_path, filename)
        current_filename = os.path.join(dest_path, 'dtv_Velo.csv')
        print(f'Saving {current_filename}...')
        df_dtv_velo.to_csv(current_filename, sep=';', encoding='utf-8', index=False)
        generated_filenames.append(current_filename)
        current_filename = os.path.join(dest_path, 'dtv_Fussgaenger.csv')
        print(f'Saving {current_filename}...')
        df_dtv_fuss.to_csv(current_filename, sep=';', encoding='utf-8', index=False)
        generated_filenames.append(current_filename)

    print(f'Created the following files to further processing: {str(generated_filenames)}')
    return generated_filenames


def download_locations():
    url_to_locations = 'https://data.bs.ch/explore/dataset/100038/download/'
    params = {
        'format': 'csv',
        'timezone': 'Europe/Zurich',
        'klasse': 'Dauerzaehlstelle'
    }
    r = common.requests_get(url_to_locations, params=params)
    df_locations = pd.read_csv(io.StringIO(r.text), sep=';', encoding='utf-8')
    # Expand ZWECK to several lines if there is a +
    df_locations['zweck'] = df_locations['zweck'].str.split('+')
    df_locations = df_locations.explode('zweck')
    # Replace Velo/Moto with Velo and Fuss with Fussgänger
    df_locations['zweck'] = df_locations['zweck'].str.replace('Velo/Moto', 'Velo')
    df_locations['zweck'] = df_locations['zweck'].str.replace('Fuss', 'Fussgänger')
    # Save id_zst as string
    df_locations['id_zst'] = df_locations['id_zst'].astype(str)
    return df_locations


def calculate_dtv_zst_miv(df, df_locations, dest_path, filename):
    # For each filename first sum up the daily traffic volume per site and traffic type, then calculate the average
    aggregation_dict = {
        'MIV_Speed.csv': ['Total', '<20', '20-30', '30-40', '40-50', '50-60', '60-70', '70-80', '80-90', '90-100',
                          '100-110', '110-120', '120-130', '>130'],
        'MIV_Class_10_1.csv': ['Total', 'MR', 'PW', 'PW+', 'Lief', 'Lief+', 'Lief+Aufl.', 'LW', 'LW+', 'Sattelzug',
                               'Bus', 'andere']
    }
    if filename in aggregation_dict:
        columns = aggregation_dict[filename]
        df_tv = df.groupby(['Zst_id', 'Date', 'TrafficType'])[columns].sum().reset_index()
        # Remove rows with Total = 0
        df_tv = df_tv[df_tv['Total'] > 0]
        df_dtv = df_tv.groupby(['Zst_id', 'TrafficType'])[columns].mean().reset_index()

        df_count = df.groupby(['Zst_id', 'TrafficType'])['DateTimeFrom'].count().reset_index()
        df_count = df_count[df_count['DateTimeFrom'] > 0]
        df_dtv = df_dtv.merge(df_count, on=['Zst_id', 'TrafficType'], how='left')
        df_dtv.rename(columns={'DateTimeFrom': 'NumMeasures'}, inplace=True)
        # Merge with locations
        df_dtv = df_dtv.merge(df_locations, left_on=['Zst_id', 'TrafficType'], right_on=['id_zst', 'zweck'],
                              how='left').drop(columns=['id_zst', 'zweck'])

        return df_dtv


def calculate_dtv_zst_velo_fuss(df, df_locations, dest_path, filename):
    df_tv = df.groupby(['Zst_id', 'Date', 'TrafficType'])['Total'].sum().reset_index()
    # Remove rows with Total = 0
    df_tv = df_tv[df_tv['Total'] > 0]
    df_dtv = df_tv.groupby(['Zst_id', 'TrafficType'])['Total'].mean().reset_index()
    # Remove rows with NaN-values
    df_dtv = df_dtv.dropna()

    df_count = df.groupby(['Zst_id', 'TrafficType'])['DateTimeFrom'].count().reset_index()
    df_count = df_count[df_count['DateTimeFrom'] > 0]
    df_dtv = df_dtv.merge(df_count, on=['Zst_id', 'TrafficType'], how='left')
    df_dtv.rename(columns={'DateTimeFrom': 'NumMeasures'}, inplace=True)
    # Merge with locations
    df_dtv = df_dtv.merge(df_locations, left_on=['Zst_id', 'TrafficType'], right_on=['id_zst', 'zweck'],
                          how='left').drop(columns=['id_zst', 'zweck'])

    df_dtv_velo = df_dtv[df_dtv['TrafficType'] == 'Velo']
    df_dtv_fuss = df_dtv[df_dtv['TrafficType'] == 'Fussgänger']
    df_dtv_fuss['TrafficType'] = 'Fussgaenger'
    return df_dtv_velo, df_dtv_fuss


def main():
    no_file_copy = False
    if 'no_file_copy' in sys.argv:
        no_file_copy = True
        print('Proceeding without copying files...')

    filename_orig = ['MIV_Class_10_1.csv', 'Velo_Fuss_Count.csv', 'MIV_Speed.csv']

    # Upload processed and truncated data
    for datafile in filename_orig:
        datafile_with_path = os.path.join(credentials.path_orig, datafile)
        if True or ct.has_changed(datafile_with_path):
            file_names = parse_truncate(credentials.path_orig, datafile, credentials.path_dest, no_file_copy)
            if not no_file_copy:
                for file in file_names:
                    common.upload_ftp(file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, '')
                    os.remove(file)
            ct.update_hash_file(datafile_with_path)

    # Upload original unprocessed data
    if not no_file_copy:
        for orig_file in filename_orig:
            path_to_file = os.path.join(credentials.path_dest, orig_file)
            if ct.has_changed(path_to_file):
                common.upload_ftp(path_to_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, '')
                ct.update_hash_file(path_to_file)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
