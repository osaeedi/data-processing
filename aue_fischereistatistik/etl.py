import pandas as pd
import geopandas as gpd
from aue_fischereistatistik import credentials
import locale
from datetime import datetime


# datetime in German
# MAC:
locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8')
# Windows:
# locale.setlocale(
#     category=locale.LC_ALL,
#     locale="German"  # Note: do not use "de_DE" as it doesn't work
# )

columns = ['Fischereikarte', 'Fangbüchlein_retourniert', 'Datum', 'Monat', 'Jahr', 'Gewässercode', 'Fischart', 'Gewicht',
           'Länge', 'Nasenfänge', 'Kesslergrundel', 'Schwarzmundgrundel', 'Nackthalsgrundel',
           'Abfluss_Rhein_über_1800m3']

df = pd.DataFrame(columns=columns)


for year in range(2010, 2021):
    year = str(year)
    path = f'{credentials.path_csv}/fangstatistik_{year}.csv'
    df_year = pd.read_csv(path, encoding='utf-8', keep_default_na=False)
    df_year['Jahr'] = year
    # replace '0' with empty string
    # Question: In Gewässercode: 0=unbekannt?
    df_year[['Datum', 'Monat', 'Fischart',  'Gewicht',
             'Länge','Abfluss_Rhein_über_1800m3']]\
        =  df_year[['Datum', 'Monat', 'Fischart',  'Gewicht',
                    'Länge','Abfluss_Rhein_über_1800m3']].replace('0','')

    # make month column complete/in same format and add day column
    if (df_year['Monat'] == '').all():
        # remove empty space from datum column
        df_year['Datum'] = df_year['Datum'].str.strip()
        # remove point/komma at end of entries in Datum column if it's there
        df_year['Datum'] = df_year['Datum'].apply(lambda x: x[:-1] if (x != '' and (x[-1] == '.' or x[-1] ==',')) else x)
        df_year['Monat'] = pd.to_datetime(df_year['Datum'], format='%d.%m', errors='coerce').dt.strftime('%m')
        # add day column
        df_year['Tag'] = pd.to_datetime(df_year['Datum'], format='%d.%m', errors='coerce').dt.strftime('%d')
    else:
        # Complete month column all in same format
        # need to correct in month column: 'juli' 'juö' 'ap' '3' 'mai' '0' ''
        df_year['Monat'].replace('juli', 'Juli', inplace=True)
        df_year['Monat'].replace('ap', 'April', inplace=True)
        df_year['Monat'].replace('mai', 'Mai', inplace=True)
        df_year['Monat'].replace('3', 'März', inplace=True)
        df_year['Monat'].replace('juö', 'Juli', inplace=True)
        # change month names to zero-padded decimal numbers
        df_year['Monat'] = df_year['Monat'].apply(
            lambda x: datetime.strptime(x, '%B') if type(x) == str and x != '' else pd.NaT)
        df_year['Monat'] = df_year['Monat'].dt.strftime('%m')
        # add day column
        if year == '2012':
            df_year['Tag'] = pd.to_datetime(df_year['Datum'], format='%d.%m.', errors='coerce').dt.strftime('%d')
        else:
            df_year['Tag'] = df_year['Datum']
    df = pd.concat([df, df_year])

# make date column
cols=["Jahr", "Monat", "Tag"]
df['Datum'] = df[cols].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns")

# Add year to month column
df['Monat'] = df['Monat'] + ' ' + df['Jahr']

# correct date
df['Datum'].replace('2020-09-31', '2020-09-30', inplace=True)

# put date column in correct datetime format (thereby removing incomplete dates)
df['Datum'] = pd.to_datetime(df['Datum'], format = '%Y-%m-%d', errors='coerce')

# add column Gewässer
dict_gew =  {   '0' : '-',
                '1' : 'Rhein - Basel-Stadt',
                '2' : 'Rhein - Basel-Stadt',
                '3' : 'Wiese - Pachtstrecke Stadt Basel',
                '4' : 'Birs - Pachtstrecke Stadt Basel',
                '5' : 'Neuer Teich / Mühleteich - Pachtstrecke Riehen',
                '6' : 'Wiese - Pachtstrecke Riehen',
                '7' : 'Wiese - Pachstrecke Riehen'
}

df['Gewässer'] = df['Gewässercode'].map(dict_gew)

# remove "unbekannt" in column Länge
df['Länge'].replace('unbekannt', '', inplace=True)

# correct typo in 'Gewicht' column
df['Gewicht'].replace('1.1.', '1.1', inplace=True)

# filter columns for export
df = df[['Jahr', 'Monat', 'Fischereikarte', 'Gewässer', 'Fischart', 'Gewicht',
           'Länge','Kesslergrundel', 'Schwarzmundgrundel']]

# force some columns to be of integer type
df['Kesslergrundel'] = pd.to_numeric(df['Kesslergrundel'], errors='coerce').astype('Int64')
df['Schwarzmundgrundel'] = pd.to_numeric(df['Schwarzmundgrundel'], errors='coerce').astype('Int64')

# make new column with total grundel
df['Grundel Total'] = df['Kesslergrundel'] + df['Schwarzmundgrundel']

# filter empty rows: remove all rows that have no entry for Fischart or Grundeln
condition = ~((df['Fischart'] == '') & (df['Grundel Total'] == 0))
df = df[condition]


# Correct spelling fish names
df['Fischart'].replace('Bach/Flussforelle', 'Bach-/Flussforelle', inplace=True)
df['Fischart'].replace('Bach-/ Flussforelle', 'Bach-/Flussforelle', inplace=True)
df['Fischart'].replace('Barbe ', 'Barbe', inplace=True)
df['Fischart'].replace('Barsch (Egli)', 'Egli', inplace=True)
df['Fischart'].replace('Aesche', 'Äsche', inplace=True)
df['Fischart'].replace('Barsch', 'Egli', inplace=True)


# Names Fischereikarte as in the Fischereiverordnung
df['Fischereikarte'] = df['Fischereikarte'].str.replace(' R$', ' Rhein', regex=True)
df['Fischereikarte'] = df['Fischereikarte'].str.replace(' W$', ' Wiese', regex=True)
df['Fischereikarte'] = df['Fischereikarte'].str.replace(' B$', ' Birs', regex=True)
df['Fischereikarte'] = df['Fischereikarte'].str.replace('Fischerkarte', 'Fischereikarte')
df['Fischereikarte'] = df['Fischereikarte'].str.replace('Jahreskarte', 'Fischereikarte')

dict_karten = {'unbekannt': 'Fischereikarte Rhein', 'Fischereikarte der Gemeinde Riehen': 'Fischereikarte Wiese',
               'Fischereikarte Wiese, Fischereikarte der Gemeinde Riehen': 'Fischereikarte Wiese',
               'Fischereikarte der Gemeinde Riehen': 'Fischereikarte Wiese',
               'Fischereikarte Riehen': 'Fischereikarte Wiese',
               'Galgenkarte': ' Galgenkarte Rhein',
               'Jugendfischerkarte Rhein': 'Jugendfischereikarte Rhein',
               'Jugendfischerkarte': 'Jugendfischereikarte Rhein',
               'Jugendliche Rhein': 'Jugendfischereikarte Rhein',
               'Fischereikarte E': 'Fischereikarte Rhein'
               }

df['Fischereikarte'].replace(dict_karten, inplace=True)


# Add geometry
df_geom = gpd.read_file("gewaesser_adapted.geojson")

gdf = df_geom.merge(df, on='Gewässer')

# export csv file
gdf.to_file(f'{credentials.base_path_local}/fangstatistik.geojson', index=False)
