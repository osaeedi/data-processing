"""
check which day it is, differentiate between monday and other weekdays, variables: day

get data of the day at 9:15, and the weekend if it's Monday, variables: day, data

if no data there, send email, check again after 15 minutes and get data, if still no data, give warning

make dataframe with data of the day (+weekend), variables in:day, data ; variables out: day, data frame

Select latest entry of each day, variables in: day, dataframe, variables out: day, data row

Calculate needed numbers: variabels in: day, data row ; variables out: day, new data row

[Betten_frei_Normal, Betten_frei_Normal_COVID, Betten_frei_IMCU, Betten_frei_IPS_ohne_Beatmung,
      Betten_frei_IPS_mit_Beatmung, Betten_frei_ECMO, Betten_belegt_Normal, Betten_belegt_IMCU, Betten_belegt_IPS_ohne_Beatmung,
      Betten_belegt_IPS_mit_Beatmung, Betten_belegt_ECMO] = calculate_numbers(ies_numbers)

enter numbers in CoReport
"""
import pandas as pd
import get_data
#import send_email
import calculation
import datetime
import threading
import credentials
import update_coreport


def retry(date, list_hospitals):
    print("retrying")
    df, still_missing_hospitals = get_df_for_date(date=date, list_hospitals=list_hospitals, weekend=False)
    if df.empty == False:
        update_coreport.write_in_coreport(df)
        filled_hospitals = [x for x in list_hospitals if x not in still_missing_hospitals]
        print("entries in coreport for ", filled_hospitals)
    if still_missing_hospitals is not []:
        print("Still missing: ", still_missing_hospitals)


def all_together(date, list_hospitals):
    if get_data.check_day() == "Monday":
        saturday = date-datetime.timedelta(2)
        df_saturday, missing_saturday = get_df_for_date(date=saturday, list_hospitals=list_hospitals, weekend=True)
        list_hospitals = [x for x in list_hospitals if x not in missing_saturday]
        update_coreport.write_in_coreport(df_saturday, list_hospitals)
        sunday = date - datetime.timedelta(1)
        df_sunday, missing_sunday = get_df_for_date(date=sunday, list_hospitals=list_hospitals, weekend=True)
        list_hospitals = [x for x in list_hospitals if x not in missing_sunday]
        update_coreport.write_in_coreport(df_sunday, list_hospitals)
        df_monday, missing_hospitals = get_df_for_date(date=date, list_hospitals=list_hospitals, weekend=False)
        filled_hospitals = [x for x in list_hospitals if x not in missing_hospitals]
        update_coreport.write_in_coreport(df_monday, filled_hospitals)
        if not not missing_hospitals:
            print("repeat after 15 minutes for ", missing_hospitals)
            threading.Timer(10, function=retry, args=[date, missing_hospitals]).start()
    elif get_data.check_day() == "Other workday":
        df, missing_hospitals = get_df_for_date(date=date, list_hospitals=list_hospitals, weekend=False)
        if df.empty == False:
            filled_hospitals = [x for x in list_hospitals if x not in missing_hospitals]
            update_coreport.write_in_coreport(df, filled_hospitals)
            print("entries in coreport for ", filled_hospitals)
        elif df.empty == True:
            print("dataframe is empty, nothing is entered into coreport")
        if not not missing_hospitals:
            print("repeat after 15 minutes for ", missing_hospitals)
            threading.Timer(10, function=retry, args=[date, missing_hospitals]).start()
    else:
        print("It is weekend")


def get_df_for_date(date, list_hospitals, weekend=False):
    df = pd.DataFrame()
    missing_hospitals = []
    for hospital in list_hospitals:
        result = get_df_for_date_hospital(hospital=hospital, date=date, weekend=weekend)
        if result.empty:
            missing_hospitals.append(hospital)
        else:
            result['Hospital'] = hospital
            df = pd.concat([df, result])
    return df, missing_hospitals


def get_df_for_date_hospital(hospital, date, weekend=False):
    df_entries = get_data.get_dataframe(hospital=hospital, date=date)
    number_of_entries = df_entries.shape[0]
    if number_of_entries == 0:
        if weekend:
            print("Numbers for the weekend day " + str(date) + " are not available!")
            return pd.Dataframe()
        else:
            print("send reminder email for " + hospital)
            return pd.DataFrame()
    elif number_of_entries >= 1:
        df_entry = df_entries[df_entries.CapacTime == df_entries.CapacTime.max()]
        return df_entry

if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    date = datetime.datetime.today().date() #+ datetime.timedelta(1)
    list_hospitals = ['USB', 'Clara', 'UKBB']
    all_together(date=date, list_hospitals=list_hospitals)

