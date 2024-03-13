from cities.city import City
import matplotlib.pyplot as plt
from math import trunc, nan
import schedule
import time
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
from dotenv import load_dotenv
from notify.DeliveryMethod import DeliveryMethod

def main():
    schedule.every(2).weeks.do(covid_stats)

    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
    covid_stats()


def covid_stats():
    load_dotenv()

    PHONE_NUMBER = os.getenv('PHONE_NUMBER')    
    EMAIL = os.getenv('EMAIL')    
    CARRIER = os.getenv('CARRIER') 

    df = City("new_york_city").get_df().sort_values(["sample_date", "site_name"])
    
    site_date_df = df.groupby([df["sample_date"].dt.date, df["site_name"]])[["copies_avg_flowrate"]].mean()
    site_df = df.groupby([df["site_name"]])[["copies_avg_flowrate"]].mean()
    date_df = df.groupby([df["sample_date"].dt.date])[["copies_avg_flowrate"]].mean().sort_values(["sample_date"])

    past_year = df[pd.to_datetime(df["sample_date"]) >= (datetime.today() - relativedelta(months=6))]
    date_df_recent = df.groupby([past_year["sample_date"].dt.date])[["copies_avg_flowrate"]].mean().sort_values(["sample_date"])


    peak = (date_df.idxmax()[0].strftime("%m/%d/%Y"), date_df.max()[0])
    peak_recent = (date_df_recent.idxmax()[0].strftime("%m/%d/%Y"), date_df_recent.max()[0])
    low = (date_df.idxmin()[0].strftime("%m/%d/%Y"), date_df.min()[0])
    low_recent = (date_df_recent.idxmin()[0].strftime("%m/%d/%Y"), date_df_recent.min()[0])
    curr = (df["sample_date"].iloc[-1].strftime("%m/%d/%Y"), date_df.iloc[-1][0])

    pct_change_from_high = trunc(((curr[1] - peak_recent[1]) / curr[1]) * 100)
    pct_change_from_low = trunc(((curr[1] - low_recent[1]) / curr[1]) * 100)

    message = '''
    Peak: {0} - avg of {1} copies/ml.
    Low:  {2} - avg of {3} copies/ml.
    --------------------------------------------------------------
    As of {4}, there's been a {5}% decrease since {6} with an avg reading of {7} copies/ml. This is a {8}% increase since the low on {9}.
    '''.format(peak_recent[0], shorten_number(peak_recent[1]), low_recent[0], shorten_number(low_recent[1]), curr[0], pct_change_from_high, peak_recent[0], shorten_number(curr[1]), pct_change_from_low, low_recent[0])
    
    DeliveryMethod(EMAIL, PHONE_NUMBER, CARRIER, message)

    
    print(f"Peak: {peak_recent[0]} - avg of {shorten_number(peak_recent[1])} copies/ml.")
    print(f"Low:  {low_recent[0]} - avg of {shorten_number(low_recent[1])} copies/ml.")
    print("--------------------------------------------------------------")
    print(f"As of {curr[0]}, there's been a {pct_change_from_high}% decrease since {peak_recent[0]} with an avg reading of {shorten_number(curr[1])} copies/ml.")
    print(f"This is a {pct_change_from_low}% increase since the low on {low_recent[0]}.")

    # print results to local csv
    site_date_df.to_csv("site_date.csv", sep='\t', encoding='utf-8')
    site_df.to_csv("site.csv", sep='\t', encoding='utf-8')
    date_df.to_csv("date.csv", sep='\t', encoding='utf-8')     

def shorten_number(num):
    num = trunc(num)
    str_num = str(num)
    if len(str_num) > 9 and len(str_num) <= 12:
        return str_num[:-9] + "B"
    elif len(str_num) >= 7 and len(str_num) <= 9:
        return str_num[:-6] + "M"
    else:    
        return str_num

if __name__ == "__main__":
    main()
