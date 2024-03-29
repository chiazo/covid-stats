from cities.city import City
import matplotlib.pyplot as plt
from math import trunc
import schedule
import time
import os
from dotenv import load_dotenv
from notify.DeliveryMethod import DeliveryMethod

def main():
    schedule.every(2).weeks.do(covid_stats)

    while True:
        schedule.run_pending()
        time.sleep(1)


def covid_stats():
    load_dotenv()

    PHONE_NUMBER = os.getenv('PHONE_NUMBER')    
    EMAIL = os.getenv('EMAIL')    
    CARRIER = os.getenv('CARRIER') 

    df = City("new_york").get_df().sort_values(["sample_date", "site_name"])
    
    site_date_df = df.groupby([df["sample_date"].dt.date, df["site_name"]])[["copies_avg_flowrate"]].mean()
    site_df = df.groupby([df["site_name"]])[["copies_avg_flowrate"]].mean()
    date_df = df.groupby([df["sample_date"].dt.date])[["copies_avg_flowrate"]].mean().sort_values(["sample_date"])
    # max_row = df.iloc[site_date_df["copies_avg_flowrate"].argmax()][["sample_date", "site_name", "copies_avg_flowrate"]].tolist()
    # min_row = df.iloc[site_date_df["copies_avg_flowrate"].argmin()][["sample_date", "site_name", "copies_avg_flowrate"]].tolist()

    peak = (date_df.idxmax()[0].strftime("%m/%d/%Y"), date_df.max()[0])
    low = (date_df.idxmin()[0].strftime("%m/%d/%Y"), date_df.min()[0])
    curr = (df["sample_date"].iloc[-1].strftime("%m/%d/%Y"), date_df.iloc[-1][0])

    pct_change_from_high = trunc(((curr[1] - peak[1]) / curr[1]) * 100)
    pct_change_from_low = trunc(((curr[1] - low[1]) / curr[1]) * 100)
    message = '''
    Peak: {0} - avg of {1} copies/ml.
    Low:  {2} - avg of {3} copies/ml.
    --------------------------------------------------------------
    As of {4}, there's been a {5}% decrease since {6} with an avg reading of {7} copies/ml. This is a {8}% increase since the low on {9}.
    '''.format(peak[0], shorten_number(peak[1]), low[0], shorten_number(low[1]), curr[0], pct_change_from_high, peak[0], shorten_number(curr[1]), pct_change_from_low, low[0])
    
    DeliveryMethod(EMAIL, PHONE_NUMBER, CARRIER, message)

    
    print(f"Peak: {peak[0]} - avg of {shorten_number(peak[1])} copies/ml.")
    print(f"Low:  {low[0]} - avg of {shorten_number(low[1])} copies/ml.")
    print("--------------------------------------------------------------")
    print(f"As of {curr[0]}, there's been a {pct_change_from_high}% decrease since {peak[0]} with an avg reading of {shorten_number(curr[1])} copies/ml.")
    print(f"This is a {pct_change_from_low}% increase since the low on {low[0]}.")

    print(message)
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

if __name__ == "__main__":
    main()
