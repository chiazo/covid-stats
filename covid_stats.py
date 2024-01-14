from cities.city import City
import matplotlib.pyplot as plt
import pandas as pd

def main():
    df = City("new_york").get_df().sort_values(["sample_date", "site_name"])
    
    site_date_df = df.groupby([df["sample_date"].dt.date, df["site_name"]])[["copies", "copies_avg_flowrate"]].mean()
    site_df = df.groupby([df["site_name"]])[["copies", "copies_avg_flowrate"]].mean()
    date_df = df.groupby([df["sample_date"].dt.date])[["copies", "copies_avg_flowrate"]].mean()
    
    print(site_date_df.max())

    # print results to local csv
    site_date_df.to_csv("site_date.csv", sep='\t', encoding='utf-8')
    site_df.to_csv("site.csv", sep='\t', encoding='utf-8')
    date_df.to_csv("date.csv", sep='\t', encoding='utf-8')


if __name__ == "__main__":
    main()
