import requests
import json
from datetime import datetime
import numpy as np
import pandas as pd


class CityDirectory:
    city_directory = {
        "new_york": {
            "endpoint": "https://data.cityofnewyork.us/resource/f7dc-2q9f.json",
            "full_name": "New York City",
            "nickname": "NYC",
            "date_format": "%Y-%m-%dT%H:%M:%S.%f",
            "columns": {
                'sample_date': 'sample_date',
                'test_date': 'test_date',
                'wrrf_name': 'site_name',
                'wrrf_abbreviation': 'site_abbreviation',
                'copies_l': 'copies',
                'copies_l_x_average_flowrate': 'copies_avg_flowrate',
                'population_served': 'population',
            }
        }
    }

    def get_all_data(self):
        return self.city_directory

    def get_city_data(self, name):
        return self.city_directory.get(name)


class City:
    test_data = [{'sample_date': '2023-05-23T00:00:00.000', 'test_date': '2023-05-25T00:00:00.000', 'wrrf_name': 'Red Hook', 'wrrf_abbreviation': 'RH', 'copies_l': '106176', 'copies_l_x_average_flowrate': '41300000', 'population_served': '224029', 'technology': 'dPCR'}, {'sample_date': '2023-05-23T00:00:00.000', 'test_date': '2023-05-25T00:00:00.000', 'wrrf_name': 'Rockaway', 'wrrf_abbreviation': 'RK', 'copies_l': '11808', 'copies_l_x_average_flowrate': '7050000', 'population_served': '120539', 'technology': 'dPCR'}, {'sample_date': '2023-05-23T00:00:00.000', 'test_date': '2023-05-25T00:00:00.000', 'wrrf_name': '26th Ward', 'wrrf_abbreviation': '26W', 'copies_l': '38688', 'copies_l_x_average_flowrate': '22700000', 'population_served': '290608', 'technology': 'dPCR'}, {'sample_date': '2023-05-23T00:00:00.000', 'test_date': '2023-05-25T00:00:00.000', 'wrrf_name': 'Bowery Bay', 'wrrf_abbreviation': 'BB', 'copies_l': '76824', 'copies_l_x_average_flowrate': '27000000', 'population_served': '924695', 'technology': 'dPCR'}, {'sample_date': '2023-05-23T00:00:00.000', 'test_date': '2023-05-25T00:00:00.000', 'wrrf_name': 'Oakwood Beach', 'wrrf_abbreviation': 'OB', 'copies_l': '75384', 'copies_l_x_average_flowrate': '29600000', 'population_served': '258731', 'technology': 'dPCR'}, {'sample_date': '2023-05-23T00:00:00.000', 'test_date': '2023-05-25T00:00:00.000', 'wrrf_name': 'Hunts Point', 'wrrf_abbreviation': 'HP', 'copies_l': '7704', 'copies_l_x_average_flowrate': '3740000', 'annotation': 'This sample was analyzed in duplicate.', 'population_served': '755948', 'technology': 'dPCR'}, {'sample_date': '2023-05-21T00:00:00.000', 'test_date': '2023-05-22T00:00:00.000', 'wrrf_name': 'Wards Island', 'wrrf_abbreviation': 'WI', 'copies_l': '24192', 'copies_l_x_average_flowrate': '14000000', 'population_served': '1201485', 'technology': 'dPCR'}, {'sample_date': '2023-05-21T00:00:00.000', 'test_date': '2023-05-22T00:00:00.000', 'wrrf_name': 'Oakwood Beach', 'wrrf_abbreviation': 'OB', 'copies_l': '64824', 'copies_l_x_average_flowrate': '26700000', 'population_served': '258731', 'technology': 'dPCR'}, {'sample_date': '2023-05-21T00:00:00.000', 'test_date': '2023-05-22T00:00:00.000', 'wrrf_name': 'Owls Head', 'wrrf_abbreviation': 'OH', 'copies_l': '123768', 'copies_l_x_average_flowrate': '42900000', 'population_served': '906442', 'technology': 'dPCR'}, {'sample_date': '2023-05-21T00:00:00.000', 'test_date': '2023-05-22T00:00:00.000', 'wrrf_name': 'Newtown Creek', 'wrrf_abbreviation': 'NC', 'copies_l': '62760', 'copies_l_x_average_flowrate': '37000000', 'annotation': 'This sample was analyzed in duplicate.', 'population_served': '1156473', 'technology': 'dPCR'}, {'sample_date': '2023-05-21T00:00:00.000', 'test_date': '2023-05-22T00:00:00.000', 'wrrf_name': 'North River', 'wrrf_abbreviation': 'NR', 'copies_l': '75288', 'copies_l_x_average_flowrate': '44100000', 'population_served': '658596', 'technology': 'dPCR'}, {'sample_date': '2023-05-21T00:00:00.000', 'test_date': '2023-05-22T00:00:00.000', 'wrrf_name': 'Port Richmond', 'wrrf_abbreviation': 'PR', 'copies_l': '41208', 'copies_l_x_average_flowrate': '17900000', 'population_served': '226167', 'technology': 'dPCR'}, {'sample_date': '2023-05-21T00:00:00.000', 'test_date': '2023-05-22T00:00:00.000', 'wrrf_name': 'Jamaica Bay', 'wrrf_abbreviation': 'JA', 'copies_l': '110520', 'copies_l_x_average_flowrate': '44100000', 'annotation': 'This sample was analyzed in duplicate.', 'population_served': '748737', 'technology': 'dPCR'}, {'sample_date': '2023-05-21T00:00:00.000', 'test_date': '2023-05-22T00:00:00.000', 'wrrf_name': 'Red Hook', 'wrrf_abbreviation': 'RH', 'copies_l': '29208', 'copies_l_x_average_flowrate': '10900000', 'population_served': '224029', 'technology': 'dPCR'},
                 {'sample_date': '2023-05-21T00:00:00.000', 'test_date': '2023-05-22T00:00:00.000', 'wrrf_name': 'Coney Island', 'wrrf_abbreviation': 'CI', 'copies_l': '28032', 'copies_l_x_average_flowrate': '12300000', 'population_served': '682342', 'technology': 'dPCR'}, {'sample_date': '2023-05-21T00:00:00.000', 'test_date': '2023-05-22T00:00:00.000', 'wrrf_name': '26th Ward', 'wrrf_abbreviation': '26W', 'copies_l': '7656', 'copies_l_x_average_flowrate': '6080000', 'population_served': '290608', 'technology': 'dPCR'}, {'sample_date': '2023-05-21T00:00:00.000', 'test_date': '2023-05-22T00:00:00.000', 'wrrf_name': 'Hunts Point', 'wrrf_abbreviation': 'HP', 'copies_l': '14112', 'copies_l_x_average_flowrate': '7630000', 'annotation': 'This sample was analyzed in duplicate.', 'population_served': '755948', 'technology': 'dPCR'}, {'sample_date': '2023-05-21T00:00:00.000', 'test_date': '2023-05-22T00:00:00.000', 'wrrf_name': 'Rockaway', 'wrrf_abbreviation': 'RK', 'copies_l': '13104', 'copies_l_x_average_flowrate': '8640000', 'population_served': '120539', 'technology': 'dPCR'}, {'sample_date': '2023-05-21T00:00:00.000', 'test_date': '2023-05-22T00:00:00.000', 'wrrf_name': 'Bowery Bay', 'wrrf_abbreviation': 'BB', 'copies_l': '90960', 'copies_l_x_average_flowrate': '35000000', 'population_served': '924695', 'technology': 'dPCR'}, {'sample_date': '2023-05-21T00:00:00.000', 'test_date': '2023-05-22T00:00:00.000', 'wrrf_name': 'Tallman Island', 'wrrf_abbreviation': 'TI', 'copies_l': '83016', 'copies_l_x_average_flowrate': '47500000', 'population_served': '449907', 'technology': 'dPCR'}, {'sample_date': '2023-05-16T00:00:00.000', 'test_date': '2023-05-17T00:00:00.000', 'wrrf_name': 'Wards Island', 'wrrf_abbreviation': 'WI', 'copies_l': '117480', 'copies_l_x_average_flowrate': '68500000', 'population_served': '1201485', 'technology': 'dPCR'}, {'sample_date': '2023-05-16T00:00:00.000', 'test_date': '2023-05-17T00:00:00.000', 'wrrf_name': 'Port Richmond', 'wrrf_abbreviation': 'PR', 'copies_l': '47808', 'copies_l_x_average_flowrate': '21600000', 'annotation': 'This sample was analyzed in duplicate.', 'population_served': '226167', 'technology': 'dPCR'}, {'sample_date': '2023-05-16T00:00:00.000', 'test_date': '2023-05-17T00:00:00.000', 'wrrf_name': 'Owls Head', 'wrrf_abbreviation': 'OH', 'copies_l': '96168', 'copies_l_x_average_flowrate': '31700000', 'population_served': '906442', 'technology': 'dPCR'}, {'sample_date': '2023-05-16T00:00:00.000', 'test_date': '2023-05-17T00:00:00.000', 'wrrf_name': 'Red Hook', 'wrrf_abbreviation': 'RH', 'copies_l': '112200', 'copies_l_x_average_flowrate': '56900000', 'population_served': '224029', 'technology': 'dPCR'}, {'sample_date': '2023-05-16T00:00:00.000', 'test_date': '2023-05-17T00:00:00.000', 'wrrf_name': 'Rockaway', 'wrrf_abbreviation': 'RK', 'copies_l': '44616', 'copies_l_x_average_flowrate': '26600000', 'population_served': '120539', 'technology': 'dPCR'}, {'sample_date': '2023-05-16T00:00:00.000', 'test_date': '2023-05-17T00:00:00.000', 'wrrf_name': 'Tallman Island', 'wrrf_abbreviation': 'TI', 'copies_l': '67488', 'copies_l_x_average_flowrate': '31800000', 'population_served': '449907', 'technology': 'dPCR'}, {'sample_date': '2023-05-16T00:00:00.000', 'test_date': '2023-05-17T00:00:00.000', 'wrrf_name': 'Hunts Point', 'wrrf_abbreviation': 'HP', 'copies_l': '16392', 'copies_l_x_average_flowrate': '8450000', 'annotation': 'This sample was analyzed in duplicate.', 'population_served': '755948', 'technology': 'dPCR'}, {'sample_date': '2023-05-16T00:00:00.000', 'test_date': '2023-05-17T00:00:00.000', 'wrrf_name': 'Jamaica Bay', 'wrrf_abbreviation': 'JA', 'copies_l': '89712', 'copies_l_x_average_flowrate': '33600000', 'annotation': 'This sample was analyzed in duplicate.', 'population_served': '748737', 'technology': 'dPCR'}]

    def __init__(self, name) -> None:
        self.name = name
        self.city_directory = CityDirectory()
        self.data = self.get_raw_data()

    def get_raw_data(self):
        # input = self.city_directory.get_city_data(self.name)
        # endpoint = input.get("endpoint")
        # full_name = input.get("full_name")
        # nickname = input.get("nickname")

        # response = requests.get(endpoint)
        # print(f"response status code: {response.status_code}")
        # data = response.text
        # result = json.loads(data)
        # print(f"# of results for {full_name} - {nickname}: {len(result)}")
        self.data = self.test_data
        return self.data

    def get_df(self):
        df = pd.DataFrame(self.data)
        city_inputs = self.city_directory.get_city_data(self.name)

        # make column names more readable
        cols = city_inputs.get("columns")
        df.rename(columns=cols, inplace=True)

        # turn strings into date objs
        date_format = city_inputs.get("date_format")
        date_cols = [col for col in df.columns if 'date' in col]
        print(date_cols)

        for col in date_cols:
            df[col] = pd.to_datetime(df[col], format=date_format)
        return df

    def parse_data(self):
        data = self.data
        for sample in data:
            # '2023-05-21T00:00:00.000'
            sample_date = datetime.strptime(
                sample.get("sample_date"), '%Y-%m-%yT%H:%M:%S.%f')
            test_date = datetime.strptime(
                sample.get("test_date"), '%Y-%m-%yT%H:%M:%S.%f')
            site_name = sample.get("wrrf_name")
            site_abbreviation = sample.get("wrrf_abbreviation")
            copies = int(sample.get("copies_l"))
            copies_avg_flowrate = int(
                sample.get("copies_l_x_average_flowrate"))
            population = sample.get("population_served")
            technology = sample.get("technology")
            print(sample_date, test_date, site_name, site_abbreviation)
            print(copies, copies_avg_flowrate, population, technology)
