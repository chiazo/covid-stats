import requests
import json
from datetime import datetime
import pandas as pd


class CityDirectory:
    city_directory = {
        "new_york_city": {
            "endpoint": "https://health.data.ny.gov/resource/hdxs-icuh.json",
            "full_name": "New York City",
            "nickname": "NYC",
            "date_format": "%Y-%m-%dT%H:%M:%S.%f",
            "columns": {
                # date collection occurred
                'samplecollectdate': 'sample_date',
                # date sample was tested
                'testresultdate': 'test_date',
                # wastewater resource recovery facility name of sample collection
                'wwtpname': 'site_name',
                # concentration of SARS-Cov2 per liter
                'flowrate': 'copies',
                # normalized SARS-Cov2 to average flow and population size
                'pcrtargetavgconc': 'copies_avg_flowrate',
                'populationserved': 'population',
            },
            "source_website": "https://health.data.ny.gov/Health/New-York-State-Statewide-COVID-19-Wastewater-Surve/hdxs-icuh/explore/query/SELECT%0A%20%20%60county%60%2C%0A%20%20%60zipcode%60%2C%0A%20%20%60populationserved%60%2C%0A%20%20%60samplelocation%60%2C%0A%20%20%60samplelocationspecify%60%2C%0A%20%20%60epaid%60%2C%0A%20%20%60wwtpname%60%2C%0A%20%20%60capacitymgd%60%2C%0A%20%20%60stormwaterinput%60%2C%0A%20%20%60sampletype%60%2C%0A%20%20%60samplematrix%60%2C%0A%20%20%60pretreatment%60%2C%0A%20%20%60solidseparation%60%2C%0A%20%20%60concentrationmethod%60%2C%0A%20%20%60extractionmethod%60%2C%0A%20%20%60recefftargetname%60%2C%0A%20%20%60receffspikematrix%60%2C%0A%20%20%60receffspikeconc%60%2C%0A%20%20%60pcrtarget%60%2C%0A%20%20%60pcrgenetarget%60%2C%0A%20%20%60pcrgenetargetref%60%2C%0A%20%20%60pcrtype%60%2C%0A%20%20%60lodref%60%2C%0A%20%20%60humfractargetmic%60%2C%0A%20%20%60humfractargetmicref%60%2C%0A%20%20%60quantstantype%60%2C%0A%20%20%60stanref%60%2C%0A%20%20%60inhibitionmethod%60%2C%0A%20%20%60numnotargetcontrol%60%2C%0A%20%20%60samplecollectdate%60%2C%0A%20%20%60flowrate%60%2C%0A%20%20%60sampleid%60%2C%0A%20%20%60labid%60%2C%0A%20%20%60testresultdate%60%2C%0A%20%20%60pcrtargetavgconc%60%2C%0A%20%20%60pcrtargetbelowlod%60%2C%0A%20%20%60lodsewage%60%2C%0A%20%20%60ntcamplify%60%2C%0A%20%20%60receffpercent%60%2C%0A%20%20%60inhibitiondetect%60%2C%0A%20%20%60inhibitionadjust%60%2C%0A%20%20%60humfracmicconc%60%2C%0A%20%20%60qualityflag%60/page/filter"
        },
        "new_york": {
            "endpoint": "https://data.cityofnewyork.us/resource/f7dc-2q9f.json",
            "full_name": "New York",
            "nickname": "NY",
            "date_format": "%Y-%m-%dT%H:%M:%S.%f",
            "columns": {
                # date collection occurred
                'sample_date': 'sample_date',
                # date sample was tested
                'test_date': 'test_date',
                # wastewater resource recovery facility name of sample collection
                'wrrf_name': 'site_name',
                'wrrf_abbreviation': 'site_abbreviation',
                # concentration of SARS-Cov2 per liter
                'copies_l': 'copies',
                # normalized SARS-Cov2 to average flow and population size
                'copies_l_x_average_flowrate': 'copies_avg_flowrate',
                'population_served': 'population',
            },
            "source_website": "https://data.cityofnewyork.us/Health/SARS-CoV-2-concentrations-measured-in-NYC-Wastewat/f7dc-2q9f/data"
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
        input = self.city_directory.get_city_data(self.name)
        endpoint = input.get("endpoint")
        full_name = input.get("full_name")
        nickname = input.get("nickname")

        response = requests.get(endpoint)
        data = response.text
        result = json.loads(data)
        self.data = result
        return self.data

    def get_df(self):
        df = pd.DataFrame(self.data)
        city_inputs = self.city_directory.get_city_data(self.name)

        # make column names more readable
        # cols = dict((v,k) for k,v in city_inputs.get("columns").items()) 
        cols = city_inputs.get("columns")
        df.rename(columns=cols, inplace=True)
        df = df.loc[(df["copies"]!= "NaN") & (df["copies"]!= "NA")]
        df = df.loc[(df["copies_avg_flowrate"]!= "NaN") & (df["copies_avg_flowrate"]!= "NA")]
        df = df.loc[(df["population"]!= "NaN") & (df["population"]!= "NA")]

        # convert strings to ints
        int_cols = ["copies","copies_avg_flowrate", "population"]
        for col in int_cols:
            df[col] = pd.to_numeric(df[col])

        # filter out zeros
        df = df.loc[(df["copies"]!= 0) & (df["copies"]!= 0)]
        df = df.loc[(df["copies_avg_flowrate"]!= 0) & (df["copies_avg_flowrate"]!= 0)]
        df = df.loc[(df["population"]!= 0) & (df["population"]!= 0)]
        
        # turn strings into date objs
        date_format = city_inputs.get("date_format")
        date_cols = [col for col in df.columns if 'date' in col]

        for col in date_cols:
            df[col] = pd.to_datetime(df[col], format=date_format)
        return df
