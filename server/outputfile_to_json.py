import os
import pandas as pd
import json


class JsonCreator(object):
    def __init__(self, stocks_params, input_data_path='files_data/', output_data_path='final/output/part-r-00000'):
        self.stocks_params = stocks_params
        self.input_data_path = input_data_path
        self.output_data_path = output_data_path
        self.csv_df = self.open_all_csvs_into_one_df()

    def open_all_csvs_into_one_df(self):
        df = pd.DataFrame()
        for csv in os.listdir(self.input_data_path):
            with open(self.input_data_path + csv, 'r') as f:
                current_csv_df = pd.read_csv(f, header=None)
                df = pd.concat([df, current_csv_df], ignore_index=True)
        df[0] = df[0].apply(lambda x: x.strip())
        return df

    def get_list_of_companies_in_cluster(self, cluster):
        details = cluster.split('\t')
        cluster_name = details[0]
        companies = details[1].split(',')
        companies =[company.strip() for company in companies]
        return cluster_name, companies

    def get_list_of_name_and_vector_jsons(self, companies):
        list_of_name_and_vector_jsons = list()
        for company in companies:
            company_row = self.csv_df.loc[self.csv_df[0] == company]
            vector = company_row.values.tolist()[0][1:-1]
            len_of_stocks_params = len(self.stocks_params)
            values = [[] for i in range(0, len_of_stocks_params)]
            for index in xrange(0, len(vector), len_of_stocks_params):
                for param in range(0, len_of_stocks_params):
                    values[param].append(vector[index + param])

            list_of_name_and_vector_jsons.append({"name": company,
                                                  "values": values})
        return list_of_name_and_vector_jsons

    def create_json(self):
        output_json = dict()
        with open(self.output_data_path, 'r') as f:
            for line in f:  # for each Kmeans-center
                cluster_name, companies = self.get_list_of_companies_in_cluster(line)
                vector = self.get_list_of_name_and_vector_jsons(companies)
                output_json[cluster_name] = vector

        output_json = [{'name': 'cluster ' + str(index + 1), 'stocks': output_json[cluster]} for index, cluster in enumerate(output_json.keys())]
        return json.dumps({
            'stocks_params': self.stocks_params,
            'clusters': output_json
        })

# print JsonCreator(['Open', 'High', 'Low', 'Close']).create_json()