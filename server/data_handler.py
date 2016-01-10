# utf-8
import datetime as dt
import requests as req
import pandas as pd
import io
from tomorrow import threads

YAHOO_FINANCE_API_URL_FORMAT = \
    r'http://ichart.yahoo.com/table.csv?' \
    r's={{company_name}}&' \
    r'a={beginning_month}&' \
    r'b={beginning_day}&' \
    r'c={beginning_year}&' \
    r'd={end_month}&' \
    r'e={end_day}&' \
    r'f={end_year}&' \
    r'g=d&' \
    r'ignore=.csv'


class DataHandler(object):
    def __init__(self, days_from_today, number_of_stocks=None, maximum_lines=10, stocks_file_path='data.txt'):
        self.number_of_stocks = number_of_stocks
        self.maximum_lines = maximum_lines
        self.stocks_file_path = stocks_file_path

        days_from_today += (days_from_today / 7) * 2  # fridays, saturdays
        end_date = dt.date.today()
        beginning_date = end_date - dt.timedelta(days=days_from_today)

        self.url = YAHOO_FINANCE_API_URL_FORMAT.format(
            beginning_month=beginning_date.month - 1,
            beginning_day=beginning_date.day,
            beginning_year=beginning_date.year,
            end_month=end_date.month - 1,
            end_day=end_date.day,
            end_year=end_date.year
        )

    def get_list_of_companies(self):
        companies = []
        with open(self.stocks_file_path) as f:
            for index, line in enumerate(f):
                if index == self.number_of_stocks:
                    break
                else:
                    companies.append(line.split('|')[0])
        return companies

    @threads(10)
    def download_company_stocks_details_between_given_dates(self, companies):
        with open('files/{companies}.csv'.format(companies=', '.join(companies)), 'wb') as f:
            for company_name in companies:
                r = req.get(url=self.url.format(company_name=company_name))
                if r.status_code is 200:
                    self.split_csv_file_by_maximum_lines(f, company_name, r.content)

    def split_csv_file_by_maximum_lines(self, f, company_name, content):
        print company_name
        df = pd.read_csv(io.StringIO(unicode(content)))
        df = df[df.columns[1:-2]]
        # df.to_csv(company_name + '1.csv')
        normalized_df = df.apply(lambda x: (x - pd.np.min(x)) / (pd.np.max(x) - pd.np.min(x)))
        # normalized_df.to_csv(company_name + '2.csv')
        data = io.BytesIO()
        normalized_df.to_csv(data, index=False, header=None)
        f.write(company_name + ';')
        f.write(';'.join(str(data.getvalue()).split('\n')) + '\n')

    def download_companies_stocks_as_csv_files(self):
        companies = self.get_list_of_companies()
        for index in xrange(0, len(companies), self.maximum_lines):
            self.download_company_stocks_details_between_given_dates(companies=companies[index:index+self.maximum_lines])


import time
data_handler = DataHandler(days_from_today=15, number_of_stocks=200)
start_time = time.time()
data_handler.download_companies_stocks_as_csv_files()
print("\n--- %s seconds ---\n" % (time.time() - start_time))

