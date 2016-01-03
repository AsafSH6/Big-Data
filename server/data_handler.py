# utf-8
import datetime as dt
import requests as req
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
        # print companies
        return companies

    @threads(10)
    def download_company_stocks_details_between_given_dates(self, company_name):
        r = req.get(url=self.url.format(company_name=company_name))
        # print r.status_code
        self.split_csv_file_by_maximum_lines(company_name, r.content)

    def split_csv_file_by_maximum_lines(self, company_name, content):
        lines = [line for line in content.split('\n')]
        title = lines[0]
        for index in xrange(1, len(lines), self.maximum_lines):
            with open('files/{0}{1}.csv'.format(company_name, index - 1), 'wb') as new_file:
                new_file.write(title + '\n')
                new_file.write('\n'.join(lines[index:index+self.maximum_lines]))

    def download_companies_stocks_as_csv_files(self):
        for company in self.get_list_of_companies():
            self.download_company_stocks_details_between_given_dates(company_name=company)


import time
data_handler = DataHandler(days_from_today=100, number_of_stocks=10)
start_time = time.time()
data_handler.download_companies_stocks_as_csv_files()
print("--- %s seconds ---" % (time.time() - start_time))

