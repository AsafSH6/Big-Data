from flask import Flask, request
from data_handler import DataHandler
from program_manager import ProgramManager
from outputfile_to_json import JsonCreator

app = Flask(__name__, static_url_path='/static')


@app.route('/')
def index():
    return app.send_static_file('index.html')


@app.route('/analyze', methods=['POST'])
def analyze():
    form = request.get_json()
    if form['debug']:
        print 'DEBUG'
        return JsonCreator(stocks_params=['Open', 'High', 'Low', 'Close'],
                           input_data_path='debug_files_data/',
                           output_data_path='debug_output_files/part-r-00000').create_json()

    print form
    stocks_params = [param for param in form['stocks_params'] if form['stocks_params'][param]]

    ordered_dict = {
        'Open': 1,
        'High': 2,
        'Low': 3,
        'Close': 4
    }
    stocks_params = sorted(stocks_params, key=lambda x: ordered_dict[x])
    print stocks_params

    DataHandler(days_from_today=form['days_from_today'],
                stocks_parameters=stocks_params,
                number_of_stocks=form['number_of_stocks']).download_companies_stocks_as_csv_files()
    ProgramManager().run_final(params=[form['clusters']]).close_program()
    print 'done'
    return JsonCreator(stocks_params=stocks_params).create_json()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
