import json

from flask import Flask, request, redirect
import re

app = Flask(__name__, static_url_path='/static')

@app.route('/')
def index():
    return app.send_static_file('client.html')


@app.route('/analyze', methods=['POST'])
def hello_world():
    # print '\n'.join(dir(request))
    print request.form
    data = dict()
    data['params'] = list()
    for name, value in request.form.iteritems():
        if value != "on":
            data[name] = int(value)
        else:
            data['params'].append(name)
    print data
    return json.dumps(create_json())
    # return redirect('/')

'''
{
    cluster1: [{name: A, values: [1,2,3]}, {name: AA, values: [1,4,3]}],
    cluster2: [{name: B, values: [1,2,3]}],
    cluster3: [{name: C, values: [1,2,3]}],
}
'''


def create_json():
    json= dict()
    json_cluster_to_number = dict()
    with open('r-000000', 'r') as f:
        for line in f:
            params = re.sub(r"[()]", "", line).split('\t')
            cluster = params[0]
            if cluster not in json_cluster_to_number:
                json_cluster_to_number[cluster] = "cluster" + str(len(json))
                json[json_cluster_to_number[cluster]] = list()
            sub_params = params[1].split()
            json[json_cluster_to_number[cluster]].append({
                "name": sub_params[0],
                "values": [float(value) for value in sub_params[1].split(',')]
            })
    return json




if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
