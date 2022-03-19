# -*- coding: UTF-8 -*-
"""
hello_jinja2: Get start with Jinja2 templates
"""
from flask import Flask, render_template, request, Response
import os
import sys
import pandas as pd
app = Flask(__name__)
root_dir = '/home/vijay/external_pipelines/generation_substations/data_store'
# root_dir = '/home/ubuntu/external_pipelines/generation_substations/data_store'


def get_req_file(date, ssid):
    os.chdir(root_dir)
    par_dir_ls = os.listdir(root_dir)
    directory = get_req_dir(par_dir_ls, ssid)
    csv_file = get_raw_frame(directory, date)
    return csv_file, directory


def get_raw_frame(directory, date):
    os.chdir(directory)
    files = os.listdir()
    fin_frame = pd.DataFrame()
    for file in files:
        if date in file and 'cleaned' not in file:
            df = pd.read_csv(file)
            fin_frame = fin_frame.append(df)
    return fin_frame


def get_req_dir(dir_ls, ssid):
    for dr in dir_ls:
        if ssid in dr:
            directory = f"{root_dir}/{dr}"
            return directory


@app.route('/')
def main():
    return render_template('j2_query.html')


@app.route('/process', methods=['POST'])
def process():
    # Retrieve the HTTP POST request parameter value from 'request.form' dictionary
    dt = request.form.get('date')
    sub_id = request.form.get('ssid')
    # get(attr) returns None if attr is not present
    # Validate and send response
    if dt and sub_id:
        csv_file, directory = get_req_file(dt, sub_id)
        dir_name = directory.split('/')[-1]
        file_name = f'RAW_{dt}_{dir_name}.csv'
        return Response(csv_file.to_csv(),
                        mimetype="text/csv",
                        headers={"Content-disposition":
                                 "attachment; filename=" + file_name})
        # return render_template('j2_response.html', date=dt)
    else:
        return 'RAW FILES NOT FOUND...', 400  # 400 Bad Request


if __name__ == '__main__':
    app.run(debug=True)
