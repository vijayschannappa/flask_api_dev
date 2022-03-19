import flask
from flask import Flask, render_template, request, Response
import os
import sys
import pandas as pd
import paramiko
import platform
import pymysql
from flask import request, jsonify
from flask import Flask
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime

creds = {
    'host': '****',
    'db': 'DATA_INTEGRATION',
    'user': '****',
    'passwd': '****'
}

if platform.node().startswith("ip-"):
    key_path = (
        "/home/ubuntu/internal_pipelines/ops_api/DIKey.pem"
    )  # Path where key is stored
else:
    key_path = ('/home/reconnect/Keys/DIKey.pem')
user = 'ubuntu'
host = '****'
key = paramiko.RSAKey.from_private_key_file(key_path)

#COMMAND = """sudo /home/ubuntu/.pyenv/versions/data_integration/bin/python /home/ubuntu/external_pipelines/generation_substations/script/controllers/Realtime_Integrator.py"""

COMMAND = """/home/ubuntu/.pyenv/versions/data_integration/bin/python /home/ubuntu/external_pipelines/generation_substations/script/controllers/Realtime_Integrator.py --gchat-update --ss-ids"""

abt_command = """sudo /home/ubuntu/.pyenv/versions/internal_pipelines/bin/python /home/ubuntu/internal_pipelines/lsd_abt_di/abt_data_process.py"""

meter_command = "/home/ubuntu/.pyenv/versions/meter_integration/bin/python /home/ubuntu/external_pipelines/generation_imdas_meters_v2/meter_data_integrator.py"""

turbine_agg_command = """/home/ubuntu/.pyenv/versions/pipelines/bin/python /home/ubuntu/internal_pipelines/aggregation_turbines/turbine_aggregator.py"""

raw_to_pro_command = """/home/ubuntu/.pyenv/versions/internal_pipelines/bin/python /home/ubuntu/internal_pipelines/resampling_substations/pss_raw_to_pro.py"""

ldc_dsm_di_command = """/home/ubuntu/.pyenv/versions/pipelines/bin/python /home/ubuntu/external_pipelines/ldc_dsm_di/dsm_data_di.py"""

fif_min_cm = """/home/ubuntu/.pyenv/versions/data_integration/bin/python /home/ubuntu/CODES/VINOD_CODES/OTHER_SCRIPTS/MIS/15MIN/RUN_DATES.py"""

ten_dif_end = """/home/ubuntu/.pyenv/versions/data_integration/bin/python /home/ubuntu/CODES/VINOD_CODES/OTHER_SCRIPTS/MIS/10MIN/DIFF_CAP/END_TIME/RUN_DATES_END.py"""

ten_dif_st = """/home/ubuntu/.pyenv/versions/data_integration/bin/python /home/ubuntu/CODES/VINOD_CODES/OTHER_SCRIPTS/MIS/10MIN/DIFF_CAP/START_TIME/RUN_DATES_START.py"""

ten_sm_end = """/home/ubuntu/.pyenv/versions/data_integration/bin/python /home/ubuntu/CODES/VINOD_CODES/OTHER_SCRIPTS/MIS/10MIN/SAME_CAP/END_TIME/RUN_DATES_END.py"""

ten_sm_st = """/home/ubuntu/.pyenv/versions/data_integration/bin/python  /home/ubuntu/CODES/VINOD_CODES/OTHER_SCRIPTS/MIS/10MIN/SAME_CAP/START_TIME/RUN_DATES_START.py"""

query_temp = """SELECT * FROM OPS_MIS WHERE SUBSTATION_ID='{}';"""

change_env = """pyenv shell data_integration"""

#reports commands

rj = """/home/ubuntu/.pyenv/versions/gujarat_sldc_report/bin/python3.6 /home/ubuntu/reports/scheduling_report_rajasthan/rajasthan_report.py"""

mp = """/home/ubuntu/.pyenv/versions/gujarat_sldc_report/bin/python3.6 /home/ubuntu/reports/madhya_pradesh/mp_download.py"""

ap = """/home/ubuntu/.pyenv/versions/gujarat_sldc_report/bin/python3.6 /home/ubuntu/reports/andhra_pradesh/ap_report.py"""

gj = """/home/ubuntu/.pyenv/versions/gujarat_sldc_report/bin/python3.6 /home/ubuntu/reports/gujarat/gujarat_report.py"""

inter_state = """/home/ubuntu/.pyenv/versions/gujarat_sldc_report/bin/python3.6 /home/ubuntu/reports/interstate/inter_state.py"""
# raw file download
root_dir = '/home/ubuntu/external_pipelines/generation_substations/data_store'

# DEFINING USERS TO ACCESS API


users = {
    "ops_di": generate_password_hash("reconnect"),
    "algo_di": generate_password_hash("reconnect")
}


def execute_command(command,change_env=None):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=user, pkey=key)
    stdin, stdout, stderr = ssh.exec_command(command)
    stdin.close()
    log = []
    for line in stdout.read().splitlines():
        log.append(line)
    log= str(log)
    ssh.close()
    return stdout


app = flask.Flask(__name__)
auth = HTTPBasicAuth()
app.config["DEBUG"] = True


@auth.verify_password
def verify_password(username, password):
    if username in users:
        return check_password_hash(users.get(username), password)
    else:
        return False


@app.route('/', methods=['GET'])
def home():
    return '''<h1>ON SERVER</h1>
<p>ROOT PAGE ON THE SERVER.</p>'''


@app.route('/run_code/sldc_reports/', methods=['GET'])
@auth.login_required
def generate_reports():
    if 'date' in request.args:
        dt= str(request.args['date'])
    else:
        return 'No date provided, please specify a date'
    if 'state_code' in request.args:
        state = str(request.args['state_code'])
    else:
        return 'no state code provided,please specify a state code'
    if state == 'rj':
        command = rj
    elif state == 'ka':
        command = ka
    elif state == 'mp':
        command = mp
    elif state == 'ap':
        command = ap
    elif state == 'gj':
        command = gj
    elif state == 'inter_state':
        command = inter_state
    cmd = command + " " + dt
    log_book = execute_command(cmd)
    return cmd


@app.route('/run_code/di_code1.0/', methods=['GET'])
@auth.login_required
def fetch_sys_args():
    if 'ss_id' in request.args:
        ss_id = str(request.args['ss_id'])
    else:
        return 'No substation id provided, please specify a substation id'
    if 'num_days' in request.args:
        num_days = str(request.args['num_days'])
    else:
        num_days = str(2)

    command = COMMAND + " " + ss_id + " " + "--last-days" + " " + num_days
    log_book = execute_command(command)
    return command


@app.route('/run_code/di_code2.0/', methods=['GET'])
@auth.login_required
def fetch_di_args():
    if 'ss_id' in request.args:
        ss_id = str(request.args['ss_id'])
    else:
        return 'No substation id provided, please specify a substation id'
    if 'num_days' in request.args:
        num_days = str(request.args['num_days'])
        command = COMMAND + " " + ss_id + " " + "--last-days" + " " + num_days
    elif 'st_date' and 'en_date' in request.args:
        st = str(request.args['st_date'])
        en = str(request.args['en_date'])
        command = COMMAND + " " + ss_id + " " + "--date-range" + " " + st + " " + en
    log_book = execute_command(command)
    return command


@app.route('/run_code/lsd_data_integration/', methods=['GET'])
@auth.login_required
def fetch_arguments():
    if 'date' in request.args:
        date = str(request.args['date'])
        command = abt_command + " " + date
    else:
        command = abt_command
    log_book = execute_command(command)
    return command

@app.route('/run_code/ldc_dsm_di/',methods=['GET'])
@auth.login_required
def fetch_directory_path():
    if 'month' in request.args:
        month = str(request.args['month'])
    if 'state' in request.args:
        state = str(request.args['state'])
    if 'revision'in request.args:
        revision_num = str(request.args['revision'])
    if 'week' in request.args:
        week_num = str(request.args['week'])
    if 'dt_form' in request.args:
        dt_form = str(request.args['dt_form'])
    if 'ssid' in request.args:
        ssid = str(request.args['ssid'])
    if state == 'IN_GJ' or state == 'IN_MH':
        try:
            command = f"{ldc_dsm_di_command} -m {month} -s {state} -r {revision_num} -w WK_{week_num} -d '{dt_form}' -id {ssid}"
        except Exception as e:
            command = f"{ldc_dsm_di_command} -m {month} -s {state} -r {revision_num} -d '{dt_form}' -w WK_{week_num}"
    else:
        try:
            command = f"{ldc_dsm_di_command} -m {month} -s {state} -r {revision_num} -d '{dt_form}' -id {ssid}"
        except Exception as e:
            command = f"{ldc_dsm_di_command} -m {month} -s {state} -r {revision_num} -d '{dt_form}'"

    log_book = execute_command(command)
    return command

@app.route('/run_code/meter_data_integration/', methods=['GET'])
@auth.login_required
def fetch_meter_arguments():
    global meter_command
    if 'meter_ids' in request.args:
        meter_ids = str(request.args["meter_ids"])
        meter_command = meter_command + " " + "-m" + " " + \
            meter_ids + " " + '-t' + " " + "20200127" + " " + "20200211"
    else:
        return "NO METER ID PROVIDED"
    # if 'start_date' in request.args:
    #     start_date = str(request.args['start_date'])
    # else:
    #     return "NO START DATE PROVIDED"
    # if 'end' in request.args:
    #     end_date = str(request.args['end'])
    # else:
    #     return "NO END DATE PROVIDED"
    # meter_command = meter_command + " " + '-t' + " " + "20200127" + " " + "20200211"
    # print(meter_command)
    # return meter_command
    log_book = execute_command(meter_command)
    return meter_command


@app.route('/run_code/turbine_aggregation/', methods=['GET'])
@auth.login_required
def fetch_aggregation_arguments():
    global turbine_agg_command
    if 'ssid' in request.args:
        ss_id = str(request.args['ssid'])
    else:
        return 'PLEASE PROVIDE PROPER SUBSTATION ID'
    if 'st_date' in request.args:
        start = str(request.args['st_date'])
    if 'end_date' in request.args:
        end = str(request.args['end_date'])
    turbine_aggregator_command = turbine_agg_command + " " + "--ss-ids" + \
        " " + ss_id + " " + "-t" + " " + start + " " + end
    # return turbine_aggregator_command
    log_book = execute_command(turbine_aggregator_command)
    return turbine_aggregator_command


@app.route('/run_code/raw_to_pro/', methods=['GET'])
@auth.login_required
def fetch_raw_to_pro_args():
    global raw_to_pro_command
    if 'st_date' in request.args:
        start = str(request.args['st_date'])

    if 'end_date' in request.args:
        end = str(request.args['end_date'])

    raw_to_pro_command = raw_to_pro_command + \
        " " + "--date-range" + " " + start + " " + end
    log_book = execute_command(raw_to_pro_command)
    return "command successfull"


@app.route('/run_code/mis_code/', methods=['GET'])
@auth.login_required
def fetch_ssid_start_end():
    global mis_sql_query
    if 'ssid' in request.args:
        ssid = str(request.args['ssid'])
    if 'st_date' in request.args:
        start = str(request.args['st_date'])
    if 'end_date' in request.args:
        end = str(request.args['end_date'])
    details = execute_query(ssid)
    try:
        ss_id = details[1]
    except Exception as e:
        return ("substation not found")
    freq = details[4]
    capacity = details[3]
    interval = details[2]
    st_details = str(details)
    cmd = select_command(freq, capacity, interval, ss_id, start, end)
    # mis_sql_query = mis_sql_query + " " + ssid + " " + start + " " + end
    log_book = execute_command(cmd,change_env)
    return log_book
    #return cmd

def select_command(freq, cap, interval, ssid, st, end):
    if freq == "15_MIN":
        if cap == 'SAME':
            if interval == 'START':
                command = fif_min_cm + " " + ssid + " " + st + " " + end
                return command
    if freq == '10_MIN':
        command = check_for_cap(cap, interval)
        command = command + " " + ssid + " " + st + " " + end
        return command


def check_for_cap(cap, interval):
    if cap == 'SAME' and interval == 'START':
        command = ten_sm_st
    if cap == 'SAME' and interval == 'END':
        command = ten_sm_end
    if cap == 'DIFF' and interval == 'START':
        command = ten_dif_st
    if cap == 'DIFF' and interval == 'END':
        command = ten_dif_end
    return command


def execute_query(ssid):
    conn = pymysql.connect(**creds)
    cursor = conn.cursor()
    query = query_temp.format(ssid)
    cursor.execute(query)
    details = list(cursor.fetchone())
    cursor.close()
    conn.close()
    print(details)
    return details


def get_req_file(dat, ssid):
    os.chdir(root_dir)
    par_dir_ls = os.listdir(root_dir)
    #print(par_dir_ls)
    directory = get_req_dir(par_dir_ls, ssid)
    print(directory)
    csv_file = get_raw_frame(directory, dat)
    return csv_file, directory


def get_raw_frame(directory, dat):
    os.chdir(directory)
    files = os.listdir()
    #print(files)
    fin_frame = pd.DataFrame()
    dts = ['%Y-%m-%d','%Y/%m/%d','%Y%m%d','%Y_%m_%d','%d-%m-%Y','%d/%m/%Y','%d%m%Y','%d_%m_%Y','%-d-%-m-%Y']
    dt = datetime.strptime(dat,'%Y%m%d')
    dt_forms= [dt.strftime(dt_form) for dt_form in dts]
    for date in dt_forms:
        print(date)
        for file in files:
            if date in file and 'cleaned' not in file:
                print(file)
                try:
                    df = pd.read_csv(file)
                except Exception as e:
                    df = pd.read_excel(file)
                fin_frame = fin_frame.append(df)
            else:
                continue
    return fin_frame


def get_req_dir(dir_ls, ssid):
    for dr in dir_ls:
        if ssid in dr:
            directory = f"{root_dir}/{dr}"
            #print(directory)
            return directory

@app.route('/get_log_file/')
@auth.login_required
def serve_raw_file():
    return render_template('log_file_query.html')

@app.route('/get_log_file/process', methods=['POST'])
def process():
    dat = str(request.form.get('date'))
    sub_id = request.form.get('ssid')
    ss_name = request.form.get('ss_name')
    if dat and sub_id and ss_name:
        par_dir = '/home/ubuntu/external_pipelines/generation_substations/logs'
        dt = datetime.strptime(dat,'%Y%m%d').strftime('%Y_%b')
        req_dir = f"{par_dir}/{dt}"
        os.chdir(req_dir)
        folds = os.listdir()
    else:
        return 'BAD REQUEST'
    for fold in folds:
        if sub_id in fold and ss_name in fold:
            act_dir = f'{req_dir}/{fold}'
            os.chdir(act_dir)
            log_files = os.listdir()
        else:
            continue
    dt_in_log = datetime.strptime(dat,'%Y%m%d').strftime('%Y_%m_%d')
    for file in log_files:
        if dt_in_log in file:
            with open(file,'r') as f:
                return render_template('content.html',text=f.read())
        else:
            continue
    return 'No log file generated for the given date'

@app.route('/get_raw_file/')
@auth.login_required
def serve_raw_file():
    return render_template('ssid_date_query.html')


@app.route('/get_raw_file/process', methods=['POST'])
def process():
    # Retrieve the HTTP POST request parameter value from 'request.form' dictionary
    dat = str(request.form.get('date'))
    sub_id = request.form.get('ssid')
    if dat and sub_id:
        csv_file, directory = get_req_file(dat, sub_id)
    if csv_file.empty:
        return 'NO RAW DATA FOUND FOR THE GIVEN DATE'
    else:
        dir_name = directory.split('/')[-1]
        print(dir_name)
        file_name = f'RAW_{dat}_{dir_name}.csv'
        return Response(csv_file.to_csv(),mimetype="text/csv",headers={"Content-disposition":"attachment; filename=" + file_name})
        

@app.route('/update_wrong_data/',methods=['GET'])
@auth.login_required
def delete_wrong_points():
    old_cmd = """UPDATE {} SET {} = NULL WHERE SUBSTATION_ID = '{}' AND TIME_STAMP BETWEEN '{}' AND '{}';"""
    old_cmd_single = """UPDATE {} SET {} = NULL WHERE SUBSTATION_ID = '{}' AND TIME_STAMP = '{}';"""
    new_cmd = """UPDATE {} SET ATTRIBUTE_1= NULL WHERE SUBSTATION_ID = '{}' AND SOURCE_TAG = '{}' AND TIMESTAMP BETWEEN '{}' AND '{}';"""
    new_cmd_single = """UPDATE {} SET ATTRIBUTE_1= NULL WHERE SUBSTATION_ID = '{}' AND SOURCE_TAG = '{}' AND TIMESTAMP = '{}';"""
   # return request.args
    if 'table' in request.args:
        table = str(request.args['table'])
    if 'col' in request.args:
        col = str(request.args['col'])
    if 'ssid' in request.args:
        ssid = str(request.args['ssid'])
    if 'ts' in request.args:
        ts = str(request.args['ts'])
        if not 'tag' in request.args:
            query = old_cmd_single.format(table,col,ssid,ts)
        if 'tag' in request.args:
            tag = str(rquest.args['tag'])
            query = new_cmd_single.format(table,ssid,tag,ts)
    if  'st' and 'end' in request.args:
        st = str(request.args['st'])
        end = str(request.args['end'])
        if 'tag' in request.args:
            tag = str(request.args['tag'])
            query = new_cmd.format(table,ssid,tag,st,end)
        else:
            query = old_cmd.format(table,col,ssid,st,end)
    update_data(query)
    return query

def update_data(query):
    creds = {
    'host': '54.69.58.20',
    'db': 'RRF_TABLES_TEST',
    'user': 'data_integration',
    'passwd': 'XeMLjXnrBe54XnZZ'}
    conn = pymysql.connect(**creds)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    conn.close()
    

if __name__ == '__main__':
    app.run(host='0.0.0.0')


# http://34.209.114.102:5000/run_code/meter_data_integration/?meter_ids=MTRID00035
# http://34.209.114.102:5000/run_code/turbine_aggregation/?ssid=SS00170&st_date=20200501&end_date=20200508
#http://34.209.114.102:5000/update_wrong_data/?table=DATA_SOLAR_ACTUAL_SUBSTATION&col=POWER_GENERATED&ssid=SS00160&ts=202005011081500
#http://34.209.114.102:5000/run_code/sldc_reports/?date=20210109&state_code=rj

