from flask import Flask, abort, flash, make_response, redirect, request, render_template, Response, session, url_for
from flaskext.markdown import Markdown
from functools import wraps
import json
import jwt
import logging
import os
import pandas as pd
import psycopg2
from psycopg2 import Error
import re
#import requests
import settings
import sys
from titlecase import titlecase

# define our webapp
app = Flask(__name__)
Markdown(app, extensions=['extra'])

#configuration from settings.py
app.secret_key = os.getenv('SESSION_KEY') or settings.SESSION_KEY
app.config['LDP_HOST'] = os.getenv('LDP_HOST') or settings.LDP_HOST
app.config['LDP_PORT'] = os.getenv('LDP_PORT') or settings.LDP_PORT
app.config['LDP_DATABASE'] = os.getenv('LDP_DATABASE') or settings.LDP_DATABASE
app.config['REPORTS_DIR'] = os.getenv('REPORTS_DIR') or settings.REPORTS_DIR
app.config['ORG_NAME'] = os.getenv('ORG_NAME') or settings.ORG_NAME
app.config['ANALYTICS_VERSION'] = os.getenv('ANALYTICS_VERSION') or settings.ANALYTICS_VERSION
print(app.config['LDP_HOST'])

CROSSTAB_LIST = ['consortial_requester.sql', 'consortial_supplier.sql']

# logging
logger = logging.getLogger()
streamHandler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

# login wrapper
def auth_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'username' in session:
            return f(*args, **kwargs)
        elif app.config['ENV'] == 'development':
            session['username'] = 'devuser'
            return f(*args, **kwargs)
        else:
            return redirect(url_for('login'))
    return decorated

# error handlers
@app.errorhandler(403)
def forbidden(e):
    return render_template('403.html'), 403

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

@app.errorhandler(500)
def application_error(e):
    return render_template('500.html'), 500

# set global variable for reports based on git repo
@app.before_first_request
def set_reports():
    global all_reports
    all_reports = _build_reports_index(app.config['REPORTS_DIR'])

# routes and controllers
@app.route('/')
@auth_required
def index():
    reports_list = []
    print("found reports: ")
    for key in all_reports:
        print(key)
        reports_list.append({"name": all_reports[key]['nice_name'], "report": all_reports[key]['report']})
    # sort the list of reports
    reports = sorted(reports_list, key=lambda d: d['name'])
    return render_template("index.html", reports=reports)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        try:
            connection = _postgres_connect(
                app.config['LDP_HOST'],
                app.config['LDP_PORT'],
                username,
                password,
                app.config['LDP_DATABASE']
            )
        except psycopg2.OperationalError:
            abort(403)
        if connection.status == 1:
            authorized = True # can put a function here to authorize
            if authorized:
                session['username'] = username
                session['password'] = password
                _postgres_close_connection(connection)
                return redirect(url_for('index'))
            else:
                return abort(403)        
        else:
            flash('User not found, check username and password')
            return redirect(url_for('login'))
    else:
        return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return render_template('logout.html')

@app.route('/report/<report>')
@auth_required
def report(report):
    current_report = _get_report(report, all_reports)
    try:
        readme = open(current_report['readme'], 'r').read()
    except KeyError:
        readme = None

    queries = []
    for k in current_report['queries']:
        queries.append(k)

    return render_template('report.html', readme=readme, report=current_report, queries=queries)

@app.route('/report/<report>/params/<query>')
@auth_required
def parameterize_query(report, query):
    crosstab = False
    csv = request.args.get("csv", default=False, type=bool)
    if not bool(csv):
        csv = None
    current_report = _get_report(report, all_reports)
    if not current_report['queries'][query]['has_params']:
        return redirect(url_for('execute_query', report=report, query=query, csv=csv))
    else:
        try:
            query_sql = open(current_report['queries'][query]['sql'], 'r').read()
        except:
            abort(404)
        if current_report['queries'][query]['crosstab']:
            crosstab = True
        #return "parameterize {}".format(query)
        return render_template('parameterize.html',
                               report=report,
                               query=query,
                               query_sql=query_sql,
                               csv=csv,
                               crosstab=crosstab)

@app.route('/report/<report>/execute/<query>', methods=['GET', 'POST'])
@auth_required
def execute_query(report, query):
    start_date = None
    end_date = None
    current_report = _get_report(report, all_reports)
    try:
        query_sql = open(current_report['queries'][query]['sql'], 'r').read()
        if request.method == 'POST':
            start_date = request.form['start-date']
            end_date = request.form['end-date']
            crosstab = request.form.get('crosstab')
            query_sql = _sub_dates(query_sql, start_date, end_date)
    except:
        abort(404)
    try: 
        connection = _postgres_connect(
            app.config['LDP_HOST'],
            app.config['LDP_PORT'],
            session['username'],
            session['password'],
            app.config['LDP_DATABASE']
        )
    except psycopg2.OperationalError:
        abort(500)
    csv = request.args.get("csv", default=False, type=bool)
    # handle reports with crosstabs
    if crosstab == "True":
        df = pd.read_sql(query_sql, connection)
        crosstab_result = pd.crosstab(df.requester, df.supplier, df.count_of_requests, aggfunc='sum', margins=True, dropna=False).fillna(0)
        # csv crosstab
        if bool(csv):
            return Response(
                _crosstab_result_to_csv(crosstab_result),
                mimetype="text/csv",
                headers={"Content-disposition": "attachment; filename={}.csv".format(report)})   
        # web display crosstab
        else:
            return render_template('crosstab-results.html',
                                   result=crosstab_result,
                                   report=report,
                                   start_date=start_date,
                                   end_date=end_date,
                                   titlecase=titlecase)
    else:
        result = _postgres_query(connection, query_sql)
    _postgres_close_connection(connection)
    # csv
    if bool(csv):
        return Response(
            _result_to_csv(result),
            mimetype="text/csv",
            headers={"Content-disposition": "attachment; filename={}.csv".format(report)})
    # web display
    else:
        return render_template('results.html', result=result, report=report, start_date=start_date, end_date=end_date)

@app.route('/report/<report>/show/<query>')
@auth_required
def describe_query(report, query):
    current_report = _get_report(report, all_reports)
    print(current_report['queries'][query]['sql'])
    try:
        query_sql = open(current_report['queries'][query]['sql'], 'r').read()
    except:
        abort(404)
    return render_template('show_sql.html', report=current_report, query=query, query_sql=query_sql)

# local functions
def _postgres_connect(host, port, username, password, database):
    try:
        # Connect to an existing database
        connection = psycopg2.connect(user=username,
                                      password=password,
                                      host=host,
                                      port=port,
                                      database=database)
    
    
    except (Exception, Error) as error:
        print(error)
        raise
    return connection

def _postgres_query(connection, query):
    result = {}
    # get a cursor
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            result['column_names'] = [desc[0] for desc in cursor.description]
            result['data'] = cursor.fetchall()
        finally:
            cursor.close()
    except (Exception, Error) as error:
        print(error)
        raise
    return result

def _result_to_csv(result):
    '''Takes the output of _postgres_query'''
    csv_result = ','.join(f'"{column_name}"' for column_name in result['column_names']) + '\n'
    for record in result['data']:
        csv_result += ','.join(f'"{datum}"' for datum in record) + '\n'
    return csv_result

def _crosstab_result_to_csv(crosstab_result):
    '''Takes a pandas dataframe in crosstab form'''
    csv_result = ' ,' + ','.join(f'"{column_name}"' for column_name in crosstab_result.columns) + '\n'
    for index, row in crosstab_result.iterrows():
        #csv_result += ' ,'
        csv_result += titlecase(row.name) + ',' +','.join(f'"{datum}"' for datum in row) + '\n'
    return csv_result

def _postgres_close_connection(connection):
    try:
        return connection.close()
    except (Exception, Error) as error:
        print(error)
        raise

def _build_reports_index(rootdir):
    reports = {}
    for path, subdirs, files in os.walk(rootdir):
        for dir in subdirs:
            # skip requesting_ration DEVOPS-963
            if dir != "requesting_ratio":
                reports[dir] = {"report":dir,"name": dir}
                reports[dir] = {"report":dir,"nice_name": dir.replace('_', ' ')}
                for rpath, rsubdirs, rfiles in os.walk(os.path.join(path, dir)):
                    reports[dir]["queries"] = {}
                    for rname in rfiles:
                        if rname.endswith(".md"):
                            reports[dir]["readme"] = os.path.join(rpath, rname)
                        elif rname.endswith(".sql"):
                            reports[dir]["queries"][rname] = {
                                "sql": os.path.join(rpath, rname),
                                "name": rname,
                                "has_params": _check_report_params(os.path.join(rpath, rname)),
                                "crosstab" : _check_report_crosstab(rname, CROSSTAB_LIST)
                            }
    print(json.dumps(reports, sort_keys=True, indent=4))
    #if app.debug:
    #    print(json.dumps(reports, sort_keys=True, indent=4))
    return reports

def _check_report_params(sql, from_file=True):
    has_params = False
    if from_file:
        query_sql = open(sql, 'r').read()
    else:
        #query sql is string
        query_sql = sql
    if 'WITH parameters AS' in query_sql:
        has_params = True
    return has_params

def _check_report_crosstab(report_name, crosstab_list):
    crosstab = False
    if report_name in crosstab_list:
        crosstab = True
    return crosstab

    

def _sub_dates(query_sql, start_date, end_date):
        # sub start date
        query_sql  = re.sub(r"\'\d{4}-\d{2}-\d{2}\'::date AS start_date",
            "\'{}\'::date AS start_date".format(start_date), query_sql)
        # sub end date
        query_sql  = re.sub(r"\'\d{4}-\d{2}-\d{2}\'::date AS end_date",
            "\'{}\'::date AS end_date".format(end_date), query_sql)
        return(query_sql)


def _get_report(report, reports_dict):
    try:
        current_report = reports_dict[report]
    except KeyError:
        abort(404)
    return current_report

# run this app
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
