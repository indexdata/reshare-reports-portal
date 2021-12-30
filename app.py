from flask import Flask, abort, flash, make_response, redirect, request, render_template, Response, session, url_for
from flaskext.markdown import Markdown
from functools import wraps
import json
import jwt
import logging
import os
import psycopg2
from psycopg2 import Error
import requests
import settings
import sys

# define our webapp
app = Flask(__name__)
Markdown(app, extensions=['extra'])

#configuration from settings.py
app.secret_key = settings.SESSION_KEY
app.config['LDP_HOST'] = settings.LDP_HOST
app.config['LDP_PORT'] = settings.LDP_PORT
app.config['LDP_USER'] = settings.LDP_USER
app.config['LDP_PASSWORD'] = settings.LDP_PASSWORD
app.config['LDP_DATABASE'] = settings.LDP_DATABASE
app.config['REPORTS_DIR'] = settings.REPORTS_DIR

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
    for key in all_reports:
        print(key)
        reports_list.append({"name": all_reports[key]['name'], "report": all_reports[key]['report']})
    # sort the list of reports
    reports = sorted(reports_list, key=lambda d: d['name'])
    return render_template("index.html", reports=reports)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        response_code = 204 # can put a function here to check against okapi or ldp
        if response_code == 204:
            authorized = True # can put a function here to authorize
            if authorized:
                session['username'] = username
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

@app.route('/report/<report>/execute/<query>')
@auth_required
def execute_query(report, query):
    current_report = _get_report(report, all_reports)
    print(current_report['queries'][query]['sql'])
    try:
       
        query_sql = open(current_report['queries'][query]['sql'], 'r').read()
    except:
        abort(404)
    connection = _postgres_connect(
            app.config['LDP_HOST'],
            app.config['LDP_PORT'],
            app.config['LDP_USER'],
            app.config['LDP_PASSWORD'],
            app.config['LDP_DATABASE']
        )
    result = _postgres_query(connection, query_sql)
    _postgres_close_connection(connection)
    csv = request.args.get("csv", default=False, type=bool)
    if csv:
        return Response(
            _result_to_csv(result),
            mimetype="text/csv",
            headers={"Content-disposition": "attachment; filename={}.csv".format(report)})
    else:
        return render_template('results.html', result=result, report=report)

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


@app.route('/test')
@auth_required
def test():
    return("test")


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
        cursor.execute(query)
        result['column_names'] = [desc[0] for desc in cursor.description]
        result['data'] = cursor.fetchall()
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
            reports[dir] = {"report":dir,"name": dir.replace('_', ' ').capitalize()}
            for rpath, rsubdirs, rfiles in os.walk(os.path.join(path, dir)):
                reports[dir]["queries"] = {}
                for rname in rfiles:
                    if rname.endswith(".md"):
                        reports[dir]["readme"] = os.path.join(rpath, rname)
                    elif rname.endswith(".sql"):
                        reports[dir]["queries"][rname] = {
                            "sql": os.path.join(rpath, rname),
                            "name": rname
                        }
    if app.debug:
        print(json.dumps(reports, sort_keys=True, indent=4))
    return reports

def _get_report(report, reports_dict):
    try:
        current_report = reports_dict[report]
    except KeyError:
        abort(404)
    return current_report




# run this app
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
