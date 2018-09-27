#!/usr/bin/env python3

import time
import sqlite3
from flask import Flask
import psycopg2
# import upload_data
app = Flask(__name__)

connection_params = {
    'host': 'db',
    'port': '5432',
    'user': 'root',
    'password': 'password',
    'dbname': 'weather_report'
}

@app.route('/')
def hello():
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    sql = """
    drop table if exists weather;
create table weather(
n_day integer,
t float
);
insert into weather values (1, 1.39)
        """
    cursor.execute(sql)
    conn.commit()
    return '<h1>Hello world!</h1>\n'

@app.route('/weather')
def weather():
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    sql = """
insert into weather values (1, %s)
        """ % time.time()
    cursor.execute(sql)
    conn.commit()
    sql = """
        SELECT * FROM weather
        """
    cursor.execute(sql)
    res = cursor.fetchall()
    # s = [str(i)+'<br><hr>' for i in res]
    s = ''
    for i in res:
        s += str(i) + '<br><hr>'
    return s

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
