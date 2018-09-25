#!/usr/bin/env python3

import time
import sqlite3
from flask import Flask
import psycopg2

app = Flask(__name__)

connection_params = {
    'host': 'db',
    'port': '5432',
    'user': 'root',
    'password': 'password',
    'dbname': 'homework'
}


@app.route('/')
def hello():

    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    return '<h1>Hello world!</h1>\n'

@app.route('/weather')
def weather():
    return '<h1>weather!</h1>\n'

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
