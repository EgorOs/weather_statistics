#!/usr/bin/env python3

from flask import Flask, render_template
from flask_bootstrap import Bootstrap
from flask_sqlalchemy import SQLAlchemy
from wtforms import StringField
import psycopg2

app = Flask(__name__)
connection_params = {
    'host': 'db',
    'port': '5432',
    'user': 'root',
    'password': 'password',
    'dbname': 'weather_report'
}
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql+psycopg2://%(user)s:\
%(password)s@%(host)s:%(port)s/%(dbname)s' % connection_params

db = SQLAlchemy(app)
bootstrap = Bootstrap(app)


class WeatherReport(db.Model):

    __tablename__ = 'ryazan'

    day_id = db.Column(db.Integer, primary_key=True)
    dmy = db.Column(db.Date)
    time_of_day = db.Column(db.Time)
    t = db.Column(db.Float) 
    humidity = db.Column(db.Float)
    wind_speed = db.Column(db.Integer)
    wind_direction = db.Column(db.String)

# db.create_all()

@app.route('/')
def index():
    # create_tables(connection_params, 'tmp.csv')
    return render_template('index.html')

@app.route('/weather')
def weather():
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    # sql = """
    #     SELECT * FROM alexandria
    #     """
    # cursor.execute(sql)
    # res = cursor.fetchall()
    # WeatherReport.__tablename__ = 'ryazan'
    sql = db.session.query(WeatherReport)
    cursor.execute(str(sql))
    res = cursor.fetchall()
    s = [str(i)+'<br><hr>' for i in res]
    s = ''
    for i in res:
        s += str(i) + '<br><hr>'
    return s

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
