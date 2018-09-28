#!/usr/bin/env python3

from flask import Flask, redirect, url_for, render_template
from flask_bootstrap import Bootstrap
from flask_sqlalchemy import SQLAlchemy
from wtforms import StringField
# from wtforms import DateField
from flask_wtf import FlaskForm
from wtforms.fields.html5 import DateField
import psycopg2
import datetime as dt

app = Flask(__name__)
app.config['SECRET_KEY'] = 'SecretKey'
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

class DateForm(FlaskForm):
    entrydate = DateField('entrydate', format='%Y-%m-%d')

class WeatherReport(db.Model):

    __tablename__ = 'ryazan'

    day_id = db.Column(db.Integer, primary_key=True)
    dmy = db.Column(db.Date)
    time_of_day = db.Column(db.Time)
    t = db.Column(db.Float) 
    humidity = db.Column(db.Float)
    wind_speed = db.Column(db.Integer)
    wind_direction = db.Column(db.String)


@app.route('/', methods=['GET', 'POST'])
def index():
    form = DateForm()
    if form.validate():
        # return 'Form is submitted ' + str(form.entrydate.data)
        return redirect(url_for('weather_city', city='alexandria', ymd=str(form.entrydate.data)))
    return render_template('index.html', form=form)

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
    sql = db.session.query(WeatherReport.t, WeatherReport.dmy)
    # cursor.execute(str(sql))
    # res = cursor.fetchall()
    res = WeatherReport.query.all()
    print(res[0].dmy)
    s = [str(i.dmy)+'<br><hr>' for i in res]
    s = ''
    print(s)
    for i in res:
        s += str(i) + '<br><hr>'
    return s


@app.route('/weather_city/<string:city>/<string:ymd>')
def weather_city(city, ymd):
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    y, m, d = [int(i) for i in ymd.split('-')]
    sql = """SELECT * 
               FROM %s
               WHERE dmy = '%s';""" % (city, dt.date(y,m,d))
    cursor.execute(sql)
    res = cursor.fetchall()
    cursor.execute(str(sql))
    res = cursor.fetchall()
    s = [str(i)+'<br><hr>' for i in res]
    s = ''
    for i in res:
        s += str(i) + '<br><hr>'
    if not s:
        return '404 not found %s/%s/%s' % (y,m,d)
    return s

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
