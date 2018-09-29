#!/usr/bin/env python3

from flask import Flask, redirect, url_for, render_template
from flask_bootstrap import Bootstrap
from flask_sqlalchemy import SQLAlchemy
from wtforms import SelectField, StringField
from flask_wtf import FlaskForm
from wtforms.fields.html5 import DateField
from wtforms.validators import InputRequired, Required
from wtforms_sqlalchemy.fields import QuerySelectField
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
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://%(user)s:\
%(password)s@%(host)s:%(port)s/%(dbname)s' % connection_params

db = SQLAlchemy(app)
bootstrap = Bootstrap(app)

class City(db.Model):
    
    __tablename__ = 'city'

    city_id = db.Column('city_id', db.Integer, primary_key=True)
    city_name = db.Column('city_name', db.Unicode)

    def __repr__(self):
        return self.city_name.capitalize()

class Weather(db.Model):

    __tablename__ = 'weather'

    record_id = db.Column('record_id', db.Integer, primary_key=True)
    city_id = db.Column(db.Integer, db.ForeignKey(City.city_id))
    dmy = db.Column(db.Date)
    time_of_day = db.Column(db.Time)
    t = db.Column(db.Float) 
    humidity = db.Column(db.Float)
    wind_speed = db.Column(db.Integer)
    wind_direction = db.Column(db.Unicode)

def city_selection():
    """ Represents query as callable object, required for QuerySelectField """
    return City.query

def city_to_id(city):
    """ Translates city name into its id if this city is present in database """
    name = str(city).lower()
    return City.query.filter_by(city_name=name).first().city_id

class DateForm(FlaskForm):
    # city_records = City.query.all()
    # options = [('-1', '')] + [(str(c.city_id), c.city_name.capitalize()) for c in city_records]
    city = QuerySelectField(query_factory=city_selection, allow_blank=True, validators=[Required()])
    period_start = DateField('period_start', validators=[InputRequired()], format='%Y-%m-%d')
    period_end = DateField('period_end', validators=[InputRequired()], format='%Y-%m-%d')

@app.route('/', methods=['GET', 'POST'])
def index():
    form = DateForm()
    if form.validate_on_submit():
        return redirect(url_for('weather_city', city=city_to_id(form.city.data), ymd_min=str(form.period_start.data),  ymd_max=str(form.period_end.data)))
    return render_template('index.html', form=form)

@app.route('/weather')
def weather():
    city_records = City.query.all()
    cities = [(c.city_id, c.city_name) for c in city_records]
    return str(cities)


@app.route('/weather_city/<int:city>/<string:ymd_min>/<string:ymd_max>')
def weather_city(city, ymd_min, ymd_max):
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    y, m, d = [int(i) for i in ymd_min.split('-')]
    ymd_min = dt.date(y,m,d)
    y, m, d = [int(i) for i in ymd_max.split('-')]
    ymd_max = dt.date(y,m,d)
    sql = """SELECT * 
               FROM weather
               WHERE city_id = %s and dmy > '%s 'and dmy < '%s';""" % (city, ymd_min, ymd_max)
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
