#!/usr/bin/env python3

from flask import Flask, redirect, url_for, render_template
from flask_bootstrap import Bootstrap
from flask_sqlalchemy import SQLAlchemy
from wtforms import SelectField, StringField
from flask_wtf import FlaskForm
from wtforms.fields.html5 import DateField
from wtforms.validators import InputRequired, Required, ValidationError
from wtforms_sqlalchemy.fields import QuerySelectField
from sqlalchemy import func, desc, asc
import queries
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
        return self.city_name.upper()

class Weather(db.Model):

    __tablename__ = 'weather'

    record_id = db.Column('record_id', db.Integer, primary_key=True)
    city_id = db.Column(db.Integer, db.ForeignKey(City.city_id))
    dmy = db.Column(db.Date)
    t = db.Column(db.Float) 
    humidity = db.Column(db.Float)
    wind_speed = db.Column(db.Integer)
    wind_direction = db.Column(db.Unicode)
    precipitation = db.Column(db.Float)
    precipitation_type = db.Column(db.Unicode)

def city_selection():
    """ Represents query as callable object, required for QuerySelectField """
    return City.query

def city_to_id(city):
    """ Translates city name into its id if this city is present in database """
    name = str(city).lower()
    return City.query.filter_by(city_name=name).first().city_id

def id_to_city(city_id):
    """ Translates city id into its name if this city is present in database """
    return City.query.filter_by(city_id=city_id).first().city_name

def date_interval_check(form, field):
    last_date = '2018-10-02'
    first_date = '2010-01-01'
    l_y, l_m, l_d = last_date.split('-')
    last_mdy = '-'.join([l_m, l_d, l_y])
    f_y, f_m, f_d = first_date.split('-')
    first_mdy = '-'.join([f_m, f_d, f_y])
    upper_limit = dt.datetime.strptime(last_date, '%Y-%m-%d').date()
    lower_limit = dt.datetime.strptime(first_date, '%Y-%m-%d').date()
    if field.data > upper_limit:
        field.error = ['Cannot select value above %s ' % last_mdy]
        raise ValidationError('Value exeeds dataset limits.')

    if field.data < lower_limit:
        field.error = ['Cannot select value below %s ' % first_mdy]
        raise ValidationError('Value exeeds dataset limits.')

def date_compare(form, field):
    if field.data < form.period_start.data:
        field.error = ['Must be greater than initial date']
        raise ValidationError('Value exeeds dataset limits.')

class DateForm(FlaskForm):
    city = QuerySelectField(query_factory=city_selection, allow_blank=True, validators=[Required()])
    period_start = DateField('period_start', validators=[InputRequired(), date_interval_check], format='%Y-%m-%d')
    period_end = DateField('period_end', validators=[InputRequired(),date_interval_check, date_compare], format='%Y-%m-%d')


@app.route('/', methods=['GET', 'POST'])
def index():
    form = DateForm()

    # Check if min < max
    if form.validate_on_submit():
        return redirect(url_for('weather_city', 
            city_id=city_to_id(form.city.data), 
            ymd_min=form.period_start.data,  
            ymd_max=str(form.period_end.data)))

    return render_template('index.html', form=form)


@app.route('/weather')
def weather():
    city_records = City.query.all()
    cities = [(c.city_id, c.city_name) for c in city_records]
    return str(cities)


@app.route('/weather_city/<int:city_id>/<string:ymd_min>/<string:ymd_max>')
def weather_city(city_id, ymd_min, ymd_max):
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()

    if ymd_min == ymd_max:
        ordered_t = Weather.query.order_by(asc(Weather.t)).filter(Weather.dmy == ymd_min, Weather.city_id == city_id).all()
        min_t = ordered_t[0].t
        max_t = ordered_t[-1].t
        avg_t = db.session.query(func.avg(Weather.t).filter(Weather.dmy == ymd_min, Weather.city_id == city_id)).first()[0]
        avg_t = round(avg_t, ndigits=1)
    else:
        ordered_t = Weather.query.order_by(asc(Weather.t)).filter(Weather.dmy >= ymd_min, Weather.dmy <= ymd_max, Weather.city_id == city_id).all()
        min_t = ordered_t[0].t
        max_t = ordered_t[-1].t
        avg_t = db.session.query(func.avg(Weather.t).filter(Weather.dmy >= ymd_min, Weather.dmy <= ymd_max, Weather.city_id == city_id)).first()[0]
        avg_t = round(avg_t, ndigits=1)

    prec_proc = queries.precipitation_stat(cursor, city_id, ymd_min, ymd_max)
    common_prec = queries.most_common_prec_types(cursor, city_id, ymd_min, ymd_max)
    avg_ws = queries.avg_wind_speed(cursor, city_id, ymd_min, ymd_max)
    wind_dir = queries.common_wind_dir(cursor, city_id, ymd_min, ymd_max)
    st_days = queries.similar_t_days(cursor, city_id, ymd_min, ymd_max)


    low_lim = dt.datetime.strptime(ymd_min, '%Y-%m-%d')
    high_lim = dt.datetime.strptime(ymd_max, '%Y-%m-%d')
    date_interval = high_lim - low_lim 

    params = {
    'city_name': id_to_city(city_id).upper(),
    'ymd_min':ymd_min,
    'ymd_max':ymd_max,
    'max_t':max_t, 
    'min_t': min_t,
    'avg_t': avg_t,
    'prec_proc':prec_proc, 
    'common_prec': common_prec, 
    'avg_ws': avg_ws, 
    'wind_dir': wind_dir,
    'avg_min_by_year': None,
    'avg_max_by_year': None,
    'st_days': st_days,
    }

    if date_interval.days > 365.25*2:
        avg_min_by_year = queries.avg_min_by_year(cursor, city_id, ymd_min, ymd_max)
        avg_max_by_year = queries.avg_max_by_year(cursor, city_id, ymd_min, ymd_max)
        params['avg_min_by_year'] = avg_min_by_year
        params['avg_max_by_year'] = avg_max_by_year

    return render_template('weather.html', **params)

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
