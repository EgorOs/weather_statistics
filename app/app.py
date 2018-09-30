#!/usr/bin/env python3

from flask import Flask, redirect, url_for, render_template
from flask_bootstrap import Bootstrap
from flask_sqlalchemy import SQLAlchemy
from wtforms import SelectField, StringField
from flask_wtf import FlaskForm
from wtforms.fields.html5 import DateField
from wtforms.validators import InputRequired, Required
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

class DateForm(FlaskForm):
    city = QuerySelectField(query_factory=city_selection, allow_blank=True, validators=[Required()])
    period_start = DateField('period_start', validators=[InputRequired()], format='%Y-%m-%d')
    period_end = DateField('period_end', validators=[InputRequired()], format='%Y-%m-%d')


@app.route('/', methods=['GET', 'POST'])
def index():
    form = DateForm()

    if form.validate_on_submit():
        return redirect(url_for('weather_city', 
            city_id=city_to_id(form.city.data), 
            ymd_min=str(form.period_start.data),  
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
    # sql = """SELECT * 
    #            FROM weather
    #            WHERE city_id = %s and dmy > '%s 'and dmy < '%s';""" % (city_id, ymd_min, ymd_max)
    # cursor.execute(sql)
    if ymd_min == ymd_max:
        ordered_t = Weather.query.order_by(asc(Weather.t)).filter(Weather.dmy == ymd_min, Weather.city_id == city_id).all()
        min_t = ordered_t[0].t
        max_t = ordered_t[-1].t
        avg_t = db.session.query(func.avg(Weather.t).filter(Weather.dmy == ymd_min, Weather.city_id == city_id)).first()[0]
    else:
        ordered_t = Weather.query.order_by(asc(Weather.t)).filter(Weather.dmy >= ymd_min, Weather.dmy <= ymd_max, Weather.city_id == city_id).all()
        min_t = ordered_t[0].t
        max_t = ordered_t[-1].t
        avg_t = db.session.query(func.avg(Weather.t).filter(Weather.dmy >= ymd_min, Weather.dmy <= ymd_max, Weather.city_id == city_id)).first()[0]
    # Zero devision check!
    # clear_days = db.session.query(func.count(Weather.record_id).filter(
    #     Weather.dmy >= ymd_min, 
    #     Weather.dmy <= ymd_max, 
    #     Weather.city_id == city_id,
    #     Weather.precipitation_type == 'NO')).first()[0]
    # rainy_days = db.session.query(func.count(Weather.record_id).filter(
    #     Weather.dmy >= ymd_min, 
    #     Weather.dmy <= ymd_max, 
    #     Weather.city_id == city_id,
    #     Weather.precipitation_type == 'RAIN')).first()[0]
    # snowy_days = db.session.query(func.count(Weather.dmy).filter(
    #     Weather.dmy >= ymd_min, 
    #     Weather.dmy <= ymd_max, 
    #     Weather.city_id == city_id,
    #     Weather.precipitation_type == 'SNOW')).first()[0]
    prec_proc = queries.precipitation_stat(cursor, city_id, ymd_min, ymd_max)

    low_lim = dt.datetime.strptime(ymd_min, '%Y-%m-%d')
    high_lim = dt.datetime.strptime(ymd_max, '%Y-%m-%d')
    date_interval = high_lim - low_lim 
    if date_interval.days > 365.25*2:
        return 'Interval is greater than two years'
    # sql = """select count(*) from (select count(*) from weather where precipitation_type = 'RAIN' and city_id = %s and dmy >= '%s' and dmy <= '%s' group by dmy) as days""" % (city_id, ymd_min, ymd_max)
    # cursor.execute(sql)
    # res = cursor.fetchall()
    # res = cursor.fetchall()
    # cursor.execute(sql)
    # res = cursor.fetchall()
    # s = [str(i)+'<br><hr>' for i in res]
    # s = ''
    # for i in res:
    #     s += str(i) + '<br><hr>'
    # if not s:
    #     return '404 not found'
    # return str(max_t) + '/' + str(min_t) + '/' + str(avg_t) + '/'
    params = {'max_t':max_t, 'min_t': min_t, 'prec_proc':prec_proc}
    return render_template('weather.html', **params)

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
