#!/usr/bin/env python3

from flask import Flask, render_template
from flask_bootstrap import Bootstrap
from flask_sqlalchemy import SQLAlchemy
from wtforms import StringField
# from wtforms import DateField
from flask_wtf import FlaskForm
from wtforms.fields.html5 import DateField
import psycopg2

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

# db.create_all()

@app.route('/', methods=['GET', 'POST'])
def index():
    form = DateForm()
    if form.validate():
        return 'Form is submitted ' + str(form.entrydate.data)
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
    return app.static_url_path

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
