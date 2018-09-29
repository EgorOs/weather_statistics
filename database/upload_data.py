#!/usr/bin/env python3

import os
import gzip
import csv
import psycopg2
import time
from datetime import datetime


class RawDataRP5:

    """ Processes raw csv files from rp5, extracted parameters
    are date, time, temperature (C), humidity (%), wind speed (m/s), 
    wind direction, precipitation (mm)."""
    def __init__(self, path, tempfile, city_id, record_id):
        self.path = path
        self.tempfile = tempfile
        self.city_id = city_id
        self.record_id = record_id

        # Valid values kept to reffer to them
        # in case if some values are missing
        self.last_valid_t = None
        self.last_valid_U = None

        self.broken_measurements_buffer = []

        # A variable to avoid dublicates
        self.prev_date = ''

    def parse_line(self, line):
        """ Get required data from line """
        date_time, t, p0, p, pa, U, dd, ws, *other_data, RRR, tR, E, Tg, EE, sss = line.split(';')
        date, time = date_time.strip('"').split(' ')
        date = datetime.strptime(date, '%d.%m.%Y')

        # Y-m-d fromat is required for postgres
        date = date.strftime('%Y-%m-%d')

        wind_direction = dd.split(' ')[-1].rstrip('"').upper()
        wind_speed = ws.strip('"')
        if wind_speed == '0' or wind_speed == '':
            wind_direction ='NO-WIND'
            wind_speed = 0
        else:
            wind_speed = int(wind_speed)

            # ADD MORE TYPES BASED ON NUMBERS INCLUDE tR
        # RRR stands for precipitation
        if RRR.isdigit():
            precipitation = int(RRR.strip('"'))
            # sss stands for snow depth
            if sss.isdigit():
                precipitation_type = 'SNOW'
            else:
                precipitation_type = 'RAIN'
        else:
            precipitation = 0
            precipitation_type = 'NO'

        measurements = {
        'line': line,
        'date': date,
        'time': time,
        't': t.strip('"'),
        'humidity': U.strip('"'),
        'wind_speed': wind_speed,
        'wind_direction': wind_direction,
        'precipitation': precipitation,
        'precipitation_type': precipitation_type,
        }

        return measurements

    def fix_measurements(self, measurements, old_t, new_t, old_U, new_U):
        """ Replace missing values with average """
        if measurements['t'] == '':
            old_t = float(old_t)
            new_t = float(new_t)
            measurements['t'] = str(old_t/2 + new_t/2)
        if measurements['humidity'] == '':
            old_U = float(old_U)
            new_U = float(new_U)
            measurements['humidity'] = str(old_U/2 + new_U/2)

        return measurements

    def write_rows(self, measurements, writer):
        daytime = {'11:00', '12:00', '13:00', '14:00', '15:00', '16:00'}

        del measurements['line']
        measurements['record_id'] = self.record_id
        measurements['city_id'] = self.city_id
        measurements['humidity'] = float(measurements['humidity'].strip('"'))

        order = ('record_id', 'city_id','date', 't', 'humidity', 'wind_speed',
                'wind_direction', 'precipitation', 'precipitation_type')
        
        time = measurements['time']
        date = measurements['date']

        if time in daytime and date != self.prev_date:
            writer.writerow([measurements[key] for key in order])
            self.record_id += 1

            # Avoid dublicates
            self.prev_date = date
    
    def process(self):
        with gzip.open(self.path, mode='rt') as f, open(self.tempfile, mode='a', newline='') as f_out:
            
            # Skip header
            for i in range(7):
                next(f)

            writer = csv.writer(f_out, delimiter=',')

            for line in f:
                measurements = self.parse_line(line)

                # Validate problematic values
                if measurements['t'] != '' and measurements['humidity'] != '':

                    # Fix broken measurements if they exist
                    if self.broken_measurements_buffer:
                        fixed_measurements_lst = [
                            self.fix_measurements(m, 
                                self.last_valid_t, 
                                measurements['t'], 
                                self.last_valid_U, 
                                measurements['humidity']
                                )
                            for m in self.broken_measurements_buffer
                            ]

                        self.broken_measurements_buffer = []
                        [self.write_rows(fm, writer) for fm in fixed_measurements_lst]

                    # Process values normally
                    self.last_valid_t = measurements['t']
                    self.last_valid_U = measurements['humidity']
                    self.write_rows(measurements, writer)

                else:
                    self.broken_measurements_buffer.append(measurements)

        return self.record_id

class RawDataNOAA:
    """ NOAA doesn't provide all required data and is 
        used as supplementary source. """
    def __init__(self, path, tempfile, city_id, record_id):
        self.path = path
        self.tempfile = tempfile
        self.city_id = city_id
        self.record_id = record_id

    def parse_line(self, line):
        """ Get required data from line """
        station, name, country, date, PRCP, SNWD, t, tmax, tmin = line.split(',')
        date = date.strip('"')

        date = datetime.strptime(date, '%Y-%m-%d')
        date = str(date).split(' ')[0]

        precipitation_str = PRCP.strip('"').strip('\n')
        if precipitation_str == '':
            precipitation = 0
        else:
            precipitation = int(float(precipitation_str))
        
        if precipitation == 0:
            precipitation_type = 'NO'
        elif SNWD.strip('"').strip('\n') == '':
            precipitation_type = 'RAIN'
        else:
            precipitation_type = 'SNOW'

        measurements = {
        'date': date,
        't': t.strip('"'),
        'humidity': 'NULL',
        'wind_speed': 'NULL',
        'wind_direction': 'NULL',
        'precipitation': precipitation,
        'precipitation_type': precipitation_type,
        }

        return measurements


    def write_rows(self, measurements, writer):
        measurements['record_id'] = self.record_id
        measurements['city_id'] = self.city_id

        order = ('record_id', 'city_id','date', 't', 'humidity', 'wind_speed',
                'wind_direction', 'precipitation', 'precipitation_type')
        writer.writerow([measurements[key] for key in order])
        self.record_id += 1

    def process(self):
        with gzip.open(self.path, mode='rt') as f, open(self.tempfile, mode='a', newline='') as f_out:
            
            # Skip header
            for i in range(1):
                next(f)

            writer = csv.writer(f_out, delimiter=',')

            for line in f:
                measurements = self.parse_line(line)
                self.write_rows(measurements, writer)

        return self.record_id

class DataUploader:
    def __init__(self, path, tempfile, connection_params):
        self.path = path
        self.tempfile = tempfile
        self.connection_params = connection_params
        self.record_id = 0
        self.city_id = 0
        self.rp5_path = path + 'rp5/'
        self.NOAA_path = path + 'NOAA/'
        self.city_names = set()
        self.city_id_mapper = {}

        if os.path.exists(self.tempfile):
            os.remove(self.tempfile)

    def get_city_names(self):
        if os.path.exists(self.rp5_path):
            dataset_names = [n for n in os.listdir(self.rp5_path) if n.endswith('.csv.gz')]
            self.city_names.update({n.rstrip('.csv.gz') for n in dataset_names})
        else:
            print('WARNING: Rp5 data is missing.\nThere is no directory %s' % self.rp5_path)

        if os.path.exists(self.NOAA_path):
            dataset_names = [n for n in os.listdir(self.NOAA_path) if n.endswith('.csv.gz')]
            self.city_names.update({n.rstrip('.csv.gz') for n in dataset_names})
        else:
            print('WARNING: NOAA data is missing.\nThere is no directory %s' % self.NOAA_path)

    def set_city_ids(self):
        for city in sorted(list(self.city_names)):
            self.city_id_mapper[city] = self.city_id
            self.city_id += 1

    def process_rp5(self):
        if os.path.exists(self.rp5_path):
            dataset_names = [n for n in os.listdir(self.rp5_path) if n.endswith('.csv.gz')]
            city_names = [n.rstrip('.csv.gz') for n in dataset_names]
            for city in city_names:
                city_path = '%s%s.csv.gz' % (self.rp5_path, city)
                data_proc = RawDataRP5(city_path, self.tempfile, self.city_id_mapper[city], self.record_id)
            self.record_id = data_proc.process()

    def process_NOAA(self):
        if os.path.exists(self.NOAA_path):
            dataset_names = [n for n in os.listdir(self.NOAA_path) if n.endswith('.csv.gz')]
            city_names = [n.rstrip('.csv.gz') for n in dataset_names]
            for city in city_names:
                city_path = '%s%s.csv.gz' % (self.NOAA_path, city)
                data_proc = RawDataNOAA(city_path, self.tempfile, self.city_id_mapper[city], self.record_id)
            self.record_id = data_proc.process()

    def prepare(self):
        self.get_city_names()
        self.set_city_ids()
        self.process_rp5()
        self.process_NOAA()

    def upload(self):
        # Wait for connection
        while True:
            try:
                conn = psycopg2.connect(**self.connection_params)
                print('Connetction established')
                conn.close()
                break
            except psycopg2.OperationalError:
                print('Trying to connect to database')
                time.sleep(1)

        # Upload cities, since weather table depends on them
        with psycopg2.connect(**self.connection_params) as conn, open(self.tempfile, mode='rt') as tmp:
            cursor = conn.cursor()
            for c, i in self.city_id_mapper.items():
                sql = """INSERT INTO city VALUES(%s, '%s')""" % (i, c.lower())
                cursor.execute(sql)
                conn.commit()
            cursor.copy_from(tmp, 'weather', null='NULL', sep=',')
            conn.commit()

        os.remove(self.tempfile)
        

if __name__ == '__main__':
    connection_params = {
        'host': 'db',
        'port': '5432',
        'user': 'root',
        'password': 'password',
        'dbname': 'weather_report'
    }

    uploader = DataUploader('csv/', 'tmp.csv', connection_params)
    uploader.prepare()
    uploader.upload()
"""
Possible tests:

Validate csv data for non-standart wind description
like 'Calm, no wind'

Validate time of measurements in case there is no
12:00, 11:00 measurements

Count total number of records

Combinations like NORTH-NORTHEAST are possible

Check if date/time on local machine is not ahead of real time

Validation might crush on letters

Check long city names like Saint-Petersburg

Optimization:

Store the date of last update in database and
download only required data
"""