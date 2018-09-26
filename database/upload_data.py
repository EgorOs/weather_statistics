#!/usr/bin/env python3

import os
import gzip
import csv

dataset_names = [n for n in os.listdir('csv') if n.endswith('.csv.gz')]
name = 'out.csv'
print(dataset_names)

class RawDataRP5:

    """ Processes raw csv files from rp5, extracted parameters
    are date, time, temperature (C), humidity (%), wind speed (m/s), 
    wind direction, precipitation (mm), precipitation intensity."""
    def __init__(self, path, tempfile):
        self.path = path
        self.tempfile = tempfile

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
        order = ('date', 'time', 't', 'humidity', 'wind_speed',
                'wind_direction', 'precipitation', 'precipitation_type')
        
        time = measurements['time']
        date = measurements['date']

        if time in daytime and date != self.prev_date:
            writer.writerow([measurements[key] for key in order])
            # Avoid dublicates
            self.prev_date = date
    
    def process(self):
        with gzip.open(self.path, mode='rt') as f, open(self.tempfile, mode='w', newline='') as f_out:
            
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


data_proc = RawDataRP5('csv/' + dataset_names[1], name)
data_proc.process()

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

Optimization:

Store the date of last update in database and
download only required data
"""