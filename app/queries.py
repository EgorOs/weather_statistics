#!/usr/bin/env python3

import psycopg2

def precipitation_stat(cursor, city_id, ymd_min, ymd_max):
    if ymd_min == ymd_max:
        # If only one day selected
        no_prec = """
        select count(*) from (
            select count(*) from weather   
            where precipitation_type != 'NO' and 
                city_id = %s
                and dmy = '%s' 
            group by dmy
                ) as days""" % (city_id, ymd_min)
        cursor.execute(no_prec)
        no_prec_days = cursor.fetchall()[0][0]
        total = """
        select count(*) from (
            select count(*) from weather   
            where city_id = %s
                and dmy = '%s' 
            group by dmy
                ) as days""" % (city_id, ymd_min)
    else:
        # If range selected
        no_prec = """
        select count(*) from (
            select count(*) from weather   
            where precipitation_type != 'NO' and 
                city_id = %s and dmy >= '%s' 
                and dmy <= '%s' 
            group by dmy
                ) as days""" % (city_id, ymd_min, ymd_max)
        cursor.execute(no_prec)
        no_prec_days = cursor.fetchall()[0][0]
        total = """
        select count(*) from (
            select count(*) from weather   
            where city_id = %s and dmy >= '%s' 
                and dmy <= '%s' 
            group by dmy
                ) as days""" % (city_id, ymd_min, ymd_max)
    cursor.execute(total)
    total_days = cursor.fetchall()[0][0]
    prec_days = total_days - no_prec_days
    if prec_days == 0:
        # return 'no_percipitation'
        return '0'
    elif prec_days == total_days:
        # return 'no_clear_days'
        return '∞'
    else:
        return str(round((prec_days / no_prec_days) * 100, ndigits=2))

"""
select to_char(dmy, 'yyyy') as year, avg(t)
from weather
group by year
"""

def avg_max_by_year(cursor, city_id, ymd_min, ymd_max):
    sql = """
    select avg(max_t) from (
    select to_char(dmy, 'yyyy') as year, max(t) as max_t
    from weather
    where city_id = %s 
        and dmy >= '%s' 
        and dmy <= '%s' 
    group by year) as yearly_max
    """ % (city_id, ymd_min, ymd_max)
    cursor.execute(sql)
    avg_max_lst = cursor.fetchall()[0][0]
    
    return round(avg_max_lst, ndigits=1)


def avg_min_by_year(cursor, city_id, ymd_min, ymd_max):
    sql = """
    select avg(min_t) from (
    select to_char(dmy, 'yyyy') as year, min(t) as min_t
    from weather
    where city_id = %s 
        and dmy >= '%s' 
        and dmy <= '%s' 
    group by year) as yearly_min
    """ % (city_id, ymd_min, ymd_max)
    cursor.execute(sql)
    avg_min_lst = cursor.fetchall()[0][0]
    
    return round(avg_min_lst, ndigits=1)


def most_common_prec_types(cursor, city_id, ymd_min, ymd_max):
    sql = """
    select count(*), precipitation_type from weather
    where precipitation_type != 'NO' 
        and city_id = %s 
        and dmy >= '%s' 
        and dmy <= '%s' 
    group by precipitation_type
    order by count desc
    limit(2)
    """ % (city_id, ymd_min, ymd_max)
    cursor.execute(sql)
    res = cursor.fetchall()
    common_prec = [r[1] for r in res]

    if len(common_prec) == 0:
        return ['NO_PRECIPIATION']

    return common_prec


def avg_wind_speed(cursor, city_id, ymd_min, ymd_max):
    sql = """
    select avg(wind_speed) from weather
    where
        city_id = %s 
        and dmy >= '%s' 
        and dmy <= '%s'
    """ % (city_id, ymd_min, ymd_max)
    cursor.execute(sql)
    try:
        avg_ws = cursor.fetchall()[0][0]
    except IndexError:
        return 'NO_DATA'

    if avg_ws is None:
        return 'NO_DATA'

    return round(avg_ws, ndigits=1)


def common_wind_dir(cursor, city_id, ymd_min, ymd_max):
    sql = """
    select count(*), wind_direction from weather
    where wind_direction is not null and wind_direction != ''
        and city_id = %s 
        and dmy >= '%s' 
        and dmy <= '%s'
    group by wind_direction
    order by count desc
    """ % (city_id, ymd_min, ymd_max)
    cursor.execute(sql)

    try:
        wind_dir = cursor.fetchall()[0][1]
    except IndexError:
        return 'NO_DATA'
    
    if wind_dir == '':
        return 'NO_DATA'
    
    return wind_dir