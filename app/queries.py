#!/usr/bin/env python3

import psycopg2

def precipitation_stat(cursor, city_id, ymd_min, ymd_max):
    if ymd_min == ymd_max:
        # If only one day selected
        no_prec = """
            select precipitation_type from weather   
            where  
                city_id = %s
                and dmy = %s"""
        cursor.execute(no_prec, (city_id, ymd_min))
        res = cursor.fetchall()
        prec = [r[0] for r in res if r[0] != 'NO'] 
        if prec:
            return '∞'
        else:
            return '0'
    else:
        # If range selected
        prec = """
        select count(*) from(select dmy from weather
        where precipitation_type != 'NO'
        and city_id = %s
                and dmy >= %s 
                and dmy <= %s 
        group by dmy) as t_days
        """
        cursor.execute(prec, (city_id, ymd_min, ymd_max))
        prec_days = cursor.fetchall()[0][0]
        total = """
        select count(*) from (
            select count(*) from weather   
            where city_id = %s 
                and dmy >= %s 
                and dmy <= %s 
            group by dmy
                ) as days"""
    cursor.execute(total, (city_id, ymd_min, ymd_max))
    total_days = cursor.fetchall()[0][0]
    no_prec_days = total_days - prec_days
    if prec_days == 0:
        return '0'
    elif prec_days == total_days:
        return '∞'
    else:
        return str(round((prec_days / no_prec_days) * 100, ndigits=2))


def avg_max_by_year(cursor, city_id, ymd_min, ymd_max):
    sql = """
    select avg(max_t) from (
    select to_char(dmy, 'yyyy') as year, max(t) as max_t
    from weather
    where city_id = %s 
        and dmy >= %s
        and dmy <= %s
    group by year) as yearly_max
    """
    cursor.execute(sql, (city_id, ymd_min, ymd_max))
    avg_max_lst = cursor.fetchall()[0][0]
    
    return round(avg_max_lst, ndigits=1)


def avg_min_by_year(cursor, city_id, ymd_min, ymd_max):
    sql = """
    select avg(min_t) from (
    select to_char(dmy, 'yyyy') as year, min(t) as min_t
    from weather
    where city_id = %s 
        and dmy >= %s
        and dmy <= %s
    group by year) as yearly_min
    """
    cursor.execute(sql, (city_id, ymd_min, ymd_max))
    avg_min_lst = cursor.fetchall()[0][0]
    
    return round(avg_min_lst, ndigits=1)


def most_common_prec_types(cursor, city_id, ymd_min, ymd_max):
    if ymd_min == ymd_max:
        sql = """
        select precipitation_type from weather
        where city_id = %s 
            and dmy = %s 
        """
        cursor.execute(sql, (city_id, ymd_min))
        res = cursor.fetchall()
        common_prec = [r[0] for r in res if r[0] != 'NO']
        if common_prec:
            return common_prec
        else:
            return ['NO_PRECIPIATION']
    else:
        sql = """
        select count(*), precipitation_type from weather
        where precipitation_type != 'NO' 
            and city_id = %s 
            and dmy >= %s 
            and dmy <= %s 
        group by precipitation_type
        order by count desc
        limit(2)
        """
        cursor.execute(sql, (city_id, ymd_min, ymd_max))
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
        and dmy >= %s 
        and dmy <= %s
    """
    cursor.execute(sql, (city_id, ymd_min, ymd_max))
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
        and dmy >= %s 
        and dmy <= %s
    group by wind_direction
    order by count desc
    """
    cursor.execute(sql, (city_id, ymd_min, ymd_max))

    try:
        wind_dir = cursor.fetchall()[0][1]
    except IndexError:
        return 'NO_DATA'
    
    if wind_dir == '':
        return 'NO_DATA'
    
    return wind_dir

def similar_t_days(cursor, city_id, ymd_min, ymd_max):
    sql = """
        select avg(t) from weather
        where city_id = %s
        group by dmy
        order by dmy desc
        limit 1
    """

    cursor.execute(sql, (city_id,))
    today_t = cursor.fetchall()[0][0]

    sql = """
    select avg(t), dmy from weather
    where city_id = %s
    group by dmy
    order by abs(avg(t) - %s)
    limit 3
    """
    cursor.execute(sql, (city_id, today_t))
    closest = cursor.fetchall()
    closest = [ (round(c[0], ndigits=1),c[1]) for c in closest]
    today_t = round(today_t, ndigits=1)
    return today_t, closest