#!/usr/bin/env python3

def precipitation_stat(cursor, city_id, ymd_min, ymd_max):
    if ymd_min == ymd_max:
        # If only one day selected
        no_prec = """
        select count(*) from (
            select count(*) from weather   
            where precipitation_type = 'NO' and 
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
            where precipitation_type = 'NO' and 
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
        return 0.0
    elif prec_days == total_days:
        return 100.0
    else:
        return round((prec_days / no_prec_days) * 100, ndigits=2)
