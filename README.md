# Weather Statistics

## How to run
1. Run ```sudo docker-compose up```
2. Open ```0.0.0.0:5000``` in your browser

## Performance test

[Script to measure TTFB](https://stackoverflow.com/a/38915617/9475474)

+ Script output for *weather_city/5/2010-10-01/2018-10-02'*
    ```
    {
        "total_time": 1.198361,
        "starttransfer_time": 1.198221,
        "dns_time": 2.6e-05,
        "conn_time": 0.000147
    }
    ```
+ Script output for *weather_city/2/2015-10-01/2018-10-02'*
    ```
    {
        "total_time": 0.874665,
        "starttransfer_time": 0.874526,
        "dns_time": 2.5e-05,
        "conn_time": 0.000103
    }
    ```
+ Script output for */*
    ```
    {
        "total_time": 0.033784, 
        "starttransfer_time": 0.033707, 
        "dns_time": 2.4e-05, 
        "conn_time": 0.000101
    }
    ```
    
# Notes

## Flask tutorials
+ [Bootstrap](https://www.youtube.com/watch?v=PE9ZGniSDW8)
+ [Bootstrap + forms](https://www.youtube.com/watch?v=S7ZLiUabaEo)
+ [Dropdown menu](https://www.youtube.com/watch?v=b9W2ul2VRRc)
+ [Postgres + Flask](https://blog.theodo.fr/2017/03/developping-a-flask-web-app-with-a-postresql-database-making-all-the-possible-errors/)
+ [Connect to Postgres](https://vsupalov.com/flask-sqlalchemy-postgres/)
+ [Mapping multiple tables to the same SQLAlchemy model](https://stackoverflow.com/questions/25451335/sqlalchemy-using-the-same-model-with-multiple-tables)
+ [Datepicker](https://www.youtube.com/watch?v=il9hh5Ysw9o)
+ [Bootstrap examples](https://pythonhosted.org/Flask-Bootstrap/basic-usage.html#examples)
+ [SQL Tables relations](https://www.youtube.com/watch?v=jyklg0cTN3M)
+ [SQLAlchemy, an Object Relational Mapper](https://www.youtube.com/watch?v=Tu4vRU4lt6k)

## Docker commands
+ Rebuild existing containter
    ``` sudo docker-compose up -d --force-recreate --build ```

## Issues

+ [Dropdown validation issue](https://stackoverflow.com/questions/46036966/flask-wtform-validation-failing-for-selectfield-why)
+ [WTForms SQLAlchemy fields had to be downgraded](https://stackoverflow.com/questions/48390207/sql-alchemy-valueerror-too-many-values-to-unpack)

## Data sources
+ [Reliable Prognosis](https://rp5.ru/Weather_archive_in_Alexandria_(airport))
+ [National Centers For Environmental Information](https://www.ncdc.noaa.gov/cdo-web/search)