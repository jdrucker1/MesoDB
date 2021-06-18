import datetime
import logging
import numpy as np
import os.path as osp
import os
import pandas as pd

# Checks if given value is an array, if not, make it into one
#
# @ Param value - given variable
#
def to_array(value):
    if isinstance(value,list):
        value = np.array(value)
    elif isinstance(value, int):
        value = np.array([value])
        
    return value

# Check coordinates and return properly coordinates
#
# @ Param latitude1,latitude2,longitude1,longitude2 - geographical coordinates in WGS84 degrees
#
def check_coords(latitude1, latitude2, longitude1, longitude2):
    valid_coords = lambda y1,y2,x1,x2: -90 < y1 < 90 and -90 < y2 < 90 and -180 < x1 < 180 and -180 < x2 < 180
    if any([latitude1 is None, latitude2 is None, longitude1 is None, longitude2 is None]) or not valid_coords(latitude1,latitude2,longitude1,longitude2):
        return None,None,None,None
    if latitude1 < latitude2:
        lat1 = latitude1
        lat2 = latitude2
    else:
        lat1 = latitude2
        lat2 = latitude1
    if longitude1 < longitude2:
        lon1 = longitude1
        lon2 = longitude2
    else:
        lon1 = longitude2
        lon2 = longitude1
    return lat1,lat2,lon1,lon2

# Returns the number of days in a given month
# 
# @ Param month - specific month
# @ Param year - specific year
#
def days_in_month(month, year):
    if month == 1 or month == 3 or month == 5 or month == 7 or month == 8 or month == 10 or month == 12:
        dayLimit = 31
    elif month == 2:
        if year % 4 == 0:
            dayLimit = 29
        else:
            dayLimit = 28
    else:
        dayLimit = 30
    return dayLimit

# Returns the time in Meso format
# 
# @ Param utc_datetime - datetime in UTC
#
def meso_time(utc_datetime):
    year = utc_datetime.year
    month = utc_datetime.month
    day = utc_datetime.day
    hour = utc_datetime.hour
    return "{:04d}{:02d}{:02d}{:02d}{:02d}".format(year,month,day,hour,0)

# Tranform dictionary data from MesoWest request to pandas dataframe
# 
# @ Param utc_datetime - datetime in UTC
#
def meso_data_2_df(mesowestData):
    keys = ['STID','LONGITUDE','LATITUDE','ELEVATION','STATE']
    sites_dic = {key: [] for key in keys}
    data = pd.DataFrame([])
    for stData in mesowestData['STATION']:
        for key in keys:
            sites_dic[key].append(stData[key]) 
        df = pd.DataFrame.from_dict(stData['OBSERVATIONS'])
        df.columns = ['datetime','fm10']
        df['STID'] = stData['STID']
        data = data.append(df)
    data['datetime'] = pd.to_datetime(data['datetime'])
    data.reset_index(drop=True)
    sites = pd.DataFrame.from_dict(sites_dic).set_index('STID')
    return data,sites

# Ensure all directories in path if a file exist, for convenience return path itself.
#
# @ Param path - the path whose directories should exist
#
def ensure_dir(path):
    path_dir = osp.dirname(path)
    if not osp.exists(path_dir):
        os.makedirs(path_dir)
    return path

# String to datetime
#
# @ Param year - user specified year
# @ Param month - user specified month
# @ Param day - user specified day
# @ Param hour - user specified hour
#
def str_2_dt(year,month,day,hour):
    userDatetime = datetime.datetime(year,month,day,hour,tzinfo=datetime.timezone.utc)
    # Checks if datetime given is valid (not a future date or a time before mesowest started collecting data)
    if userDatetime > datetime.datetime.now(datetime.timezone.utc) or userDatetime.year < 1996:
        logging.error('{} not a valid datetime'.format(userDatetime))
    else:
        return datetime.datetime(year,month,day,hour,tzinfo=datetime.timezone.utc)
    



