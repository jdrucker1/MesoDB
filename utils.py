import datetime
import logging
import os.path as osp
import os
import pandas as pd

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
    site_keys = ['STID','LONGITUDE','LATITUDE','ELEVATION','STATE']
    site_dic = {key: [] for key in site_keys}
    data_keys = ['STID','datetime','fm10']
    data_dic = {key: [] for key in data_keys}
    for stData in mesowestData['STATION']:
        for site_key in site_keys:
            site_dic[site_key].append(stData[site_key]) 
        data_dic['STID'] += [stData['STID']]*len(stData['OBSERVATIONS']['date_time'])
        data_dic['datetime'] += stData['OBSERVATIONS']['date_time']
        data_dic['fm10'] += stData['OBSERVATIONS']['fuel_moisture_set_1']
    
    data = pd.DataFrame.from_dict(data_dic)
    data['datetime'] = pd.to_datetime(data['datetime'])
    sites = pd.DataFrame.from_dict(site_dic).set_index('STID')
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

# Set UTC datetime from integers
#
# @ Param year - user specified year
# @ Param month - user specified month
# @ Param day - user specified day
# @ Param hour - user specified hour
#
def set_utc_datetime(year, month, day, hour = 0):
    userDatetime = datetime.datetime(year,month,day,hour,tzinfo=datetime.timezone.utc)
    # Checks if datetime given is valid (not a future date or a time before mesowest started collecting data)
    if userDatetime > datetime.datetime.now(datetime.timezone.utc) or userDatetime.year < 1996:
        logging.error('{} not a valid datetime'.format(userDatetime))
        return None
    else:
        return datetime.datetime(year,month,day,hour,tzinfo=datetime.timezone.utc)
    