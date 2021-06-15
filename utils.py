import numpy as np

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
def daysInMonth(month, year):
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
