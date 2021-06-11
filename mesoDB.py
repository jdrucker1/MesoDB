
# MesoDB Script

# Libraries
#
import datetime
import logging
import matplotlib.dates as md
import matplotlib.pyplot as plt
from MesoPy import Meso
import numpy as np
import os.path as osp
import os
import pandas as pd
import pytz


# Mesowest Database Class
#
class mesoDB(object):
    
    # mesoDB constructor
    #
    def __init__(self, mesoToken, folder_path=osp.join(osp.abspath(os.getcwd()),"mesoDB")):
        self.folder_path = folder_path
        self.token = Meso(token=mesoToken)
        self.exists_here()
        self.init_params()
        
        
       
    # Checks if mesoDB directory exists. If not, it is created
    #    
    def exists_here(self):
        if osp.exists(self.folder_path):
            logging.info("mesoDB - Existent DB path {}".format(self.folder_path))
        else:
            os.makedirs(self.folder_path)
            
    
    # Checks if year folder exists, if not, make it
    #
    # @ Param year - 
    def year_exists(self,year):
        if osp.exists("{}/{:04d}".format(self.folder_path,year)):
            logging.info("mesoDB/year - Existent DB path {}/{}".format(self.folder_path,year))
        else:
            os.makedirs("{}/{}".format(self.folder_path,year))
        
    # Checks if month folder exists, if not, make it
    #
    def julian_exists(self,day,month,year):
        day = datetime.datetime(year, month, day, tzinfo=datetime.timezone.utc).timetuple().tm_yday
        if osp.exists("{}/{:04d}/{:03d}".format(self.folder_path,year,day)):
            logging.info("mesoDB/year/month - Existent DB path {}/{:04d}/{:03d}".format(self.folder_path,year,day))
            return True
        else:
            os.makedirs("{}/{:04d}/{:03d}".format(self.folder_path,year,day))
            return False
    
    # Checks if day file exits, if so returns true, else false
    #
    def hour_file_exists(self,hour,day,year):
        if osp.exists("{}/{:4d}/{:03d}/{:04d}{:03d}{:02d}.pkl".format(self.folder_path,year,day,year,day,hour)):
            logging.info("mesoDB - Existent DB path {}/{:04d}/{:03d}/{:04d}{:03d}{:02d}.pkl".format(self.folder_path,year,day,year,day,hour))
            return True
        else:
            return False
                

    # Initialize parameters for get_data
    #   
    def init_params(self):
        self.params = {'year': [int(datetime.datetime.now().year)], 'month': [int(datetime.datetime.now().month)], 'day': [0],
                    'latitude1': None, 'latitude2': None, 'longitude1': None, 'longitude2': None, 'makeFile': False, 'override': False}
    
    
    # Returns the name of the month based on numerical value
    #
    def which_month(self,month):
        if month == 1:
            return 'Jan'
        elif month == 2:
            return 'Feb'
        elif month == 3:
            return 'Mar'
        elif month == 4:
            return 'Apr'
        elif month == 5:
            return 'May'
        elif month == 6:
            return 'Jun'
        elif month == 7:
            return 'Jul'
        elif month == 8:
            return 'Aug'
        elif month == 9:
            return 'Sep'
        elif month == 10:
            return 'Oct'
        elif month == 11:
            return 'Nov'
        elif month == 12:
            return 'Dec'
        else:
            print('Not a valid month (1-12)')
        
    
    # Returns the number of days in a given month
    # 
    # @ Param month - specific month
    # @ Param year - specific year
    #
    def daysInMonth(self,month,year):
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
            
    
    # Save data from mesowest to the local database
    #
    # @ Param df_new - dataframe with fuel moisture data
    # @ Param day - julian calander day (0-365)
    # @ Param year - given year
    #
    def save_to_DB(self,df_new,hour,day,year):
        
        # If the day file does not exist, make the folder and save the data
        if self.hour_file_exists(hour,day,year) == False:
            df_new.to_pickle("{}/{:04d}/{:03d}/{:04d}{:03d}{:02d}.pkl".format(self.folder_path,year,day,year,day,hour))
        # If the day file already exists, add the new data to the original file
        else:
            # If the day file already exists, append the new data to the original file
            df_local = pd.read_pickle("{}/{:04d}/{:02d}/{:04d}{:03d}{:02d}.pkl".format(self.folder_path,year,day,year,day,hour))
            df_new_local = pd.concat([df_local,df_new]).drop_duplicates().reset_index(drop=True)
            df_new_local.to_pickle("{}/{:04d}/{:02d}/{:04d}{:03d}{:02d}.pkl".format(self.folder_path,year,day,year,day,hour))
    
    
    # Pull fuel moisture data from mesowest dictionary, update the station.pkl file, and save the mesowest data to pickle files
    #
    # @ Param mesowestData - mesowest data dictionary
    # @ Param day - day of a given month
    # @ Param month - month of a given year
    # @ Param year - given year
    #
    def get_and_save(self,mesowestData,day,month,year):
        # Fills data from mesowest into lists
        data_file = pd.DataFrame([])
        day = datetime.datetime(year,month,day,tzinfo=datetime.timezone.utc).timetuple().tm_yday
        for stData in mesowestData['STATION']:
            df = pd.DataFrame.from_dict(stData['OBSERVATIONS'])
            df.columns = ['datetime','fm10']
            df['STID'] = stData['STID']
            data_file = data_file.append(df)
        
        # Creates/updates the station.pkl file which holds constant values for each station (STID, elevation, latitude, longitude, etc.)
        if osp.exists("{}/{}".format(self.folder_path,'station.pkl')):
            sts_pd = pd.read_pickle("{}/{}".format(self.folder_path,'station.pkl'))    
        # Create empty dataframe for station metadata
        else:
            sts_pd = pd.DataFrame([])
        keys = ['STID','LONGITUDE','LATITUDE','ELEVATION','STATE']
        stids = np.array([station['STID'] for station in mesowestData['STATION']])
        np_meso = np.array(mesowestData['STATION'])
        sts_miss = np_meso[~np.isin(stids,sts_pd.index)]
        sts_pd = sts_pd.append(pd.DataFrame.from_dict({key: [ms[key] for ms in sts_miss] for key in keys}).set_index('STID'))
        sts_pd.to_pickle("{}/{}".format(self.folder_path,'station.pkl'))   
        
        # Save data for each hour of a given day
        data_file['datetime'] = pd.to_datetime(data_file['datetime'])
        for hour in range(0,24):
            hourData = data_file[data_file['datetime'].dt.hour == hour]
            if len(hourData) > 0:
                hourData = hourData.reset_index()
                self.save_to_DB(hourData,hour,day,year)
    
    
    # If the user does not specify the days they want, gets the last month's data
    #
    # @ Param token - mesowest api variable
    # @ Param dayLimit - how many days are in the user specified month
    # @ Param month - user specified month
    # @ Param year - user specified year
    # @ Param siteList - list to hold the names of the site
    # @ Param fuelList - list to hold fuel moisture data
    # @ Param datesList - list to hold datetime data
    #
    def getMonthlyData(self,token,dayLimit,month,year):
        
        currentDay = 1
        while currentDay < dayLimit+1:
            self.getDailyData(token,currentDay,dayLimit,month,year)
            currentDay+=1
    
    
    # If the user specifies the days they want, gets that day's data
    #
    #
    # @ Param token - mesowest api variable
    # @ Param day - user specified day
    # @ Param dayLimit - how many days are in the user specified month
    # @ Param month - user specified month
    # @ Param year - user specified year
    # @ Param siteList - list to hold the names of the site
    # @ Param fuelList - list to hold fuel moisture data
    # @ Param datesList - list to hold datetime data
    #
    def getDailyData(self,token,day,dayLimit,month,year):
    
        override = self.params.get('override')
    
        # If it's the last day of the year, set month to 1 and year + 1. 
        if day == dayLimit:
            if month == 12:
                month2 = 1
                year2 = year+1
            # Else, increment the month by 1
            else:
                month2 = month+1
                year2 = year
            # The following month is always 1
            day2 = 1
        
        # If current month, get given this month's data up until today
        if month == int(datetime.datetime.now(datetime.timezone.utc).month) and year == int(datetime.datetime.now(datetime.timezone.utc).year):
            dayLimit = datetime.datetime.now(datetime.timezone.utc).day
            
        # Check if the julian day folder exists, create it if false
        if self.julian_exists(day,month,year) == True:
            # If override parameter is true, get data from mesowest and append any new data to existing day file (i.e. 01.pkl)
            if override == True:
                
                # if it's the last day of the month (i.e. Jan), make month2 and day2 the first day of the next month (i.e. Feb) to get all the data from the last day of the original month (i.e. Jan)
                if day+1 > dayLimit and datetime.datetime.now().month != month:
                    mesoData = token.timeseries(start="{:04d}{:02d}{:02d}{:04d}".format(year,month,day,0), end = "{:04d}{:02d}{:02d}{:04d}".format(year2,month2,day2,0),state='CA',vars='fuel_moisture')
                    
                # If the next day is in the same month
                elif day+1 <= dayLimit:
                    mesoData = token.timeseries(start="{:04d}{:02d}{:02d}{:04d}".format(year,month,day,0), end = "{:04d}{:02d}{:02d}{:04d}".format(year,month,day+1,0),state='CA',vars='fuel_moisture')
                
                self.get_and_save(mesoData,day,month,year)
                    
            else:
                print('{:02d}/{:02d}/{:04d} file already exists'.format(month,day,year))
                
        else:
            # if it's the last day of the month (i.e. Jan), make month2 and day2 the first day of the next month (i.e. Feb) to get all the data from the last day of the original month (i.e. Jan)
            if day+1 > dayLimit and datetime.datetime.now().month != month:
                mesoData = token.timeseries(start="{:04d}{:02d}{:02d}{:04d}".format(year,month,day,0), end = "{:04d}{:02d}{:02d}{:04d}".format(year2,month2,day2,0),state='CA',vars='fuel_moisture')

            # If the next day is in the same month
            elif day+1 <= dayLimit:
                mesoData = token.timeseries(start="{:04d}{:02d}{:02d}{:04d}".format(year,month,day,0), end = "{:04d}{:02d}{:02d}{:04d}".format(year,month,day+1,0),state='CA',vars='fuel_moisture')
        
            self.get_and_save(mesoData,day,month,year)
            
    
    # Supposed to get last available hour
    def getHourly(self,token):
        
        siteList = []
        fuelList = []
        datesList = []
        latList = []
        lonList = []
        stateList = []
        currentDate = datetime.datetime.now(datetime.timezone.utc)
        currentHour = int(currentDate.hour)
        currentDay = int(currentDate.day)
        currentMonth = int(currentDate.month)
        currentYear = int(currentDate.year)
        # if the previous hour was the last hour from previous day
        if currentHour - 1 == -1:
            # if the previous day was the last day of previous month
            if currentDay - 1 == 0:
                # if the previous month was the last month of previous year 
                if currentMonth - 1 == 0:
                    prevYear = currentYear - 1
                    prevMonth = 12
                    prevDay = self.daysInMonth(prevMonth,prevYear)
                    prevHour = 23
                # if the previous month was in the current year
                else:
                    prevYear = currentYear
                    prevMonth = currentMonth - 1
                    prevDay = self.daysInMonth(prevMonth,prevYear)
                    prevHour = 23
            # if the previous day was in the current month
            else:
                prevYear = currentYear
                prevMonth = currentMonth
                prevDay = currentDay - 1
                prevHour = 23
        # if the previous hour was in the current day
        else:
            prevYear = currentYear
            prevMonth = currentMonth
            prevDay = currentDay
            prevHour = currentHour - 1
    
        # Convert prevHour integer to string
        if prevHour < 10:
            prevHour = '0'+str(prevHour)
        else:
            prevHour = str(prevHour)
        
        if currentHour < 10:
            currentHour = '0' + str(currentHour)
        else:
            currentHour = str(currentHour)
        #prevDay,thisDay,prevMonth,thisMonth = self.prepDT(prevDay,currentDay,prevMonth,currentMonth)
        #fuelData = token.timeseries(start=str(prevYear)+prevMonth+prevDay+prevHour+'00', end=str(currentYear)+thisMonth+thisDay+currentHour+'00', state='CA', vars='fuel_moisture')
        mesoData = token.timeseries(start="{:04d}{:02d}{:02d}{:02d}{:02d}".format(prevYear,prevMonth,prevDay,prevHour,0), end = "{:04d}{:02d}{:02d}{:02d}{:02d}".format(currentYear,currentMonth,currentDay,currentHour,0),state='CA',vars='fuel_moisture')

        self.get_and_save(mesoData,prevDay,prevMonth,prevYear,siteList,fuelList,datesList,latList,lonList,stateList)
    
    
    # Gets fuel data from Mesowest and makes it into a pickle or csv file
    def main(self,days=[0],months=[int(datetime.datetime.now().month)],years=[int(datetime.datetime.now().year)],currentData=False):

        token = self.token
        if currentData == False:
            for year in years:
                self.year_exists(year)
                for month in months:
                    daysInMonth = self.daysInMonth(month,year)
                    if days[0] == 0:
                        self.getMonthlyData(token,daysInMonth,month,year)
                    elif len(days) > 0:
                        for day in days:
                                self.getDailyData(token,day,daysInMonth,month,year)
                    else:
                        print('Invalid Day Input')
                    
        else:
            print('Note to editor: Need to set up for hourly')
            self.year_exists(year)
            self.month_exists(year, month)
            self.getHourly(token)

    # Check coordinates and return properly coordinates
    #
    # @ Param latitude1,latitude2,longitude1,longitude2 - geographical coordinates in WGS84 degrees
    #
    def check_coords(self,latitude1, latitude2, longitude1, longitude2):
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
    
    
    # Gets mesowest data from local database
    #
    def get_db(self): 
        # Load parameters for getting data
        years = self.params.get('year')
        months = self.params.get('month')
        days = self.params.get('day')
        latitude1 = self.params.get('latitude1')
        latitude2 = self.params.get('latitude2')
        longitude1 = self.params.get('longitude1')
        longitude2 = self.params.get('longitude2')
        makeFile = self.params.get('makeFile')
        
        # Check if the coordinates are valid
        lat1,lat2,lon1,lon2 = self.check_coords(latitude1, latitude2, longitude1, longitude2)
        
        # Load station.pkl data and create an empty dataframe which will hold all the data requested
        df_sites = pd.read_pickle("{}/{}".format(self.folder_path,"station.pkl"))
        df_new = pd.DataFrame([])
        # Look at each year given
        for year in years:
            # Look at each month given
            for month in months:
                # Day reset to make sure days is unique for each month
                days = self.params.get('day')
                # If "0" value provided, assume all days for a given month are queried
                if days[0] == 0:
                    print(month)
                    days = list(np.arange(0,self.daysInMonth(month,year))+1)
                # Look at each day given
                print(days)
                for day in days:
                    # If the datetime given is in the future, don't do anything
                    #print(year,month,day)
                    if datetime.datetime(year,month,day,tzinfo=datetime.timezone.utc) <= datetime.datetime.now(datetime.timezone.utc):
                        # Gets the julian day from the day, month, and year
                        jday = datetime.datetime(year,month,day,tzinfo=datetime.timezone.utc).timetuple().tm_yday
                        # Look at each hour on a given day
                        for hour in np.arange(0,24):
                            # If the file does not exist, get it and add it to the df_new dataframe
                            if self.hour_file_exists(hour,jday,year) == False:
                                temp = self.params.get('makeFile')
                                self.params['override'] = True
                                self.getDailyData(self.token,day,self.daysInMonth(month,year),month,year)
                                self.params['override'] = temp
                            
                            # Get the queried data
                            df_local = pd.read_pickle("{}/{:04d}/{:03d}/{:04d}{:03d}{:02d}.pkl".format(self.folder_path,year,jday,year,jday,hour))
                            df_local['datetime'] = pd.to_datetime(df_local['datetime'])
                            # If valid coordinates are given, get data in those bounds
                            if lat1 != None:
                                df_sites['LATITUDE'] = df_sites['LATITUDE'].astype(float)
                                df_sites['LONGITUDE'] = df_sites['LONGITUDE'].astype(float)
                                stidLoc = df_sites[np.logical_and(df_sites['LATITUDE'].between(lat1, lat2, inclusive=True),df_sites['LONGITUDE'].between(lon1, lon2, inclusive=True))].index.values
                                df_new = df_new.append(df_local[df_local['STID'].isin(stidLoc)])
                            # If no coordinates are given, get all the data in date range given
                            else:
                                df_new = df_new.append(df_local)
                                df_new = df_new.reset_index()
                    else:
                        print("{} is not valid".format(datetime.datetime(year,month,day,tzinfo=datetime.timezone.utc)))
        df_new = df_new.reset_index()
        df_new = df_new.drop(['level_0','index'],axis=1)
        return df_new
    
    
# Runs if this is the file being used
#
if __name__ == '__main__':
    
    # Example
    meso = mesoDB('YourTokenGoesHere')
    #meso.main(m,currentData = True) 
    #meso.main(days=[0],months=[12],years=[2020])
    meso.main(days=[0],months=[5],years=[2020])
    
    meso.params['latitude1'] = 37
    meso.params['latitude2'] = 38
    meso.params['longitude1'] = -120
    meso.params['longitude2'] = -122
    meso.params['year'] = [2020]
    meso.params['month'] = [1,5]
    meso.params['day'] = [0]
    testFrame = meso.get_db()
    
    
    