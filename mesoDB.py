
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
    def __init__(self, folder_path=osp.join(osp.abspath(os.getcwd()),"mesoDB")):
        self.folder_path = folder_path
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
    def month_exists(self,year,month):
        if osp.exists("{}/{:04d}/{:02d}".format(self.folder_path,year,month)):
            logging.info("mesoDB/year/month - Existent DB path {}/{:04d}/{:02d}".format(self.folder_path,year,month))
        else:
            os.makedirs("{}/{:04d}/{:02d}".format(self.folder_path,year,month))
    
    # Checks if day file exits, if so returns true, else false
    #
    def day_file_exists(self,day,month,year):
        if osp.exists("{}/{:4d}/{:02d}/{:02d}.pkl".format(self.folder_path,year,month,day)):
            logging.info("mesoDB - Existent DB path {}/{:04d}/{:02d}/{:02d}.pkl".format(self.folder_path,year,month,day))
            return True
        else:
            return False


    # Initialize parameters for get_data
    #   
    def init_params(self):
        self.params = {'year': [int(datetime.datetime.now().year)], 'month': [int(datetime.datetime.now().year)], 'day': [0],
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
    #
    def save_to_DB(self,df_new,day,month,year):
        
        # If the day file does not exist, make the folder and save the data
        if self.day_file_exists(day,month,year) == False:
            df_new.to_pickle("{}/{:04d}/{:02d}/{:02d}.pkl".format(self.folder_path,year,month,day))
        # If the day file already exists, add the new data to the original file
        else:
            df_local = pd.read_pickle("{}/{:04d}/{:02d}/{:02d}.pkl".format(self.folder_path,year,month,day))
            df_new_local = pd.concat(df_local,df_new).drop_duplicates().reset_index(drop=True)
            df_new_local.to_pickle("{}/{:04d}/{:02d}/{:02d}.pkl"+day.format(self.folder_path,year,month,day))
    
    
    def get_and_save(self,mesowestData,day,month,year,siteList,fuelList,datesList,latList,lonList,stateList):
        # Fills data from mesowest into lists
        for i in range(len(mesowestData['STATION'])):
            for j in range(len(mesowestData['STATION'][i]['OBSERVATIONS']['date_time'])):
                siteList.append(mesowestData['STATION'][i]['NAME'])
                fuelList.append(mesowestData['STATION'][i]['OBSERVATIONS']['fuel_moisture_set_1'][j])
                datesList.append(mesowestData['STATION'][i]['OBSERVATIONS']['date_time'][j])
                latList.append(mesowestData['STATION'][i]['LATITUDE'])
                lonList.append(mesowestData['STATION'][i]['LONGITUDE'])
                stateList.append(mesowestData['STATION'][i]['STATE'])
                    
        df_new = pd.DataFrame({'Name': siteList, 'dateTime': datesList, 'Latitude':latList, 'Longitude': lonList, 'State': stateList, 'Fuel Moisture': fuelList})
        self.save_to_DB(df_new,day,month,year)
    
    
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
        while currentDay+1 <= dayLimit:
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
    
        # Lists to hold data from mesowest
        siteList = []
        fuelList = []
        datesList = []
        latList = []
        lonList = []
        stateList = [] 
        override = self.params.get('override')
    
        # If it's the last day of the year, set month to 1 and year + 1. 
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
            dayLimit = datetime.datetime.now().day
            
        # Check if the  day file exists, if true check if override parameter is true
        if self.day_file_exists(day, month, year) == True:
            print('check')
            # If override parameter is true, get data from mesowest and append any new data to existing day file (i.e. 01.pkl)
            if override == True:
                
                # if it's the last day of the month (i.e. Jan), make month2 and day2 the first day of the next month (i.e. Feb) to get all the data from the last day of the original month (i.e. Jan)
                if day+1 > dayLimit and datetime.datetime.now().month != month:
                    mesoData = token.timeseries(start="{:04d}{:02d}{:02d}{:04d}".format(year,month,day,0), end = "{:04d}{:02d}{:02d}{:04d}".format(year2,month2,day2,0),state='CA',vars='fuel_moisture')
                    
                # If the next day is in the same month
                elif day+1 <= dayLimit:
                    mesoData = token.timeseries(start="{:04d}{:02d}{:02d}{:04d}".format(year,month,day,0), end = "{:04d}{:02d}{:02d}{:04d}".format(year,month,day+1,0),state='CA',vars='fuel_moisture')
                
                self.get_and_save(mesoData,day,month,year,siteList,fuelList,datesList,latList,lonList,stateList)
                    
            else:
                print('{:02d}.pkl already exists'.format(day))
                
        else:
            print('what')
            # if it's the last day of the month (i.e. Jan), make month2 and day2 the first day of the next month (i.e. Feb) to get all the data from the last day of the original month (i.e. Jan)
            if day+1 > dayLimit and datetime.datetime.now().month != month:
                mesoData = token.timeseries(start="{:04d}{:02d}{:02d}{:04d}".format(year,month,day,0), end = "{:04d}{:02d}{:02d}{:04d}".format(year2,month2,day2,0),state='CA',vars='fuel_moisture')

            # If the next day is in the same month
            elif day+1 <= dayLimit:
                print("{:04d}{:02d}{:02d}{:04d}".format(year,month,day+1,0))
                mesoData = token.timeseries(start="{:04d}{:02d}{:02d}{:04d}".format(year,month,day,0), end = "{:04d}{:02d}{:02d}{:04d}".format(year,month,day+1,0),state='CA',vars='fuel_moisture')
        
            self.get_and_save(mesoData,day,month,year,siteList,fuelList,datesList,latList,lonList,stateList)
            
    
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
    def main(self,token,days=[0],months=[int(datetime.datetime.now().month)],years=[int(datetime.datetime.now().year)],currentData=False):

        if currentData == False:
            for year in years:
                self.year_exists(year)
                for month in months:
                    self.month_exists(year, month)
                    daysInMonth = self.daysInMonth(month,year)
                    if days[0] == 0:
                        self.getMonthlyData(token,daysInMonth,month,year)
                    elif len(days) > 0:
                        for day in days:
                                self.getDailyData(token,day,daysInMonth,month,year)
                    else:
                        print('Invalid Day Input')
                    
        else:
            print('Need to set up for hourly')
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
    def get_db(self): 
        year = self.params.get('year')
        month = self.params.get('month')
        day = self.params.get('day')
        lat1 = self.params.get('latitude1')
        lat2 = self.params.get('latitude2')
        lon1 = self.params.get('longitude1')
        lon2 = self.params.get('longitude2')
        makeFile = self.params.get('makeFile')
        
        lat1,lat2,lon1,lon2 = self.check_coords(lat1, lat2, lon1, lon2)
        for i in year:
            for j in month:
                multipleDays = False
                for k in day:
                    thisMonth = self.which_month(j)
                    print(i)
                    print(thisMonth)
                    df_local = pd.read_pickle(self.folder_path+"/"+str(i)+"/"+thisMonth+'.pkl')
                    df_local['Latitude'] = df_local['Latitude'].astype(float)
                    df_local['Longitude'] = df_local['Longitude'].astype(float)
                    df_local['dateTime'] = pd.to_datetime(df_local['dateTime'])
                    if multipleDays == False:
                        if lat1 != None:
                            df_new = df_local[np.logical_and(df_local['Latitude'].between(lat1, lat2, inclusive=True),df_local['Longitude'].between(lon1, lon2, inclusive=True))]
                            if k != 0:
                                df_new = df_new[df_new['dateTime'].dt.day.between(k-1,k+1, inclusive=True)]
                        else:
                            if k == 0:
                                df_new = df_local
                            else:
                                df_new = df_local[df_local['dateTime'].dt.day.between(k-1,k+1, inclusive=False)]
                    else:
                        if lat1 != None:
                            df_new = pd.concat([df_new,df_local[np.logical_and(df_local['Latitude'].between(lat1, lat2, inclusive=True),df_local['Longitude'].between(lon1, lon2, inclusive=True))]])
                            if k != 0:
                                df_new = df_new[df_new['dateTime'].dt.day.between(k-1,k+1, inclusive=True)]
                        else:
                            if k == 0:
                                df_new = df_local
                            else:
                                df_new = df_local[df_local['dateTime'].dt.day.between(k-1,k+1, inclusive=False)]
                    multipleDays = True
        
        return df_new
    
    
# Runs if this is the file being used
#
if __name__ == '__main__':
    
    # Put your token here
    m = Meso(token='YourMesowestTokenGoesHere')
    meso = mesoDB()
    #meso.main(m,currentData = True) 
    meso.main(token=m,days=[1],months=[12],years=[2020])
    #meso.main(token=m,days=[31],months=[5],years=[2020])
    
    