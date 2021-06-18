
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
from utils import *

# Mesowest Database Class Error
#
class mesoDBError(Exception):
    pass

# Mesowest Database Class
#
class mesoDB(object):
    
    # mesoDB constructor
    #
    def __init__(self, mesoToken=None, folder_path=osp.join(osp.abspath(os.getcwd()),"mesoDB")):
        self.folder_path = folder_path
        self.stations_path = osp.join(self.folder_path,"stations.pkl")
        self.exists_here(mesoToken)
        self.meso = Meso(token=self.tokens[0])
        self.init_params()
        
       
    # Checks if mesoDB directory and their tokens exists. If not, it is created
    #
    # @ Param token - token to be used or added to the tokens list
    def exists_here(self, token):
        if osp.exists(self.folder_path):
            logging.info("mesoDB - Existent DB path {}".format(self.folder_path))
            tokens_path = osp.join(self.folder_path,'.tokens')
            if osp.exists(tokens_path):
                with open(tokens_path,'r') as f:
                    tokens = f.read()
                self.tokens = [t for t in tokens.split('\n') if t != '']
            else:
                self.tokens = []
        else:
            os.makedirs(self.folder_path)
            self.tokens = []
        if token != None and not token in self.tokens:
            self.tokens.append(token)  
        if not len(self.tokens):
            raise mesoDBError('no tokens were provided or existent in the database.')
        with open(osp.join(self.folder_path,'.tokens'),'w') as f:
            for t in self.tokens:
                f.write(t+'\n')

    # Initialize parameters for get_data
    #   
    def init_params(self):
        # parameters for getting data
        now = datetime.datetime.now(datetime.timezone.utc)
        startTime = now - datetime.timedelta(hours=3)
        endTime = now
        self.params = {'startTime': startTime, 'endTime': endTime, 'country': 'us', 'state': None,
                    'latitude1': None, 'latitude2': None, 'longitude1': None, 'longitude2': None, 'makeFile': False}
        # general parameters
        self.realtime_length = 120 # length of current data in minutes

    # Checks if year folder exists, if not, make it
    #
    # @ Param year - integer with the year
    def year_exists(self,year):
        path = osp.join(self.folder_path,"{:04d}".format(year))
        if osp.exists():
            logging.info("mesoDB/year - Existent DB path {}".format(path))
        else:
            os.makedirs(path)
        
    # Checks if month folder exists, if not, make it
    #
    # @ Param utc_datetime - UTC datetime object
    def julian_exists(self, utc_datetime):
        jday = utc_datetime.timetuple().tm_yday
        path = osp.join(self.folder_path,"{:04d}".format(utc_datetime.year),"{:03d}".format(jday))
        if osp.exists(path):
            logging.info("mesoDB/year/month - Existent DB path {}".format(path))
            return True
        else:
            os.makedirs(path)
            return False
    
    # Checks if day file exits, if so returns true, else false
    #
    # @ Param utc_datetime - UTC datetime object
    def hour_file_exists(self, utc_datetime):
        year = utc_datetime.year
        jday = utc_datetime.timetuple().tm_yday
        hour = utc_datetime.hour
        path = osp.join(self.folder_path,"{:4d}".format(year),"{:03d}".format(jday),"{:04d}{:03d}{:02d}.pkl".format(year, jday, hour))
        if osp.exists(path):
            logging.info("mesoDB - Existent DB path {}".format(path))
            return True
        elif osp.exists(path+"_tmp"):
            return True
        else:
            return False
    
    # Checks if datetime is in realtime interval
    #
    # @ Param utc_datetime - UTC datetime object
    #
    def is_realtime(self, utc_datetime):
        now = datetime.datetime.now(datetime.timezone.utc)
        if now < utc_datetime:
            raise mesoDBError("Time must be defined in the past.")
        return (now - utc_datetime).total_seconds()/60 <= self.realtime_length

    # Save data from mesowest to the local database
    #
    # @ Param data - dataframe with fuel moisture data
    # @ Param sites - dataframe with fuel moisture data
    # @ Param start_utc - start datetime of request at UTC
    # @ Param end_utc - end datetime of request at UTC
    #
    def save_to_DB(self,data,sites,start_utc,end_utc):
        sts_pd = pd.DataFrame([])
        if osp.exists(self.stations_path):
            sts_pd = pd.read_pickle(self.stations_path)  
        sts_pd = sts_pd.append(sites[~sites.index.isin(sts_pd.index)])
        sts_pd.to_pickle(self.stations_path)
        while start_utc <= end_utc:
            data_hour = data[data['datetime'].apply(meso_time) == meso_time(start_utc)]
            year = start_utc.year
            jday = start_utc.timetuple().tm_yday
            hour = start_utc.hour
            hour_path = osp.join(self.folder_path,"{:04d}".format(year),"{:03d}".format(jday),"{:04d}{:03d}{:02d}.pkl".format(year,jday,hour))
            if self.is_realtime(start_utc):
                ensure_dir(hour_path + '_tmp')
                data_hour.to_pickle(hour_path + '_tmp')
            else:
                if osp.exists(hour_path + '_tmp'):
                    os.remove(hour_path + '_tmp')
                ensure_dir(hour_path)
                data_hour.to_pickle(hour_path)
            start_utc += datetime.timedelta(hours=1)

    # Try different tokens for MesoWest
    #
    # @ Param start_utc - start datetime of request at UTC
    # @ Param end_utc - end datetime of request at UTC
    #
    def try_meso(self, start_utc, end_utc):
        ntokens = len(self.tokens)
        country = self.params.get('country')
        state = self.params.get('state')
        lat1,lat2,lon1,lon2 = check_coords(self.params.get('latitude1'), self.params.get('latitude2'), self.params.get('longitude1'), self.params.get('longitude2'))
        try:
            if country != None:
                print(country)
                mesoData = self.meso.timeseries(start=meso_time(start_utc), 
                                                end=meso_time(end_utc), 
                                                country=country,
                                                vars='fuel_moisture')
            elif lat1 == None and country == None:
                mesoData = self.meso.timeseries(start=meso_time(start_utc), 
                                                end=meso_time(end_utc), 
                                                state=state,
                                                vars='fuel_moisture')
            elif country == None and state == None:
                mesoData = self.meso.timeseries(start=meso_time(start_utc), 
                                                end=meso_time(end_utc), 
                                                bbox=[lon1,lat1,lon2,lat2],
                                                vars='fuel_moisture')
            return meso_data_2_df(mesoData) 
        except Exception as e:
            logging.warning("Token 1 failed. Probably full for the month.")
            logging.warning(e)
            for tn,token in enumerate(self.tokens[1:]):
                try:
                    if country != None:
                        print(country)
                        mesoData = Meso(token).timeseries(start=meso_time(start_utc), 
                                                          end=meso_time(end_utc), 
                                                          country=country,
                                                          vars='fuel_moisture')
                        
                    elif lat1 == None and country == None:
                        mesoData = Meso(token).timeseries(start=meso_time(start_utc), 
                                                          end=meso_time(end_utc), 
                                                          state=state,
                                                          vars='fuel_moisture')
                    elif country == None and state == None:
                        mesoData = Meso(token).timeseries(start=meso_time(start_utc), 
                                                          end=meso_time(end_utc), 
                                                          bbox=[lon1,lat1,lon2,lat2],
                                                          vars='fuel_moisture')
                    return meso_data_2_df(mesoData) 
                except Exception as e:
                    if tn < ntokens-2:
                        logging.warning("Token {} failed. Probably full for the month.".format(tn+2))
                        logging.warning(e)
                    else:
                        logging.error(e)
                        raise mesoDBError("All the tokens failed. Please, add a token or try it again later.")

    # Get MesoWest data for time interval
    #
    # @ Param start_utc - start datetime of request at UTC
    # @ Param end_utc - end datetime of request at UTC
    #
    def get_meso_data(self, start_utc, end_utc):
        if (end_utc-start_utc).total_seconds()/3600. > 24:
            tmp_utc = start_utc + datetime.timedelta(days=1)
            data,sites = self.try_meso(start_utc,tmp_utc)
            self.save_to_DB(data,sites,start_utc,tmp_utc)
            self.get_meso_data(tmp_utc,end_utc)
        else:
            data,sites =self.try_meso(start_utc,end_utc)
            self.save_to_DB(data,sites,start_utc,end_utc)
    
    # Updates the local database
    #
    def update_DB(self):
        start_utc = self.params.get('startTime')
        end_utc = self.params.get('endTime')
        tmp_utc = start_utc+datetime.timedelta(hours=1)
        
        while tmp_utc <= end_utc:
            if not self.hour_file_exists(start_utc):
                self.get_meso_data(start_utc, tmp_utc)
            else:
                logging.info("{:04d}{:03d}{:02d}.pkl data already exists".format(start_utc.year,start_utc.timetuple().tm_yday,start_utc.hour))
            start_utc = start_utc + datetime.timedelta(hours=1)
            tmp_utc = start_utc + datetime.timedelta(hours=1)

    # Gets mesowest data from local database
    #
    def get_db(self):
        # Load parameters for getting data
        startTime = self.params.get('startTime')
        endTime = self.params.get('endTime')
        state = self.params.get('state')
        latitude1 = self.params.get('latitude1')
        latitude2 = self.params.get('latitude2')
        longitude1 = self.params.get('longitude1')
        longitude2 = self.params.get('longitude2')
        makeFile = self.params.get('makeFile')
        
        # Check if the coordinates are valid
        lat1,lat2,lon1,lon2 = check_coords(latitude1, latitude2, longitude1, longitude2)
        
        # Load station.pkl data and create an empty dataframe which will hold all the data requested
        df_sites = pd.read_pickle(self.stations_path)
        df_new = pd.DataFrame([])
        
        tmp_utc = startTime + datetime.timedelta(hours=1)
        while tmp_utc <= endTime:
            # If the data is not in the local database, get it
            if self.hour_file_exists(startTime) == False:
                self.get_meso_data(startTime, tmp_utc)
            
            jday = startTime.timetuple().tm_yday
            if osp.exists("{}/{:04d}/{:03d}/{:04d}{:03d}{:02d}.pkl".format(self.folder_path,startTime.year,jday,startTime.year,jday,startTime.hour)):
                df_local = pd.read_pickle("{}/{:04d}/{:03d}/{:04d}{:03d}{:02d}.pkl".format(self.folder_path,startTime.year,jday,startTime.year,jday,startTime.hour))
                df_local['datetime'] = pd.to_datetime(df_local['datetime'])
            elif osp.exists("{}/{:04d}/{:03d}/{:04d}{:03d}{:02d}.pkl_tmp".format(self.folder_path,startTime.year,jday,startTime.year,jday,startTime.hour)):
                df_local = pd.read_pickle("{}/{:04d}/{:03d}/{:04d}{:03d}{:02d}.pkl_tmp".format(self.folder_path,startTime.year,jday,startTime.year,jday,startTime.hour))
                df_local['datetime'] = pd.to_datetime(df_local['datetime'])
            else:
                break
            
            if lat1 != None:
                df_sites['LATITUDE'] = df_sites['LATITUDE'].astype(float)
                df_sites['LONGITUDE'] = df_sites['LONGITUDE'].astype(float)
                stidLoc = df_sites[np.logical_and(df_sites['LATITUDE'].between(lat1, lat2, inclusive=True),df_sites['LONGITUDE'].between(lon1, lon2, inclusive=True))].index.values
                df_new = df_new.append(df_local[df_local['STID'].isin(stidLoc)])
                # If no coordinates are given, get all the data in date range given
            elif state != None:
                stidLoc = df_sites[df_sites['STATE']==State].index.values
                df_new = df_new.append(df_local[df_local['STID'].isin(stidLoc)])
            else:
                df_new = df_new.append(df_local)
                
            startTime = startTime + datetime.timedelta(hours=1)
            tmp_utc = startTime + datetime.timedelta(hours=1)
        
        # Clean up the indexing
        df_new = df_new.reset_index()
        df_new = df_new.drop(['index'],axis=1)
        
        # If makeFile variable is true, create a oickle file with the requested data
        if makeFile == True:
            df_new.to_pickle("{:04d}{:02d}{:02d}{:02d}.pkl".format(datetime.datetime.now().year,datetime.datetime.now().month,datetime.datetime.now().day,datetime.datetime.now().hour))
        
        return df_new
    
    
# Runs if this is the file being used
#
if __name__ == '__main__':
    
    # Example
    meso = mesoDB('ce3ac0a4d004407da62e9c05a96a9daf')
    #m = Meso(token='ce3ac0a4d004407da62e9c05a96a9daf')
    #fuelData = m.timeseries(start='202106180700', end='202106180800', country='us', vars='fuel_moisture')
    meso.params['startTime'] = str_2_dt(2021,6,18,1)
    meso.params['endTime'] = str_2_dt(2021,6,18,2)
    #meso.update_DB()
    #meso.main(m,currentData = True) 
    #meso.main(days=[0],months=[12],years=[2020])
    #meso.main(days=[0],months=[5],years=[2020])
    
    #meso.params['latitude1'] = 37
    #meso.params['latitude2'] = 38
    #meso.params['longitude1'] = -120
    #meso.params['longitude2'] = -122
    #meso.params['startTime'] = str_2_dt(2021,5,1,0)
    #meso.params['endTime'] = str_2_dt(2021,5,2,0)
    #meso.params['makeFile'] = True
    testFrame = meso.get_db()
    
    
    