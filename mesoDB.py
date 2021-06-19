
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
import glob
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
    #
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

    # Set start UTC datetime from integers
    #
    # @ Param year - user specified year
    # @ Param month - user specified month
    # @ Param day - user specified day
    # @ Param hour - user specified hour
    #
    def set_start_datetime(self, year, month, day, hour = 0):
        self.params['startTime'] = set_utc_datetime(year, month, day, hour)

    # Set end UTC datetime from integers
    #
    # @ Param year - user specified year
    # @ Param month - user specified month
    # @ Param day - user specified day
    # @ Param hour - user specified hour
    #
    def set_end_datetime(self, year, month, day, hour = 0):
        self.params['endTime'] = set_utc_datetime(year, month, day, hour)

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
    #
    def year_exists(self,year):
        path = osp.join(self.folder_path,"{:04d}".format(year))
        if osp.exists():
            logging.info("mesoDB/year - Existent DB path {}".format(path))
        else:
            os.makedirs(path)
        
    # Checks if month folder exists, if not, make it
    #
    # @ Param utc_datetime - UTC datetime object
    #
    def julian_exists(self, utc_datetime):
        jday = utc_datetime.timetuple().tm_yday
        path = osp.join(self.folder_path,"{:04d}".format(utc_datetime.year),"{:03d}".format(jday))
        if osp.exists(path):
            logging.info("mesoDB/year/month - Existent DB path {}".format(path))
            return True
        return False
    
    # Checks if day is empty, if so returns true, else false
    #
    # @ Param utc_datetime - UTC datetime object
    #
    def day_is_empty(self, utc_datetime):
        if self.julian_exists(utc_datetime):
            year = utc_datetime.year
            jday = utc_datetime.timetuple().tm_yday
            hour = utc_datetime.hour
            return len(glob.glob(osp.join(self.folder_path,"{:4d}".format(year),"{:03d}".format(jday),"*.pkl"))) == 0
        return True

    # Checks if day is full, if so returns true, else false
    #
    # @ Param utc_datetime - UTC datetime object
    #
    def day_is_full(self, utc_datetime):
        if self.julian_exists(utc_datetime):
            year = utc_datetime.year
            jday = utc_datetime.timetuple().tm_yday
            hour = utc_datetime.hour
            return len(glob.glob(osp.join(self.folder_path,"{:4d}".format(year),"{:03d}".format(jday),"*.pkl"))) == 24
        return False

    # Checks if hour file exits, if so returns true, else false
    #
    # @ Param utc_datetime - UTC datetime object
    #
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
    def save_to_DB(self, data, sites, start_utc, end_utc):
        logging.debug('updating stations')
        sts_pd = pd.DataFrame([])
        if osp.exists(self.stations_path):
            sts_pd = pd.read_pickle(self.stations_path)  
        sts_pd = sts_pd.append(sites[~sites.index.isin(sts_pd.index)])
        sts_pd.to_pickle(self.stations_path)
        while start_utc < end_utc:
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

    # Call MesoWest
    #
    # @ Param start_utc - start datetime of request at UTC
    # @ Param end_utc - end datetime of request at UTC
    #
    def run_meso(self, start_utc, end_utc):
        country = self.params.get('country')
        state = self.params.get('state')
        lat1,lat2,lon1,lon2 = check_coords(self.params.get('latitude1'), self.params.get('latitude2'), self.params.get('longitude1'), self.params.get('longitude2'))
        if country != None:
            logging.info('retrieving for country={}'.format(country))
            mesoData = self.meso.timeseries(start=meso_time(start_utc), 
                                            end=meso_time(end_utc), 
                                            country=country,
                                            vars='fuel_moisture')
        elif state != None:
            logging.info('retrieving for state={}'.format(state))
            mesoData = self.meso.timeseries(start=meso_time(start_utc), 
                                            end=meso_time(end_utc), 
                                            state=state,
                                            vars='fuel_moisture')
        elif lat1 != None:
            bbox = [lon1,lat1,lon2,lat2]
            logging.info('retrieving for bbox={}'.format(bbox))
            mesoData = self.meso.timeseries(start=meso_time(start_utc), 
                                            end=meso_time(end_utc), 
                                            bbox=bbox,
                                            vars='fuel_moisture')
        else:
            logging.info('retrieving for country={}'.format(country))
            mesoData = self.meso.timeseries(start=meso_time(start_utc), 
                                            end=meso_time(end_utc), 
                                            country='us',
                                            vars='fuel_moisture')
        return mesoData

    # Try different tokens for MesoWest
    #
    # @ Param start_utc - start datetime of request at UTC
    # @ Param end_utc - end datetime of request at UTC
    #
    def try_meso(self, start_utc, end_utc):
        logging.info('getting data from {} to {}'.format(start_utc,end_utc))
        ntokens = len(self.tokens)
        try:
            mesoData = self.run_meso(start_utc, end_utc)
            logging.info('packing data from {} to {}'.format(start_utc,end_utc))
            return meso_data_2_df(mesoData) 
        except Exception as e:
            logging.warning("Token 1 failed. Probably full for the month.")
            logging.warning(e)
            for tn,token in enumerate(self.tokens[1:]):
                try:
                    mesoData = self.run_meso(start_utc, end_utc)
                    logging.info('packing data from {} to {}'.format(start_utc,end_utc))
                    return meso_data_2_df(mesoData) 
                except Exception as e:
                    if tn < ntokens-2:
                        logging.warning("Token {} failed. Probably full for the month.".format(tn+2))
                        logging.warning(e)
                    else:
                        logging.error(e)
                        raise mesoDBError("All the tokens failed. Please, add a token or try it again later.")

    # Get MesoWest data for time interval hourly
    #
    # @ Param start_utc - start datetime of request at UTC
    # @ Param end_utc - end datetime of request at UTC
    #
    def get_meso_data_hourly(self, start_utc, end_utc):
        year = start_utc.year
        jday = start_utc.timetuple().tm_yday
        hour = start_utc.hour
        if (end_utc-start_utc).total_seconds()/60. > 60.:
            tmp_utc = start_utc + datetime.timedelta(hours=1)
            if self.hour_file_exists(start_utc):
                logging.info("{:04d}{:03d}{:02d}.pkl data already exists".format(year,jday,hour))
            else:
                logging.info('updating database hourly from {} to {}'.format(start_utc, tmp_utc))
                data,sites = self.try_meso(start_utc,tmp_utc)
                logging.info('saving data from {} to {}'.format(start_utc,end_utc))
                self.save_to_DB(data,sites,start_utc,tmp_utc)
            self.get_meso_data_hourly(tmp_utc,end_utc)
        else:
            if self.hour_file_exists(start_utc):
                logging.info("{:04d}{:03d}{:02d}.pkl data already exists".format(year,jday,hour))
            else:
                logging.info('updating database hourly from {} to {}'.format(start_utc, end_utc))
                data,sites = self.try_meso(start_utc,end_utc)
                logging.info('saving data from {} to {}'.format(start_utc,end_utc))
                self.save_to_DB(data,sites,start_utc,end_utc)

    # Get MesoWest data for time interval daily
    #
    # @ Param start_utc - start datetime of request at UTC
    # @ Param end_utc - end datetime of request at UTC
    #
    def get_meso_data_daily(self, start_utc, end_utc):
        if (end_utc-start_utc).total_seconds()/3600. > 24:
            tmp_utc = start_utc + datetime.timedelta(days=1)
            if self.day_is_empty(start_utc):
                logging.info('updating database daily from {} to {}'.format(start_utc, tmp_utc))
                data,sites = self.try_meso(start_utc,tmp_utc)
                logging.info('saving data from {} to {}'.format(start_utc,end_utc))
                self.save_to_DB(data,sites,start_utc,tmp_utc)
            elif not self.day_is_full(start_utc):
                self.get_meso_data_hourly(start_utc,tmp_utc)
            self.get_meso_data_daily(tmp_utc,end_utc)
        else:
            if self.day_is_empty(start_utc):
                logging.info('updating database daily from {} to {}'.format(start_utc, end_utc))
                data,sites = self.try_meso(start_utc,end_utc)
                logging.info('saving data from {} to {}'.format(start_utc,end_utc))
                self.save_to_DB(data,sites,start_utc,end_utc)
            elif not self.day_is_full(start_utc):
                self.get_meso_data_hourly(start_utc,end_utc)
    
    # Updates the local database
    #
    # @ Param start_utc - start datetime of request at UTC
    # @ Param end_utc - end datetime of request at UTC
    #
    def update_DB(self):
        start_utc = self.params.get('startTime')
        end_utc = self.params.get('endTime')
        if start_utc is None or end_utc is None:
            raise mesoDBError('times specified are incorrect')
        if start_utc.hour == 0 and end_utc.hour == 23:
            self.get_meso_data_daily(start_utc, end_utc)
        else:
            if start_utc.hour != 0:
                tmp_start = (start_utc + datetime.timedelta(days=1)).replace(hour=0,minute=0,second=0,microsecond=0)
                self.get_meso_data_hourly(start_utc,tmp_start)
                start_utc = tmp_start
            tmp_end = end_utc.replace(hour=0,minute=0,second=0,microsecond=0)
            if (tmp_end-start_utc).total_seconds() > 60:
                self.get_meso_data_daily(start_utc,tmp_end)
            if (end_utc-tmp_end).total_seconds() > 60:
                self.get_meso_data_hourly(tmp_end,end_utc)

    # Gets mesowest data from local database
    #
    def get_DB(self):
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
            if not self.hour_file_exists(startTime):
                self.get_meso_data_hourly(startTime, tmp_utc)
            
            jday = startTime.timetuple().tm_yday
            path = osp.join(self.folder_path,"{:04d}".format(startTime.year),"{:03d}".format(jday),"{:04d}{:03d}{:02d}.pkl".format(startTime.year,jday,startTime.hour))
            if self.hour_file_exists(startTime):
                df_local = pd.read_pickle(path)
                df_local['datetime'] = pd.to_datetime(df_local['datetime'])
            elif osp.exists(path+ "_tmp"):
                df_local = pd.read_pickle(path+ "_tmp")
                df_local['datetime'] = pd.to_datetime(df_local['datetime'])
            else:
                startTime = startTime + datetime.timedelta(hours=1)
                tmp_utc = startTime + datetime.timedelta(hours=1)
                break
            
            if lat1 != None:
                df_sites['LATITUDE'] = df_sites['LATITUDE'].astype(float)
                df_sites['LONGITUDE'] = df_sites['LONGITUDE'].astype(float)
                stidLoc = df_sites[np.logical_and(df_sites['LATITUDE'].between(lat1, lat2, inclusive=True),
                                                df_sites['LONGITUDE'].between(lon1, lon2, inclusive=True))].index.values
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
        df_new = df_new.reset_index(drop=True)
        
        # If makeFile variable is true, create a pickle file with the requested data
        if makeFile == True:
            now_datetime = datetime.datetime.now()
            df_new.to_pickle("{:04d}{:02d}{:02d}{:02d}.pkl".format(now_datetime.year,now_datetime.month,
                                                                now_datetime.day,now_datetime.hour))
        
        return df_new
    
    
# Runs if this is the file being used
#
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    #token = input('What token do you want to use?')
    #meso = mesoDB(token)
    meso = mesoDB()
    meso.set_start_datetime(2021,6,17)
    #meso.set_end_datetime(2021,6,18,2)
    meso.update_DB()
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
    #testFrame = meso.get_DB()