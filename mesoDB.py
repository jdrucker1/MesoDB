
# MesoDB Script

# Libraries
#
import datetime
import logging
from MesoPy import Meso
import numpy as np
import os.path as osp
import os
import glob
import pandas as pd
try:
    from .utils import *
except:
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
    def __init__(self, mesoToken=[], folder_path=osp.join(osp.abspath(os.getcwd()),'mesoDB')):
        self.folder_path = folder_path
        self.stations_path = osp.join(self.folder_path,'stations.pkl')
        self.exists_here(mesoToken)
        self.meso = Meso(token=self.tokens[0])
        self.init_params()
        

    # Add tokens to the mesoDB database
    #
    # @ Param tokens - string or list of tokens
    #
    def add_tokens(self, userTokens):
        tokens_path = osp.join(self.folder_path,'.tokens')
        if osp.exists(tokens_path):
            with open(tokens_path,'r') as f:
                tokens = f.read()
            self.tokens = [t for t in tokens.split('\n') if t != '']
        else:
            self.tokens = []
        if isinstance(userTokens,str) and userTokens not in self.tokens:
            self.tokens.append(userTokens)
        elif isinstance(userTokens,list):
            for token in userTokens:  
                if token not in self.tokens:
                    self.tokens.append(token)
        if len(self.tokens) > 0:
            with open(tokens_path,'w') as f:
                for t in self.tokens:
                    f.write(t+'\n')

    # Checks if mesoDB directory and their tokens exists. If not, it is created
    #
    # @ Param token - token to be used or added to the tokens list
    #
    def exists_here(self, token):
        if not osp.exists(self.folder_path):
            os.makedirs(self.folder_path)
            self.tokens = []
        else:
            logging.info('mesoDB.exists_here - Existent DB path {}'.format(self.folder_path))
        
        self.add_tokens(token)

        if not len(self.tokens):
            raise mesoDBError('mesoDB.exists_here - no tokens were provided or existent in the database.')

    # Get sites processed by the database
    #
    def sites(self):
        if osp.exists(self.stations_path):
            return pd.read_pickle(self.stations_path)
        return pd.DataFrame([])

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

    # Return path to the julian folder
    #
    # @ Param utc_datetime - UTC datetime object
    #
    def julian_path(self, utc_datetime):
        year = utc_datetime.year
        jday = utc_datetime.timetuple().tm_yday
        path = osp.join(self.folder_path,'{:4d}'.format(year),'{:03d}'.format(jday))
        return path

    # Return path to the hour pickle file
    #
    # @ Param utc_datetime - UTC datetime object
    #
    def hour_path(self, utc_datetime):
        year = utc_datetime.year
        jday = utc_datetime.timetuple().tm_yday
        hour = utc_datetime.hour
        path = osp.join(self.julian_path(utc_datetime),'{:04d}{:03d}{:02d}.pkl'.format(year, jday, hour))
        return path
        
    # Checks if month folder exists, if not, make it
    #
    # @ Param utc_datetime - UTC datetime object
    #
    def julian_exists(self, utc_datetime):
        return osp.exists(self.julian_path(utc_datetime))

    # Checks if day is empty, if so returns true, else false
    #
    # @ Param utc_datetime - UTC datetime object
    #
    def day_is_empty(self, utc_datetime):
        if self.julian_exists(utc_datetime):
            path = osp.join(self.julian_path(utc_datetime), '*.pkl')
            return len(glob.glob(path)) == 0
        return True

    # Checks if day is full, if so returns true, else false
    #
    # @ Param utc_datetime - UTC datetime object
    #
    def day_is_full(self, utc_datetime):
        if self.julian_exists(utc_datetime):
            path = osp.join(self.julian_path(utc_datetime), '*.pkl')
            return len(glob.glob(path)) == 24
        return False

    # Checks if hour file exits, if so returns true, else false
    #
    # @ Param utc_datetime - UTC datetime object
    #
    def hour_file_exists(self, utc_datetime):
        return osp.exists(self.hour_path(utc_datetime))
    
    # Checks if datetime is in realtime interval
    #
    # @ Param utc_datetime - UTC datetime object
    #
    def is_realtime(self, utc_datetime):
        now = datetime.datetime.now(datetime.timezone.utc)
        if now < utc_datetime:
            raise mesoDBError('mesoDB.is_realtime - time must be defined in the past.')
        return (now - utc_datetime).total_seconds()/60 <= self.realtime_length

    # Save data from mesowest to the local database
    #
    # @ Param data - dataframe with fuel moisture data
    # @ Param sites - dataframe with fuel moisture data
    # @ Param start_utc - start datetime of request at UTC
    # @ Param end_utc - end datetime of request at UTC
    #
    def save_to_DB(self, data, sites, start_utc, end_utc):
        logging.debug('mesoDB.save_to_DB - updating stations')
        sts_pd = self.sites()
        sts_pd = sts_pd.append(sites[~sites.index.isin(sts_pd.index)])
        sts_pd.to_pickle(self.stations_path)
        while start_utc < end_utc:
            data_hour = data[data['datetime'].apply(meso_time) == meso_time(start_utc)]
            hour_path = self.hour_path(start_utc)
            if self.is_realtime(start_utc):
                ensure_dir(hour_path + '_tmp')
                data_hour.reset_index(drop=True).to_pickle(hour_path + '_tmp')
            else:
                ensure_dir(hour_path)
                data_hour.reset_index(drop=True).to_pickle(hour_path)
                if osp.exists(hour_path + '_tmp'):
                    os.remove(hour_path + '_tmp')
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
            logging.debug('mesoDB.run_meso - retrieving for country={}'.format(country))
            mesoData = self.meso.timeseries(start=meso_time(start_utc), 
                                            end=meso_time(end_utc), 
                                            country=country,
                                            vars='fuel_moisture')
        elif state != None:
            logging.debug('mesoDB.run_meso - retrieving for state={}'.format(state))
            mesoData = self.meso.timeseries(start=meso_time(start_utc), 
                                            end=meso_time(end_utc), 
                                            state=state,
                                            vars='fuel_moisture')
        elif lat1 != None:
            bbox = [lon1,lat1,lon2,lat2]
            logging.debug('mesoDB.run_meso - retrieving for bbox={}'.format(bbox))
            mesoData = self.meso.timeseries(start=meso_time(start_utc), 
                                            end=meso_time(end_utc), 
                                            bbox=bbox,
                                            vars='fuel_moisture')
        else:
            logging.debug('mesoDB.run_meso - retrieving for country={}'.format(country))
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
        logging.debug('mesoDB.try_meso - getting data from {} to {}'.format(start_utc, end_utc))
        try:
            mesoData = self.run_meso(start_utc, end_utc)
            logging.info('mesoDB.try_meso - re-packing data from {} to {}'.format(start_utc, end_utc))
            return meso_data_2_df(mesoData) 
        except Exception as e:
            logging.warning('mesoDB.try_meso - token 1 failed, probably full for the month, check usage at https://myaccount.synopticdata.com/#payments.')
            logging.warning('mesoDB.try_meso - exception: {}'.format(e))
            for tn,token in enumerate(self.tokens[1:]):
                try:
                    self.meso = Meso(token)
                    mesoData = self.run_meso(start_utc, end_utc)
                    logging.info('mesoDB.try_meso - re-packing data from {} to {}'.format(start_utc, end_utc))
                    return meso_data_2_df(mesoData) 
                except Exception as e:
                    logging.warning('mesoDB.try_meso - token {} failed, probably full for the month, check usage at https://myaccount.synopticdata.com/#payments.'.format(tn+2))
                    logging.warning('mesoDB.try_meso - exception: {}'.format(e))
            raise mesoDBError('mesoDB.try_meso - all the tokens failed. Please, add a token or try it again later.')

    # Get MesoWest data for time interval hourly
    #
    # @ Param start_utc - start datetime of request at UTC
    # @ Param end_utc - end datetime of request at UTC
    #
    def get_meso_data_hourly(self, start_utc, end_utc):
        if (end_utc-start_utc).total_seconds() > 3600.: # larger than an hour, recursion
            tmp_utc = start_utc + datetime.timedelta(hours=1)
            if self.hour_file_exists(start_utc):
                logging.info('mesoDB.get_meso_data_hourly - {} data already exists'.format(self.hour_path(start_utc)))
            else:
                logging.info('mesoDB.get_meso_data_hourly - updating database from {} to {}'.format(start_utc, tmp_utc))
                data,sites = self.try_meso(start_utc,tmp_utc)
                logging.info('mesoDB.get_meso_data_hourly - saving data from {} to {}'.format(start_utc, tmp_utc))
                self.save_to_DB(data,sites,start_utc,tmp_utc)
            self.get_meso_data_hourly(tmp_utc,end_utc)
        else: # else, no recursion
            if self.hour_file_exists(start_utc):
                logging.info('mesoDB.get_meso_data_hourly - {} data already exists'.format(self.hour_path(start_utc)))
            else:
                logging.info('mesoDB.get_meso_data_hourly - updating database from {} to {}'.format(start_utc, end_utc))
                data,sites = self.try_meso(start_utc,end_utc)
                logging.info('mesoDB.get_meso_data_hourly - saving data from {} to {}'.format(start_utc, end_utc))
                self.save_to_DB(data,sites,start_utc,end_utc)

    # Get MesoWest data for time interval daily
    #
    # @ Param start_utc - start datetime of request at UTC
    # @ Param end_utc - end datetime of request at UTC
    #
    def get_meso_data_daily(self, start_utc, end_utc):
        if (end_utc-start_utc).total_seconds()/3600. > 24.: # larger than a day, recursion
            tmp_utc = start_utc + datetime.timedelta(days=1)
            if self.day_is_empty(start_utc):
                logging.info('mesoDB.get_meso_data_daily - updating database from {} to {}'.format(start_utc, tmp_utc))
                data,sites = self.try_meso(start_utc,tmp_utc)
                logging.info('mesoDB.get_meso_data_daily - saving data from {} to {}'.format(start_utc, tmp_utc))
                self.save_to_DB(data,sites,start_utc,tmp_utc)
            elif not self.day_is_full(start_utc):
                self.get_meso_data_hourly(start_utc,tmp_utc)
            else:
                logging.info('mesoDB.get_meso_data_daily - {} complete day already exists'.format(self.julian_path(start_utc)))
            self.get_meso_data_daily(tmp_utc,end_utc)
        else: # else, no recursion
            if self.day_is_empty(start_utc):
                logging.info('mesoDB.get_meso_data_daily - updating database from {} to {}'.format(start_utc, end_utc))
                data,sites = self.try_meso(start_utc,end_utc)
                logging.info('mesoDB.get_meso_data_daily - saving data from {} to {}'.format(start_utc, end_utc))
                self.save_to_DB(data,sites,start_utc,end_utc)
            elif not self.day_is_full(start_utc):
                self.get_meso_data_hourly(start_utc,end_utc)
            else:
                logging.info('mesoDB.get_meso_data_daily - {} complete day already exists'.format(self.julian_path(start_utc)))
    
    # Updates the local database
    #
    # @ Param start_utc - start datetime of request at UTC
    # @ Param end_utc - end datetime of request at UTC
    #
    def update_DB(self):
        start_utc = self.params.get('startTime')
        end_utc = self.params.get('endTime')
        if start_utc is None or end_utc is None or (end_utc-start_utc).total_seconds() <= 60:
            raise mesoDBError('mesoDB.update_DB - times specified are incorrect or time inteval too small')
        logging.info('mesoDB.update_DB - updating data from {} to {}'.format(start_utc, end_utc))
        tmp_start = (start_utc + datetime.timedelta(days=1)).replace(hour=0,minute=0,second=0,microsecond=0)
        if end_utc < tmp_start:
            self.get_meso_data_hourly(start_utc, end_utc)
            start_utc = end_utc
        else:
            if start_utc.hour != 0:
                self.get_meso_data_hourly(start_utc, tmp_start)
                start_utc = tmp_start
        tmp_end = end_utc.replace(hour=0,minute=0,second=0,microsecond=0)
        if (tmp_end-start_utc).total_seconds() > 60:
            self.get_meso_data_daily(start_utc, tmp_end)
        if (end_utc-tmp_end).total_seconds() > 60:
            self.get_meso_data_hourly(tmp_end, end_utc)

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

        # Update database
        self.update_DB()

        # Load station.pkl data and filter user options
        df_sites = self.sites()
        if lat1 != None:
            df_sites['LATITUDE'] = df_sites['LATITUDE'].astype(float)
            df_sites['LONGITUDE'] = df_sites['LONGITUDE'].astype(float)
            stidLoc = df_sites[np.logical_and(df_sites['LATITUDE'].between(lat1, lat2, inclusive=True),
                                            df_sites['LONGITUDE'].between(lon1, lon2, inclusive=True))].index.values
        elif state != None:
            stidLoc = df_sites[df_sites['STATE'].str.lower() == state.lower()].index.values
        # Create an empty dataframe which will hold all the data requested
        data = []
        
        # Create temporary times to specify files that need to be read
        tmp_start = startTime.replace(minute=0,second=0,microsecond=0)
        tmp_end = (endTime+datetime.timedelta(hours=1)).replace(minute=0,second=0,microsecond=0)
        # While not at the end of time interval
        while tmp_start < tmp_end:
            # Read local file with data
            path = self.hour_path(tmp_start)
            if self.hour_file_exists(tmp_start):
                df_local = pd.read_pickle(path)
            elif osp.exists(path + "_tmp"):
                df_local = pd.read_pickle(path + "_tmp")
            else:
                logging.warning('mesoDB.get_DB - could not find data for time {}'.format(tmp_start))
                continue
            # Filter user options
            if lat1 != None:
                data.append(df_local[df_local['STID'].isin(stidLoc)])
            elif state != None:
                data.append(df_local[df_local['STID'].isin(stidLoc)])
            else:
                data.append(df_local)
            # Go to next hour
            tmp_start = tmp_start + datetime.timedelta(hours=1)
        
        # Make sure there are no dates outside of interval (starting and ending minutes) and cleanup index
        df_final = pd.concat(data)
        df_final = df_final[df_final['datetime'].between(startTime, endTime, inclusive=True)].reset_index(drop=True)
        
        # If makeFile variable is true, create a pickle file with the requested data
        if makeFile:
            now_datetime = datetime.datetime.now()
            filename = '{:04d}{:02d}{:02d}{:02d}.pkl'.format(now_datetime.year,now_datetime.month,
                                                                now_datetime.day,now_datetime.hour)
            df_final.to_pickle(osp.join(self.folder_path,filename))
        
        return df_final
    
    
# Runs if this is the file being used
#
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    token = input('What token do you want to use?')
    meso = mesoDB(token)
    meso.update_DB()