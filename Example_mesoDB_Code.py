# Libraries that need to be imported
from mesoDB import mesoDB


# CREATE DATABASE OBJECT
#
# The constructor for the database takes two arguments: a folder name path (folder_path) for the databse 
# and a Mesowest token (mesoToken). If you do not specify a folder name, a folder named mesoDB will be 
# automatically created and all queried data will be stored there. Your Mesowest token only needs to be 
# called once and then it will be saved in the database.
#
db = mesoDB(mesoToken="abc123def456") # Replace "abc123def456" with your Mesowest token
#
# To specify a folder name:
#
## db = mesoDB(folder_path="CA_mesoDB")
#
# Combining the arguments for the constructor
#
## db = mesoDB(folder_path="CA_mesoDB", mesoToken="abc123def456")
#
## db = mesoDB("CA_mesoDB", "abc123def456")
#
# If you want to just add a Mesowest token to your local database:
#
##db.add_tokens("abc123def456")
#
# Or you can also add multiple tokens at the same time in a list:
#
##db.add_tokens(["abc123def456", "123abc456def"])


# CREATE/UPDATE YOUR LOCAL DATABASE
#
# By default, the update function will get the last 3 hours of data from the United States area.
# The user can specify the state or coordinates that they wish to grab data from, but it is
# recommended to fill the data from the entire United States to maximize the user's Mesowest
# token usage
#
db.update_DB()
#
# To specify a state or coordinates to fill the local database, the parameters must be changed.
# For location parameters, country is prioritized first, then states, and lastly coordinates. Each
# prior parameter must be set to None in order to specify where the user would want the data from.
# By default, the country parameter is set to "us" and the state/coodinates are set to None.
# The coordinates can be defined using the lower-left and upper-right corner coordinates in WGS84
# latitude and logitude degrees.
#
## db.params["country"] = None
## db.params["state"] = "ca"
#
## db.params["country"] = None
## db.params["latitude1"] = 32.      # lower-left latitude (minimum latitude)
## db.params["latitude2"] = 42.5     # upper-right latitude (maximum latitude)
## db.params["longitude1"] = -125.   # lower-left longitude (minimum longitude)
## db.params["longitude2"] = -112.   # lower-left corner latitude (maximum longitude)
#
# The user can also specify the start and end times that they want their data from doing:
#
## db.set_start_datetime(2021,6,1) # where the arguments are (year, month, day)
## db.set_end_datetime(2021,6,2)
#
# The user can also specify particular hours that they want doing:
#
## db.set_start_datetime(2021,6,1,0) # where the arguments are (year, month, day, hour)
## db.set_end_datetime(2021,6,1,1)


# GET DATA FROM THE DATABASE
#
# This method is meant to be used to return the data that the user wants. If the data is not in the database,
# it will be updated first and returned in python pandas dataframe format. If the data is already in the database,
# it will be directly grabbed from the database. Default parameters are set for the get_DB function as:
#
# db.params = {"startTime": now - datetime.timedelta(hours=3), "endTime": now, "country": "us", "state": None,
#              "latitude1": None, "latitude2": None, "longitude1": None, "longitude2": None, "makeFile": False}
#
# If left unchanged, it will grab all of the data from the United States in the last 3 hours. As seen earlier, 
# the user can change the parameters by:
#
db.params["state"] = "ca"    # This will set the queried data to only be from California
db.params["makeFile"] = True # This will save the data you get into a csv if you set this to True.
#
# Once the user sets the parameters that they want, they can call the get_DB function.
#
mesoFrame = db.get_DB()
#
# If the user just wants a CSV file with the results:
#
##db.params["makeFile"] = True      # This will set the queried data to only be from California
##db.get_DB()
