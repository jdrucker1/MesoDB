# Libraries that need to be imported
from mesoDB import mesoDB


# Create database object
#
# The constructor for the database takes two arguments: a Mesowest token and a folder name. Your 
# Mesowest token only needs to be called once and then it will be saved in the database.
# If you do not specify a folder name, a folder named meosDB will be automatically created
# and all queried data will be stored there.
#
db = mesoDB("abc123def456") # Replace "abc123def456" with your Mesowest token
#
## db = mesoDB(token="abc123def456")
#
# To specify a folder name:
#
## db = mesoDB(folder_path="CA_mesoDB")
#
# Combining the arguments for the constructor
#
## db = mesoDB("abc123def456", "CA_mesoDB")
#
## db = mesoDB(token="abc123def456", folder_path="CA_mesoDB")
#
# If you want to just add a Mesowest token to your local database:
#
##db.add_tokens("abc123def456")
#
# Or you can also add multiple tokens at the same time in a list:
#
##db.add_tokens(["abc123def456", "123abc456def"])


# Create/update your local database
#
# By default, this function will get the last 3 hours of data from the United States area.
# The user can specify the state or coordinates that they wish to grab data from, but it is
# recommended to fill the data from the entire United States to maximize the user's mesowest
# token usage
#
db.update_DB()
#
# To specify a state or coordinates to fill the local database, the parameters must be changed.
# For location parameters, country is prioritized first, then states, and lastly coordinates. Each
# prior parameter must be set to "None" in order to specify where the user would want the data from.
# By default, the country paramter is set to "us" and the state/coodinates are set to None.
#
## db.params["country"] = None
## db.params["state"] = "ca"
#
## db.params["country"] = None
## db.params['latitude1'] = 32.
## db.params['latitude2'] = 42.5
## db.params['longitude1'] = -125.
## db.params['longitude2'] = -112.
#
# The user can also specify the datetimes that they want their data from
#
## db.set_start_datetime(2021,6,1) # where the arguments are (year, month, day)
## db.set_end_datetime(2021,6,2)
#
# The user can also specify the hours they want from the function above
#
## db.set_start_datetime(2021,6,1,0) # where the arguments are (year, month, day, hour)
## db.set_end_datetime(2021,6,1,1)


# Get data from the local database.
#
# Default parameters are set for the get_DB function as seen below. If left unchanged, it will grab all of
# the data from the United States in the last 3 hours in the local database.
#
# db.params = {"startTime": startTime, "endTime": endTime, "country": "us", "state": None,
#              "latitude1": None, "latitude2": None, "longitude1": None, "longitude2": None, "makeFile": False}
#
# As seen earlier, the user can change the parameters by:
#
db.params["state"] = "ca"    # This will set the queried data to only be from California
db.params["makeFile"] = True # This will save the data you get into a csv if you set this to True.
#
# Once the user sets the parameters that they want, they can call the get_DB function.
#
mesoFrame = db.get_DB()
#
# If the user has the makeFile paramter as true and just wants the csv file:
#
## db.get_DB()
