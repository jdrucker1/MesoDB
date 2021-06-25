# Mesowest Database

The goal of this project is to maximize a user's mesowest token usage and save fuel moisture data that has 
already been requested to a local database.

## Getting Started

### Dependencies

Python 3 and package modules:
* pandas

### Installing Using Anaconda

* Clone Github repository
* Install Anaconda
* Create environment for running the code:
 
      $ conda create -n mesodb python=3 pandas

* Activate the environment
      
      $ conda activate mesodb

### Create A Local Database

* Import the database class by:
```python
from mesoDB import mesoDB
```
* Create a mesoDB object. 

The two arguments for defining a mesoDB object are the path where to save the database (`folder_path`) and the Mesowest token to download the data (`mesoToken`). A Mesowest token can be obtained from [synopticdata page](https://developers.synopticdata.com/).

Note: Users only need to enter their Mesowest tokens once and they will be saved from that time on
```python
db = mesoDB(mesoToken='token') # User's Mesowest token replaces 'token'

# After running the database once with the user's Mesowest token, they no longer need to have their token as a parameter:
db = mesoDB()
```

Note: If no folder name is specified, the default is going to be a folder called `mesoDB` in the current path tree. One can specify a path where the database should be created. For instance:
```python
# Call the database this way on the first use:
db = mesoDB(folder_path = 'FMDB_CA', mesoToken='token')
# or 
db = mesoDB('FMDB_CA', 'token')

# Call the database this way after Mesowest tokens have already been placed in the local database:
db = mesoDB(folder_path = 'FMDB_CA')
# or
db = mesoDB('FMDB_CA')
```
Users can also add one or many Mesowest tokens using the add_tokens function:
```python
# Add a single token
db.add_tokens("abc123def456")

# Add multiple tokens
db.add_tokens(["abc123def456", "123abc456def"])
````


### Create/Update Local Database

Before the user adds data to the database, there are two set of parameters that the user can specify.
The parameters regarding updating the database are defined in db.update dictionary, the once about getting
data from the database are defined in db.params dictionary. Both dictionaries have the same parameters 
except `makeFile` that is only present on db.params. Both dictionaries are also initialized with the same values 
(Default in each field below). If the user does not change the default parameters, the last 3 hours 
of data from the entire United States will be added to the database. These are all the parameters:

* **startTime**: datetime variable. Default: three hours before the present. Example: 2021-6-24 01:05:31.4321.
* **endTime**: datetime variable. Default: the present. Example: 2021-6-24 04:05:31.4321.
* **country**: string value representing a country. Default: United States (case sensitive). Example: "us".
* **state**: string value representing a state (not case sensitive). Default: None. Example: "ca".
* **latitude1**: Float number of the minimum geographical latitude coordinate in WGS84 degrees. Default: None. Example: 36.93.
* **latitude2**: Float number of the maximum geographical latitude coordinate in WGS84 degrees. Default: None. Example: 40.75.
* **longitude1**: Float number of the minimum geographical longitude coordinate in WGS84 degrees. Default: None. Example: -122.43.
* **longitude2**: Float number of the maximum geographical longitude coordinate in WGS84 degrees. Default: None. Example: -118.81.
* **makeFile**: Boolean asserting if create or not a resulting CSV file with the data. Default: False. Example: True. The file is generated in the database path with name depending on time of creation using: {year}{month}{day}{hour}.csv.

All the default parameters are set when the class is initialized as:
```python
now = datetime.datetime.now(datetime.timezone.utc)
self.update = {'startTime': now - datetime.timedelta(hours=3), 'endTime': now, 'country': 'us', 'state': None,
               'latitude1': None, 'latitude2': None, 'longitude1': None, 'longitude2': None}
self.params = {'startTime': now - datetime.timedelta(hours=3), 'endTime': now, 'country': 'us', 'state': None,
               'latitude1': None, 'latitude2': None, 'longitude1': None, 'longitude2': None, 'makeFile': False}
````

To change the parameters, the user can do:
```python
# The start and end time parameters are set using functions to easily allow the user to create a datetime object
that is in UTC time. To change only update parameters:
db.set_start_time_update(2021,6,1)      # where the arguments are (year, month, day)
db.set_end_time_update(2021,6,2)        # where the arguments are (year, month, day)

# The user can also insert the hours they want using the same functions above:
db.set_start_time_update(2021,6,1,1)    # where the arguments are (year, month, day, hour)
db.set_end_time_update(2021,6,2,19)     # where the arguments are (year, month, day, hour)

# The user can also specify the parameters for both dictionaries doing:
db.set_start_time(2021,6,1,1)    # where the arguments are (year, month, day, hour)
db.set_end_time(2021,6,2,19)     # where the arguments are (year, month, day, hour)

# All of the other parameters are set as seen below:
db.update["country"] = None          # This will prevent the database from getting all the data for the country
db.update["state"] = "ca"            # This will set the data to be gathered to be from California
db.update["latitude1"] = -119.2      # This will set one of the geographical coordinates limits
db.update['makeFile'] = True         # This will save the data you get into a CSV file
````

Once the user inputs their parameters, they can query data to their local database. 

Note: Generally, making the data queries less specific maximizes the data added to the database. For example, if the user queries data for California and then later goes back to query all of the data for the United States for the same dates, the database assumes since the files for those dates are already in the system, so the data must already be there and skips acquiribg data for those dates to preserve the Mesowest token usage.

Finally, to update the database with the parameters specified:
```python
db.update_DB()
````

### Query Data From Local Database

When querying data from the user's local database, the user can be more specific in the data that wants compared to when the data is updated into the database. The data is return in a Python Pandas DataFrame and can be further saved into a CSV file using the `makeFile` parameter explained above. For example, if the user queried data for the entire United States from Mesowest, but they only want data from California, now would be when they updated the "state" parameter to use that data. This can be done doing:
```python
db.params["state"] = "ca" 
df = db.get_DB()
```
And for creating the CSV file:
```python
db.params["makeFile"] = True
db.get_DB()
```

## Authors
* jdrucker1
* Fergui
