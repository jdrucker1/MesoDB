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
* Create an mesoDB object seen below. 

Note: Users only need to enter your Mesowest token once and it will be saved from that time on
```python
db = mesoDB(mesoToken) # User's Mesowest token goes here

# After running the database once with the user's Mesowest token:

db = mesoDB()
```

Note: If no folder name is specified, the default is going to be a folder called `mesoDB` in the current path tree. One can specify a path where the database should be created. For instance:
```python
# Call the database this way on the first use:

db = mesoDB(mesoToken, folder_path = 'FMDB_CA')

# Call the database this way on the second use:

db = mesoDB(folder_path = 'FMDB_CA')
```

### Create/Update Local Database

Before the user adds data to the database, there are a few parameters that they can use to choose
which data to query. If the user does not change the default paramters, the last three hours 
of data from the entire United States will be added to the database. There are the parameters:

* **startTime**: datetime variable. Default: three hours before the present. Example: 2021-6-24 01:05:31.4321
* **endTime**: datetime variable. Default: the present. Example: 2021-6-24 04:05:31.4321
* **country**: string value representing a country. Default: United States (case sensitive). Example: "us"
* **state**: string value representing a state (not case sensitive). Default: None. Example: "CA"
* **latitude1**: Float number of the minimum geographical latitude coordinate in WGS84 degrees. Default: None, which did not filter by coordinates. Example: 36.93.
* **latitude2**: Float number of the minimum geographical latitude coordinate in WGS84 degrees. Default: None, which did not filter by coordinates. Example: 40.75.
* **longitude1**: Float number of the minimum geographical longitude coordinate in WGS84 degrees. Default: None, which did not filter by coordinates. Example: -122.43.
* **longitude2**: Float number of the minimum geographical longitude coordinate in WGS84 degrees. Default: None, which did not filter by coordinates. Example: -118.81.
* **makeFile**: Boolean asserting if create or not a resulting CSV file with the data. Default: False. Example: True. The file is generated in the database path with name depending on time of creation using: {year}{month}{day}{hour}.

All the default parameters are set when the class is initialized as:
```python
now = datetime.datetime.now(datetime.timezone.utc)
self.params = {'startTime': now - datetime.timedelta(hours=3), 'endTime': now, 'country': 'us', 'state': None,
               'latitude1': None, 'latitude2': None, 'longitude1': None, 'longitude2': None, 'makeFile': False}
````

To change the parameters, the user can do:
```python
# The start and end time parameters are set using functions to easily allow the user to create a datetime object
that is in utc time:

db.set_start_datetime(2021,6,1) # where the arguments are (year, month, day)
db.set_end_datetime(2021,6,2)   # where the arguments are (year, month, day)

# The user can also insert the hours they want using the same functions above:

db.set_start_datetime(2021,6,1,1) # where the arguments are (year, month, day, hour)
db.set_end_datetime(2021,6,2,19)   # where the arguments are (year, month, day, hour)

# All of the other parameters are set as seen below:

db.params["country"] = None        # This will prevent the database from getting all the data for the country
db.params["state"] = "ca"          # This will set the data to be gathered to be from California
db.params["latitude1"] = -119.2    # This will set one of the geographical coordinates limits
db.params['makeFile'] = True       # This will save the data you get into a CSV file
````

Once the user inputs their parameters, they can query data to their local database. 

Note: Generally, making the data queries less specific maximizes the data added to the database. For example, If  the user queries data for California and then later goes back to query all of the data for the United States for  the same dates, the database assumes since the files for those dates are already in the system, so the data must  already be there and skips acquiribg data for those dates to preserve the Mesowest token usage:
```python
db.update_DB()
````

### Query Data From The User's Local Database

When querying data from the user's local database, they should be more specific in the data they want compared to when they queried data into the database. For example, if the user queried data for the entire United States from Mesowest, but they only want data from California, now would be when they updated the "state" parameter to use that data (see below):
```python
db.params["state"] = "ca" 
 db.get_DB()
```

## Authors
* jdrucker1
* Fergui
