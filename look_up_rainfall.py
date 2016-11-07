import openpyxl; import os; import datetime
import pandas as pd 
import numpy as np
import forecastio
import json, ast 

os.chdir("/Users/haigangliu/Desktop/data_file")
all_covariates = pd.read_csv("locations_full_length_data.csv", dtype = "unicode")
all_covariates.set_index(["LONGITUDE", "LATITUDE"])

location_names = pd.read_csv("unique_locations.csv", dtype = "unicode")
# location_names = location_names.iloc[0:100,:]
location_names.set_index(["LONGITUDE", "LATITUDE"])


all_covariates.drop(["LATITUDE", "LONGITUDE"], 1, inplace  =True)
merged_table = pd.concat([location_names, all_covariates], axis = 1)
print merged_table.head()
# merged_table.to_csv("data_test_all_together.csv")

all_rainfall_data = pd.read_csv("csv_all_file.csv", dtype = "unicode")

base = datetime.datetime(2015,9,1)
to_date = datetime.datetime(2015,11,1)

# print all_rainfall_data.head()

series_names = []
for i in np.arange(61):
	series_names.append(("prcpday" + str(i) ))
# print series_names


def rainfallFinder(location_name, from_date, to_date):
	data_frame = pd.DataFrame(columns = series_names)

	rainfall = all_rainfall_data[all_rainfall_data["STATION_NAME"] == location_name]
	may = rainfall[pd.to_datetime(rainfall["DATE"]) >= from_date ]
	may = may[pd.to_datetime(may["DATE"]) < to_date ]
	rainfall = may.PRCP


	new_rain = rainfall.reset_index(drop=True)	
	try:

		for i in range(61):
			which_day = "prcpday%d" %i 
			if float(new_rain[i]) < 0:
				data_frame.loc[location_name, which_day] = ""
			else:
				data_frame.loc[location_name, which_day] = new_rain[i]
			print "success"
	except (KeyError, IndexError):
		print "incomplete data"
		pass

	return data_frame

# rainfallFinder("HUNTS BRIDGE SC US", from_date = base, to_date = to_date)


data_cargo = []

for location in location_names.STATION_NAME:
	entry = rainfallFinder(location, from_date = base , to_date = to_date)
	data_cargo.append(entry)

rainfall_data_for_60_days = pd.concat(data_cargo, axis = 0)



rainfall_data_for_60_days.to_csv("aaaaaa.csv")


# all_things = pd.concat([rainfall_data_for_60_days, merged_table], axis = 0)
all_things = pd.merge(rainfall_data_for_60_days, merged_table, left_on = "STATION_NAME", right_on = "STATION_NAME", how = "inner")
all_things.to_csv("allthings.csv")
