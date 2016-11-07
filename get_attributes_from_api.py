import openpyxl; import os; import datetime
import pandas as pd 
import numpy as np
import forecastio
import json, ast 

os.chdir("/Users/haigangliu/Desktop/data_file")
locations_and_names = pd.read_csv("unique_locations.csv", dtype = "unicode")
api_key = "fa3dd5b205b7c1ba2a3f23e1499e0322511"
working_dir = "/Users/haigangliu/Desktop"; 
attributes = ["LONGITUDE", "LATITUDE" ,"temperatureMax", "temperatureMin", "visibility", "pressure", "humidity", "windSpeed"]


def recordGetter(longitude_info, latitude_info, date):
	forecast = forecastio.load_forecast(api_key, latitude_info, longitude_info, time = date, units="si")
	daily = forecast.daily()
	records = daily.data[0].d
	records_clean = ast.literal_eval(json.dumps(records))
	records_clean["LONGITUDE"] = longitude_info; records_clean["LATITUDE"] = latitude_info;
	return records_clean

def attributeRetriver(data_frame,date):
	length_of_record = data_frame.shape[0]
	record_container = []

	for i in np.arange(length_of_record):
		
		lng = data_frame["LONGITUDE"][i]
		lat = data_frame["LATITUDE"][i]

		entry = recordGetter(lng, lat, date)
		record_container.append(entry)
		
	output_df_with_attr = pd.DataFrame(record_container)[attributes]
	return output_df_with_attr

def columnNameModifier(df, date):
	time_delta = (date - base)
	which_day = time_delta.days
	retrieved_attr = attributeRetriver(df, date)
	attributes = retrieved_attr.columns.values

	new_names = []
	for attribute in attributes:
		if attribute in [ "LONGITUDE", "LATITUDE"]:
			new_names.append(attribute)
		else:
			attribute_with_time_stamp = attribute + "day%s" %(which_day)
			new_names.append(attribute_with_time_stamp)

	a_new_dictionary = {}
	for k, j in zip(attributes, new_names):
		a_new_dictionary[k] = j

	name_updated = df.rename(columns = (a_new_dictionary))
	indexed = name_updated.set_index(["LONGITUDE", "LATITUDE"])
	return indexed

def fileMerger(df_with_locations, to_date):

	delta = (to_date - base).days
	i = 0; df_list = []
	while i < delta:
		date = datetime.timedelta(days = i) + base

		attributes_retrieved = attributeRetriver(df_with_locations, date)
		attributes_retrieved_new_name = columnNameModifier(attributes_retrieved, date)
		df_list.append(attributes_retrieved_new_name)
		i = i + 1
		print "finished %r /90" %(i)

	multiple_days = pd.concat(df_list, axis = 1)
	# final_result = pd.concat([df_with_locations, multiple_days], axis = 1)
	return multiple_days

def dataBatcher(df_frame, size_of_a_batch):
	number_of_batches = df_frame.shape[0]/size_of_a_batch
	batch_list = []
	for i in np.arange(number_of_batches):

		one_batch = df_frame.iloc[i*size_of_a_batch: (i+1)*size_of_a_batch,:]
		batch_list.append(one_batch)

	return batch_list


base = datetime.datetime(2015,9,1)
to_date = datetime.datetime(2015,11,1)

in_test = locations_and_names.iloc[0:100,:]
new_file = fileMerger(dataBatcher(in_test, 100)[0], to_date)
new_file.to_csv("locations_full_length_data.csv")
