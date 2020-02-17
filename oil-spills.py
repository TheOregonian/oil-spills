#!/usr/bin/env python
# coding: utf-8

import pandas as pd;
import xlrd;
import math;
import numpy as np;
import re;
import geopandas;
pd.set_option('display.max_colwidth', -1)

print "Fetching Excel file."
url = "http://nrc.uscg.mil/FOIAFiles/Current.xlsx"
try:
	xl = pd.ExcelFile(url)
except:
	print "Failed to fetch Excel file."

print "Parsing Excel file."
# Parse out sheets into separate DataFrames
calls = xl.parse("CALLS", na_values='');
incidents = pd.read_excel(url,"INCIDENTS",dtype={'PIER_DOCK_NUMBER':str})
incident_commons = xl.parse("INCIDENT_COMMONS");
incident_details = xl.parse("INCIDENT_DETAILS");
materials = xl.parse("MATERIAL_INVOLVED");
material_cr = xl.parse("MATERIAL_INV0LVED_CR");
trains = xl.parse("TRAINS_DETAIL");
traincars = xl.parse("DERAILED_UNITS");
vessels = xl.parse("VESSELS_DETAIL");
vehicles = xl.parse("MOBILE_DETAILS");

# Create a pretty date string for use in naming files later
import datetime
now = datetime.datetime.now()
now = now.strftime("%Y-%m-%d")

# Assemble all one-to-one tables into one big table
incident_commons = pd.merge(incident_commons,calls, on='SEQNOS')
incident_commons = pd.merge(incident_commons,incidents, on='SEQNOS')
incident_commons = pd.merge(incident_commons,incident_details, on='SEQNOS')

# Pull up our file showing the ID of the last record processed in the "Current.xls" file
# The file just keeps getting bigger every Sunday until calendar year end.
# --------------------------------------------------------------------------
# NOTE!!!!! If you are running this the first time, you want to delete the file
# called "bookmark" from your folder
# --------------------------------------------------------------------------

import os
exists = os.path.isfile('bookmark')
if exists:
    bookmark = pd.read_csv("bookmark")
    appending = True
else:
    appending = False

if appending:
	# Drop all the old records that we've already processed
	incident_commons = incident_commons[incident_commons.SEQNOS > int(bookmark.top[0])]
	materials = materials[materials.SEQNOS > int(bookmark.top[0])]
	material_cr = material_cr[material_cr.SEQNOS > int(bookmark.top[0])]
	trains = trains[trains.SEQNOS > int(bookmark.top[0])]
	traincars = traincars[traincars.SEQNOS > int(bookmark.top[0])]
	vessels = vessels[vessels.SEQNOS > int(bookmark.top[0])]
	vehicles = vehicles[vehicles.SEQNOS > int(bookmark.top[0])]

print "Writing out files."

# Export many-to-one tables as separate files for import
materials.to_csv('materials' + now + '.csv')
material_cr.to_csv('material_cr' + now + '.csv')
trains.to_csv('trains' + now + '.csv')
traincars.to_csv('traincars' + now + '.csv')
vessels.to_csv('vessels' + now + '.csv')
vehicles.to_csv('vehicles' + now + '.csv')

print "Processing coordinates."

# Use full coordinates if available
incident_commons['new_latitude']  = incident_commons.LAT_DEG + (incident_commons.LAT_MIN / 60) + (incident_commons.LAT_SEC / 3600)
incident_commons['new_longitude'] = incident_commons.LONG_DEG + (incident_commons.LONG_MIN / 60) + (incident_commons.LONG_SEC / 3600)
incident_commons['new_latquad']   = incident_commons.LAT_QUAD
incident_commons['new_longquad']  = incident_commons.LONG_QUAD

# Basic coordinate cleaning function, from string to float
def splitclean(latitude):
    if isinstance(latitude,float):
        latitude = str(latitude)
    # Clear out extraneous characters
    latitude = re.sub(r'[A-Za-z]|\/|\'|\"|\&|\:|[\x00-\x1F\x80-\xFF]','',latitude)
    latitude = re.sub(r'\-',' ',latitude)
    latitude = re.sub(r' {2,}',' ',latitude)
    latitude = re.sub(r'\\p{C}','',latitude)
    latitude = re.sub(r'([0-9]{1,2}) (\..*)',r'\1' + r'\2',latitude)
    latitude = latitude.strip()
    # Break the string into components likely to be degrees, minutes and seconds
    components= latitude.split(' ')
    degrees = float; minutes = float; seconds = float;
    for i in range(len(components)):
        # No double decimal; ignore any digits after the second one
        components[i] = re.sub(r'\..*(\..*)',r'\0',components[i])
        # No leading and trailing zeroes
        components[i] = components[i].strip('0')
        # Only numeric characters and decimal points
        components[i] = re.sub(r'[^0-9.]+','',components[i])
        # No leading and trailing spaces
        components[i] = components[i].strip()
        # No leading and trailing decimal points
        components[i] = components[i].strip('.')
        if i < 2: # Weird case of single number with multiple decimal points
            components[i] = re.sub(r'\..*(\..*)',r'\0',components[i])
    if not components[0]:
        return # Skip if empty
    # Calculate degrees and minutes and compile into a single decimal unit
    else:
        degrees = float(components[0])
    if len(components) > 1:
        if isinstance(components[1],float):
        	minutes = float(components[1]) / 60
        	if len(components) == 3:
        		seconds = float(components[2]) / 3600
        		minutes = minutes + seconds
        	if degrees > 0:
        		degrees = degrees + minutes
        	else:
        		degrees = degrees - minutes
    return degrees


####################################
# Latitude from INCIDENT_LOCATION
####################################

# Start with boolean expression to operate on
null_latitude = (incident_commons.new_latitude.isnull()) & (incident_commons.INCIDENT_LOCATION.str.extract(r'^LAT(\/| ).*:(.*),')[1].notnull())

# Find existing substring to parse for degrees, hours, minutes
latitude = incident_commons.INCIDENT_LOCATION.str.extract(r'^LAT(\/| ).*:(.*)(N|S),')[1]
latquad  = incident_commons.INCIDENT_LOCATION.str.extract(r'^LAT(\/| ).*:(.*)(N|S),')[2]

# Clean the latitude
latitude = latitude.apply(splitclean)

incident_commons.loc[null_latitude,'new_latitude'] = latitude
incident_commons.loc[null_latitude,'new_latquad']  = latquad

####################################
# Longitude from INCIDENT_LOCATION
####################################

# Find existing substring to parse for degrees, hours, minutes
longitude = incident_commons.INCIDENT_LOCATION.str.extract(r'^LAT(\/| ).*:(.*)(N|S), (.*?)(W|E)')[3]
longquad  = incident_commons.INCIDENT_LOCATION.str.extract(r'^LAT(\/| ).*:(.*)(N|S), (.*?)(W|E)')[4]
    
# Clean the longitude
longitude = longitude.apply(splitclean)

incident_commons.loc[null_latitude,'new_longitude'] = longitude
incident_commons.loc[null_latitude,'new_longquad']  = longquad

####################################
# Latitude and longitude from INCIDENT_LOCATION using pattern of comma split
####################################

# Start with boolean expression to operate on
null_latitude = (incident_commons.new_latitude.isnull()) & (incident_commons.INCIDENT_LOCATION.str.extract(r'^([0-9].*),(.*)')[0].notnull())

# Find existing substring to parse for degrees, hours, minutes
latitude  = incident_commons.INCIDENT_LOCATION.str.extract(r'^([0-9].*),(.*)')[0]
longitude = incident_commons.INCIDENT_LOCATION.str.extract(r'^([0-9].*),(.*)')[1]
latquad   = latitude.str.extract(r'(N|S)')[0]
longquad  = longitude.str.extract(r'(W|E)')[0]

latitude  = latitude.apply(splitclean)
longitude = longitude.apply(splitclean)

incident_commons.loc[null_latitude,'new_latitude'] = latitude
incident_commons.loc[null_latitude,'new_latquad']  = latquad
incident_commons.loc[null_latitude,'new_longitude'] = longitude
incident_commons.loc[null_latitude,'new_longquad']  = longquad

####################################
# LATITUDE FROM LOCATION_ADDRESS
####################################

# Start with boolean expression to operate on
null_latitude = (incident_commons.new_latitude.isnull()) & (incident_commons.INCIDENT_LOCATION.str.extract(r'^LAT(\:| |\.)(.*)')[1].notnull())

# Find existing substring to parse for degrees, hours, minutes
latitude = incident_commons.LOCATION_ADDRESS.str.extract(r'^LAT(\:| |\.)(.*)')[1]
latquad  = latitude.str.extract(r'(N|S)')[0]

latitude  = latitude.apply(splitclean)

incident_commons.loc[null_latitude,'new_latitude'] = latitude
incident_commons.loc[null_latitude,'new_latquad']  = latquad

####################################
# LONGITUDE FROM LOCATION_ADDRESS
####################################

# Find existing substring to parse for degrees, hours, minutes
longitude = incident_commons.LOCATION_STREET1.str.extract(r'^LONG(\:| |\.)(.*)')[1]
longquad  = longitude.str.extract(r'(E|W)')

longitude  = latitude.apply(splitclean)

incident_commons.loc[null_latitude,'new_longitude'] = longitude
incident_commons.loc[null_latitude,'new_longquad']  = longquad

####################################
# Latitude and longitude from LOCATION_ADDRESS using pattern of comma split
####################################

# Start with boolean expression to operate on
null_latitude = (incident_commons.new_latitude.isnull()) & (incident_commons.LOCATION_ADDRESS.str.extract(r'^([0-9].*),(.*)')[0].notnull())

# Find existing substring to parse for degrees, hours, minutes
latitude  = incident_commons.LOCATION_ADDRESS.str.extract(r'^([0-9].*),(.*)')[0]
longitude = incident_commons.LOCATION_ADDRESS.str.extract(r'^([0-9].*),(.*)')[1]
latquad   = latitude.str.extract(r'(N|S)')[0]
longquad  = longitude.str.extract(r'(W|E)')[0]

latitude  = latitude.apply(splitclean)
longitude = longitude.apply(splitclean)

incident_commons.loc[null_latitude,'new_latitude'] = latitude
incident_commons.loc[null_latitude,'new_latquad']  = latquad
incident_commons.loc[null_latitude,'new_longitude'] = longitude
incident_commons.loc[null_latitude,'new_longquad']  = longquad


####################################
# LATITUDE FROM LOCATION_STREET1
####################################

# Start with boolean expression to operate on
null_latitude = (incident_commons.new_latitude.isnull()) & (incident_commons.LOCATION_STREET1.str.extract(r'^LAT(\:| |\.)(.*)')[1].notnull())

# Find existing substring to parse for degrees, hours, minutes
latitude = incident_commons.LOCATION_STREET1.str.extract(r'^LAT(\:| |\.)(.*)')[1]
latquad  = latitude.str.extract(r'(N|S)')[0]

latitude  = latitude.apply(splitclean)

incident_commons.loc[null_latitude,'new_latitude'] = latitude
incident_commons.loc[null_latitude,'new_latquad']  = latquad

####################################
# LONGITUDE FROM LOCATION_STREET1
####################################

# Find existing substring to parse for degrees, hours, minutes
longitude = incident_commons.LOCATION_STREET2.str.extract(r'^LONG(\:| |\.)(.*)')[1]
longquad  = longitude.str.extract(r'(E|W)')

longitude  = latitude.apply(splitclean)

incident_commons.loc[null_latitude,'new_longitude'] = longitude
incident_commons.loc[null_latitude,'new_longquad']  = longquad

####################################
# Latitude and longitude from LOCATION_STREET1 using pattern of comma split
####################################

# Start with boolean expression to operate on
null_latitude = (incident_commons.new_latitude.isnull()) & (incident_commons.LOCATION_STREET1.str.extract(r'^([0-9].*),(.*)')[0].notnull())

# Find existing substring to parse for degrees, hours, minutes
latitude  = incident_commons.LOCATION_STREET1.str.extract(r'^([0-9].*),(.*)')[0]
longitude = incident_commons.LOCATION_STREET1.str.extract(r'^([0-9].*),(.*)')[1]
latquad   = latitude.str.extract(r'(N|S)')[0]
longquad  = longitude.str.extract(r'(W|E)')[0]

latitude  = latitude.apply(splitclean)
longitude = longitude.apply(splitclean)

incident_commons.loc[null_latitude,'new_latitude'] = latitude
incident_commons.loc[null_latitude,'new_latquad']  = latquad
incident_commons.loc[null_latitude,'new_longitude'] = longitude
incident_commons.loc[null_latitude,'new_longquad']  = longquad

# Function for testing if column contains a street
def findstreet(location):
    if isinstance(location,float):
        return False
    elif location == '':
        return False
    else:
        location = location.encode('utf-8')
        location = str(location)
        location = re.sub(r'\\p{C}|[\x00-\x1F\x80-\xFF]','',location)
        if re.search(r' (RD|ROAD|AVE|AVENUE|LN|LANE|PL|PLACE|HWY|HIGHWAY|BLVD|BOULEVARD|CT|COURT|CIR|CIRCLE|ROUTE|RTE|WY|WAY)( |\.|$)',location):
            return True
    return False

# Boolean for selecting records with street
has_street = incident_commons.LOCATION_ADDRESS.apply(findstreet) == True
incident_commons['new_street'] = ''
incident_commons['new_street'] = np.nan
# Apply address if exists in loc address
incident_commons.loc[has_street,'new_street'] = incident_commons.LOCATION_ADDRESS
has_street = (incident_commons.LOCATION_STREET1.apply(findstreet) == True) & (incident_commons.new_street.isnull())
# Apply address if exists in street1
incident_commons.loc[has_street,'new_street'] = incident_commons.LOCATION_STREET1
# Apply address if exists in incident location
has_street = (incident_commons.INCIDENT_LOCATION.apply(findstreet) == True) & (incident_commons.new_street.isnull())
incident_commons.loc[has_street,'new_street'] = incident_commons.INCIDENT_LOCATION

print "Starting geocoder."
# Create geodataframe for geocoding
geo = geopandas.GeoDataFrame(incident_commons)
counter = 0
from geopy.geocoders import Bing

def get_apikey(servicename):
	import pandas as pd
	import os
	if os.path.isfile('/etc/apikeys'):
		path = '/etc/apikeys'
	else:
		if os.path.isfile('apikeys'):
			path = 'apikeys'
		else:
			return False
	keyfile = pd.read_csv(path)
	keydict = keyfile.to_dict('records')
	for row in keydict:
		if row['service'] == servicename:
			apikey = row['key']
			return apikey

bingkey = get_apikey('bing')
if bingkey == False:
	print "Could not find necessary API key file."
	quit()
	
#bingkey = "AuNPKK6wEhtJOp2JSz1iQQwqgCptimUiyamkP18Bnz4ycjMaxcFdd1kYEqyWrdxL"

# Compile an address string for submission to geocoder, then submit request

for row in geo.itertuples():
    counter+=1
    print counter
    # Skip this record for geocoding if latitude is already populated
    if not math.isnan(row.new_latitude):
    	print "Latitude already there."
        continue
    # Compile the address using available street/city/county/state fields
    print "Compiling address"
    address = ''
    if not isinstance(row.LOCATION_STATE,float):
        address = str(row.LOCATION_STATE)
    if not isinstance(row.LOCATION_NEAREST_CITY,float):
        address = str(row.LOCATION_NEAREST_CITY) + ',' + address
    else:
        if not isinstance(row.LOCATION_COUNTY,float):
            address = str(row.LOCATION_COUNTY) + ' COUNTY,' + address
    if not isinstance(row.new_street,float):
        address = str(row.new_street) + ',' + address
    if address == '':
        if not isinstance(row.INCIDENT_LOCATION,float):
            address = str(row.INCIDENT_LOCATION,float)
        else:
            continue
    print str(counter) + ' ' + address
    # Geocode the address
    try:
        location = geopandas.tools.geocode(address,provider="Bing",api_key=bingkey)
        location['SEQNOS'] = row.SEQNOS
        if counter == 1:
            locations = geopandas.GeoDataFrame(location)
        else:
            locations = locations.append(location)
    	print "Geocoded one address."
    	print location
    except:
    	print "Geocoder failed."
        continue

    if counter > 10:
    	break

print 'Preparing to merge'

geo = geo.merge(locations,how='left',on='SEQNOS')

# Create a text file noting record ID of where we left off with the last import of data.
biggest = geo.SEQNOS.max()
bookmark = pd.DataFrame({'top':[biggest]})
bookmark.to_csv('bookmark')

# Clean up latitudes and longitudes to proper sign based on hemisphere
geo.new_latitude[(geo.new_latquad == 'N')|geo.new_latquad.isnull()] = abs(geo.new_latitude)
geo.new_latitude[geo.new_latquad == 'S'] = -1 * abs(geo.new_latitude)
geo.new_longitude[geo.new_longquad == 'E'] = abs(geo.new_longitude)
geo.new_longitude[(geo.new_longquad == 'W')|geo.new_longquad.isnull()] = -1 * abs(geo.new_longitude)

# Create a geodataframe by converting coordinate data to Point objects
from shapely.geometry import Point
geometry = [Point(xy) for xy in zip(geo.new_longitude, geo.new_latitude)]
points   = geopandas.GeoDataFrame(geometry, geometry=geometry)

# Boolean to specify records to update using the coordinate data
null_coordinates = geo.geometry.isnull()
#null_coordinates = geo.new_latitude.notnull()

# Update the geometry with data from the latitude and longitude coordinates
geo.loc[null_coordinates,'geometry'] = points

# DEPRECATED -- dump to geojson
# Output everything as a text file for use elswhere
#jsonfile = 'spillcalls' + now + '.geojson'
#geo.to_file(jsonfile, driver='GeoJSON')

# Redo geodataframe as a regular dataframe
export = pd.DataFrame(geo)
# Export bulk file or update file
if appending:
	export.to_csv('latest_spillcalls.csv',encoding='utf-8')
else:
	export.to_csv('spillcalls-all.csv',encoding='utf-8')

# Finished
