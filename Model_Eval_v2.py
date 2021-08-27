#####################
# LIBRARY IMPORTS #
#####################

# If any packages are missing, install the packages in the Requirements.txt file#

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import xarray as xr
from pytz import timezone

#####################
# FUNCTION IMPORTS #
#####################

sys.path.append(
    os.path.realpath(os.path.dirname("/share/air-tracker-edf/src/hrrr-uncertainty/"))
)

from google.cloud import firestore

from GoogleCloudStorage import GoogleCloudStorageBucket
from MesoWest_BB import get_mesowest_radius  # Importing the MesoWest API python code.

# db = firestore.Client()

# collection_ref = db.collection(os.getenv('FIRESTORE_COLLECTION'))

# collection_ref2 = db.collection(os.getenv('FIRESTORE_COLLECTION2'))

# The above three lines are commented out as we are not focusing on GCS integration of output just yet.

# append search path to raster tools in STILT directory
sys.path.append(
    os.path.realpath(
        os.path.join(
            os.path.dirname("/share/air-tracker-edf/src/hrrr-uncertainty/"),
            "../stilt/stilt",
        )
    )
)
from raster import Raster

#####################
# MESOWEST API PULL #
#####################

hrrr_vars = "wind_direction," + "wind_speed," + "air_temp"

utc = timezone("UTC")
# all datetimes in UTC
cDATE = utc.localize(datetime.now()) - timedelta(hours=4)
## NOTE: I'm setting a time delta of -4, to match with the most recent hour of HRRR data available in the GCS Bucket. Remove if real time data as per UTC becomes available
print(cDATE)

mDATE = cDATE.replace(minute=0, second=0, microsecond=0)


print(mDATE)

mwm = get_mesowest_radius(
    mDATE, "40.65,-112.0", "20", variables=hrrr_vars, extra="", set_num=0, verbose=True
)

if mwm == "ERROR":
    # retry?
    raise FileNotFoundError(
        "Error fetching MesoWest data. Exiting the program..."
    )  # Error catching

###################
# HRRR DATA  PULL #
###################


bucket = GoogleCloudStorageBucket(
    "air-tracker-edf-stilt-meteorology-prod"
)  # This has the HRRR SLC Subset Data


dat = mDATE.strftime("%Y%m%d")
dat2 = mDATE.strftime("%Y%m%d%H")

hrrr_time = mDATE.strftime("%Y%m%d")
# hrrr_hour = int(mDATE.hour / 6) * 6
hrrr_hour = int(mDATE.hour)
hrrr_endhour = hrrr_hour + 5
# The above variables are manipulated to match the HRRR Reanalysis filename format

# filename = f'-112.6_-111.4_40.0_41.3/{hrrr_time}_{hrrr_hour:02}-{hrrr_endhour:02}_hrrr'
filename = f"-112.6_-111.4_40.0_41.3/{dat}/hysplit.t{hrrr_hour:02}z.hrrrf"


bucket.download(
    filename, "/tmp/hrrr"
)  # This is used to pull the ARL HRRR data from the GCS bucket


command = "Rscript"

path2script = "/share/air-tracker-edf/src/hrrr-uncertainty/hrrr.r"

subprocess.call([command, path2script, dat2])


# Result is a netcdf file with lat/lon gridded u & v variables from ground level HRRR. z
output_filename = "hrrr_wind.nc"

if not os.path.exists("/share/air-tracker-edf/src/hrrr-uncertainty/hrrr_wind.nc"):
    raise FileNotFoundError("extracting wind data from HRRR failed")  # Error Handling


uv = xr.open_dataset(
    "/share/air-tracker-edf/src/hrrr-uncertainty/hrrr_wind.nc"
)  # Using the xarray library to open the NetCDF file
# Removing the ARL files downloaded in the previous step, to preserve storage space
os.remove("/tmp/hrrr")


# Calculate wind speed and wind direction
ws = xr.ufuncs.sqrt(uv.variable.sel(z=1) ** 2 + uv.variable.sel(z=2) ** 2)
wd = np.mod(
    180 + np.rad2deg(np.arctan2(uv.variable.sel(z=2), uv.variable.sel(z=1))), 360
)

ws_hrrr = []
wd_hrrr = []
# grabs the lat and lon data of the mesowest sites in latest data grab
lat = mwm["LAT"]
lon = mwm["LON"]

# Creates arrays of ws/wd that match the locations of the lat/lons from the mesowest data
for i in range(0, len(lon)):
    ws_hrrr.append(
        ws.sel(latitude=lat[i], longitude=lon[i], method="nearest").values.item()
    )
    wd_hrrr.append(
        wd.sel(latitude=lat[i], longitude=lon[i], method="nearest").values.item()
    )

os.remove("/share/air-tracker-edf/src/hrrr-uncertainty/hrrr_wind.nc")

###############################
# COMBINING MESOWEST AND HRRR #
###############################

# Defining an empty data frame to store the final MW and HRRR raw values for Windspeed and Wind Direction

master_df = pd.DataFrame()

master_df["LAT"] = mwm["LAT"]
master_df["LON"] = mwm["LON"]


master_df["MW_ws"] = mwm["wind_speed"]
master_df["MW_wd"] = mwm["wind_direction"]


master_df["HRRR_ws"] = ws_hrrr
master_df["HRRR_wd"] = wd_hrrr


# calculates ws/wd error (Mesowest measured ws/wd minus HRRR modeled ws/wd) and adds to data dictionary
master_df["wsdiff"] = abs(master_df["MW_ws"] - master_df["HRRR_ws"])
master_df["wddiff"] = abs(master_df["MW_wd"] - master_df["HRRR_wd"])

# Function to fine tune wind speed difference
def wdcorr(x):
    return (360 - abs(x)) if (abs(x) > 180) else abs(x)


wdcorr_func = np.vectorize(wdcorr)
master_df["wddiff"] = wdcorr_func(master_df["wddiff"])


master_dict = master_df.to_dict("records")


with open(
    f"/share/air-tracker-edf/src/hrrr-uncertainty/ModelEvalOutput/ME{dat2}.json", "w"
) as outfile:
    json.dump(master_dict, outfile, indent=2)


################################
# UPLOADING THE RESULTS TO GCS #
################################

# The following lines were written to upload the final dictionary to a cloudstore location. But since the location has not been finalized, it has been commented out. Modify as required.

# final = []

# tstamp = mDATE.strftime("%Y%m%d%H%M")

# for x, y in master_df.iterrows():
#  lon=y[0]
#  lat=y[1]
#  Meso_Wind_Speed=y[2]
#  Meso_Wind_Dir=y[3]
#  HRRR_Wind_Speed=y[4]
#  HRRR_Wind_Dir=y[5]
#  Wind_Speed_Err=y[6]
#  Wind_Dir_Err=y[7]
#  record = {
#        'Mesowest_Wind_Speed': Meso_Wind_Speed,
#        'Mesowest_Wind_Dir': Meso_Wind_Dir,
#        'HRRR_Wind_Speed': HRRR_Wind_Speed,
#        'HRRR_Wind_Dir': HRRR_Wind_Dir,
#        'wind_direction_err': float(Wind_Dir_Err),
#        'wind_speed_err': Wind_Speed_Err
#    }
#  doc_ref = collection_ref.document(f"{tstamp}_{round(lon,2)}_{round(lat,2)}_1")


#  doc_ref.set(record) #uncomment this line to actually write new records to firestore
#  final.append(record)
