import json
import os
import subprocess
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import xarray as xr
from pytz import timezone

from GoogleCloudStorage import GoogleCloudStorageBucket
from MesoWest_BB import get_mesowest_radius  # Importing the MesoWest API python code.

# from mesowest import get_mesowest_ts, load_json
# from extras import dist, find_nearest
# from hrrrmods import profcall, pluk_arlhrrr_metstats
# Variables sent to the mesowest call. Can add others if needed
hrrr_vars = "wind_direction," + "wind_speed," + "air_temp"
utc = timezone("UTC")
# Need to determine if the following are necessary
# sitelist = pd.read_csv("sitelist.csv")
mountain = timezone("US/Mountain")
# This is used to pull the most current mesowest data.
cDATE = utc.localize(datetime.now())
print(cDATE)

# The mDATE represents the HRRR data pull (so it should be the top of the hour AFTER mesowest data is pulled)
mDATE = cDATE.astimezone(mountain).replace(minute=0, second=0, microsecond=0)
print(mDATE)

# The mesowest data grabbed using this code needs to be stored in a google bucket so that it can be access from both front and back ends
# TODO: add this data to GCS bucket, [NAME,LAT,LON,wind_speed_DATETIME,wind_speed,wind_direction]
mwm = get_mesowest_radius(
    cDATE, "40.65,-112.0", "20", "30", hrrr_vars, extra="", set_num=0, verbose=True
)

if mwm == "ERROR":
    # retry?
    raise FileNotFoundError(
        "Error fetching MesoWest data. Exiting the program..."
    )  # Error catching

# Run R code to grab the ground level "u" and "v" wind data from the arl formatted HRRR file
# This only happens once an hour
dat = mDATE.strftime("%Y%m%d")

bucket = GoogleCloudStorageBucket("high-resolution-rapid-refresh")
# -112.6_-111.4_40.0_41.3/20210120_06-11_hrrr

hrrr_time = mDATE.strftime("%Y%m%d")
# hrrr_hour = int(mDATE.hour / 6) * 6
hrrr_hour = int(mDATE.hour)
hrrr_endhour = hrrr_hour + 5


filename = f"noaa_arl_formatted/forecast/{dat}/hysplit.t{hrrr_hour:02}z.hrrrf"
# filename = f'{hrrr_time}_{hrrr_hour:02}-{hrrr_endhour:02}_hrrr'
bucket.download(
    filename, "/tmp/hrrr"
)  # This is used to pull the ARL HRRR data from the GCS bucket
# bucket.download(filename, '/share/air-tracker-edf/src/hrrr-uncertainty/') #This is used to pull the ARL HRRR data from the GCS bucket

# Runs Ben's R package for reading HRRR ARL formatted files using code "hrrr.r" which Tammy needs to share
# TODO: Paths will need to be updated to match where the Rscript lives on the cloud
# Command is Rscript executable - hopefull Ben can install R and dependencies and let us know where the executable lives
# command = '?/bin/Rscript.exe'
command = "Rscript"
# path2script = '?/R/LaughTest/hrrr.r'
path2script = "/share/air-tracker-edf/src/hrrr-uncertainty/hrrr.r"
# path2script = 'C:/Users/Shrivatsan Ragavan/OneDrive/Desktop/Laugh Test/hrrr.r'
subprocess.call([command, path2script, dat])
os.remove(
    "/tmp/hrrr"
)  # Removing the ARL files downloaded in the previous step, presumably to preserve storage space
# Result is a netcdf file with lat/lon gridded u & v variables from ground level HRRR. location and name of file set in hrrr.r will need to be updated
output_filename = "hrrr_wind.nc"
if not os.path.exists("/share/air-tracker-edf/src/hrrr-uncertainty/hrrr_wind.nc"):
    raise FileNotFoundError("extracting wind data from HRRR failed")

sys.path.append(
    os.path.realpath(os.path.dirname("/share/air-tracker-edf/src/hrrr-uncertainty/"))
)


# Sets filename and reads the netcdf file created by the R script (contains lat/lon gridded u and v ground level winds from HRRR)
# netcdf file name can be changed - it's set in hrrr.r
# ncfile = mDATE.strftime('%Y%m%d')+'_'+mDATE.strftime('%H')+'uv.nc'
uv = xr.open_dataset(
    "/share/air-tracker-edf/src/hrrr-uncertainty/hrrr_wind.nc"
)  # Using the xarray library to open the NetCDF file

# grabs the lat and lon data of the mesowest sites in latest data grab
lat = mwm["LAT"]
lon = mwm["LON"]

# Calculate wind speed and wind direction
ws = xr.ufuncs.sqrt(uv.variable.sel(z=1) ** 2 + uv.variable.sel(z=2) ** 2)
wd = np.mod(
    180 + np.rad2deg(np.arctan2(uv.variable.sel(z=2), uv.variable.sel(z=1))), 360
)

ws_hrrr = []
wd_hrrr = []

# Creates arrays of ws/wd that match the locations of the lat/lons from the mesowest data
for i in range(0, len(lon)):
    ws_hrrr.append(
        ws.sel(latitude=lat[i], longitude=lon[i], method="nearest").values.item()
    )
    wd_hrrr.append(
        wd.sel(latitude=lat[i], longitude=lon[i], method="nearest").values.item()
    )


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


# Need to qa/qc this
def wdcorr(x):
    return (360 - abs(x)) if (abs(x) > 180) else abs(x)


# end
wdcorr_func = np.vectorize(wdcorr)
master_df["wddiff"] = wdcorr_func(master_df["wddiff"])


tstamp = mDATE.strftime("%Y%m%d%H%M")

master_dict = master_df.to_dict("records")

final_json = json.dumps(master_dict, indent=2)
