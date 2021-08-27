import json
import os
import subprocess
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import xarray as xr
from pytz import timezone

from GoogleCloudStorage import GoogleCloudStorageBucket
from MesoWest_BB import get_mesowest_radius

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


bucket.download(filename, "/tmp/hrrr", overwrite=True)


command = "Rscript"
path2script = "HRRR.R"
proc = subprocess.run([command, path2script, dat2], cwd=os.path.dirname(__file__))
assert proc.returncode == 0, "HRRR.R failed to complete successfully."

# Result is a netcdf file with lat/lon gridded u & v variables from ground level HRRR.

if not os.path.exists("hrrr_wind.nc"):
    raise FileNotFoundError("extracting wind data from HRRR failed")  # Error Handling


uv = xr.open_dataset("hrrr_wind.nc")  # Using the xarray library to open the NetCDF file
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

os.remove("hrrr_wind.nc")

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

os.makedirs("export", exist_ok=True)
with open(f"export/ME{dat2}.json", "w") as outfile:
    json.dump(master_dict, outfile, indent=2)
