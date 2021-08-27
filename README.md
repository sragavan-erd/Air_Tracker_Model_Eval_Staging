Air_Tracker_Model_Eval_Staging


Path: /share/air-tracker-edf/src/hrrr-uncertainty/Model_Eval_v2.py

Model_Eval_v2.py is a Python code that aggregates data to form a mode of comparison between predictions of wind speeds and direction, against actual measured values. This code makes use of the MesoWest API to source the real time actual measurements of wind speed and wind direction. The predictions are sourced from High Resolution Rapid Refresh (HRRR) model, the data for which is made available in Google Cloud Storage (GCS) Buckets.
Dependencies

The Model_Eval_v2.py depends on the following codes for its successful execution
•	MesoWest_BB.py 
o	This file has the functions defined for connecting to the MesoWest API and pulling the data from the measurement stations in the location. This code currently has functions to get time series data for a particular sensor and to get data from all sensors within a radius of a particular co-ordinate
	NOTE: PLEASE UPDATE THE MesoWest API TOKEN IN THIS FILE BEFORE EXECUTING! 
•	GoogleCloudStorage.py
o	Connection to the GCS buckets is enabled through this file
•	HRRR.R
o	This R file performs the task of converting the HRRR files from the ARL format to the NetCDF4 format for easier handling. 
 
Libraries
The following libraries are required for the execution of the code. 
•	pandas
•	numpy
•	json
•	netCDF4
•	os
•	re
•	datetime
•	subprocess
•	warnings
•	xarray
•	pytz
•	math
•	matplotlib
•	sys
•	base64
•	copy
•	io
•	tempfile
•	textwrap
In the event of one or more of the libraries being not installed, please run the installation using the Requirements.txt file to ensure all the relevant packages are installed in the environment.
Data Flow


MesoWest API Data Pull
The MesoWest API takes datetime input in UTC time zone. The variable cDATE is localized to the UTC time zone in the following snippet
 
Mesowest_BB.py has a function defined that allows an input of a central co-ordinate and radius, which then returns the data for all measurement sites within that zone. 
Currently, the central co-ordinates for Salt Lake City, Utah (46.65,-112.0) is hardcoded in the function call. In addition to these coordinates, we pass a radius of 20 miles, the datetime, variables that need to be pulled (wind speed and wind direction).
 
HRRR Data Pull
The HRRR data uploaded to the GCS Bucket is also in UTC. Therefore, we create an mDATE variable which is just a copy of cDATE.
 
In order to download the ARL Files from the GCS bucket, we establish a connection. The variable dat, dat2, hrrr_time, hrrr_hour, hrrr_endhour are created to be used in creating a dynamic file name based on the datetime. This filename reflects the naming convention of the ARL files stores in the GCS Buckets.
 
A temporary location called ‘/tmp/hrrr’ is created, into which the ARL file is downloaded. Post this, the HRRR.R file is called as subprocess. This function call takes ‘RScript’, the path of the R code, and the datetime as inputs.
The Rscript outputs a file called hrrr_wind.nc which is in the netCDF4 format. This file is stored in the same location as the main python code. A variable uv is assigned with this netCDF4 file for further data manipulation. The temporary location ‘/tmp/hrrr/’ is deleted to save on storage space.
 
Using the u & v variables in the netCDF4 file, we calculate the wind speed and wind direction as predicted by HRRR.
 
This wind speed and wind direction is available for every point with a resolution of 3km. The co-ordinates of interest in this case are the location of the sensors where MesoWest data is available. Therefore, we extract the co-ordinates from the extracted MesoWest file, which we then use to get the wind speed and wind directions of the nearest points in HRRR. These metrics, once matched with the coordinates of interest, is stored as lists in ws_hrrr and wd_hrrrr. 
As a final step, we delete the netCDF4 file.
 
Combining the HRRR and MesoWest Data
An empty dataframe called master_df created. To this data frame, we combine the lists of latitude, longitude, MesoWest wind speed, MesoWest wind direction, HRRR wind speed, HRRR wind direction.
 
From these metrics, the difference in wind speed and wind direction obtained from the two sources is calculated at every sensor coordinate. The difference in direction is adjusted to reflect the difference under 180 degrees using a user defined function called wdcorr.
 
The dataframe is then converted into dictionary, which is written as a json file. The json file is stored in a folder called ModelEvalOutput. The output json file is named with the timestamp for easy identification. 
 
Next Steps
1.	Location of the output file: 
•	Since the location of where the final output files will reside has not been finalized, this code temporarily stores the data in a folder in the Virtual Machine. However, based on the previous versions of the Laugh Test, I have retained the code to upload the output to a Firestore location. This part of the code is currently commented out and can be incorporated with a few changes.
2.	Sourcing MesoWest data using Bounding Boxes
•	The current MesoWest_BB.py file, has functions only for sourcing time series data from a particular sensor and the coordinate + radius method. I was unable to locate the python function that uses the bounding boxes to source the Meso West Data, although the API documentation does mention the feature to do so. 
3.	Sourcing HRRR ARL file from the GCS Bucket that has data for the whole US
•	Another file by the name Model_Eval_Pred_V3.py has been created to source the HRRR arl file from the larger bucket, so as to include data for Houston as well. While the code works, owing to the large size of the ARL file (~10GB), the execution time runs up to 15 minutes. 


