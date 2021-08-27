library(stiltread)
library(ncdf4)
library(raster)
library(sp)

dat <- commandArgs(trailingOnly = TRUE)
da <- as.POSIXct(dat,format="%Y%m%d%H")

# #Probably a better way to do this
# hhr = format(da, "%H")
# if (hhr > 17) {hrs <- "_18-23_hrrr"} else 
# {if (hhr < 7) {hrs <- "_0-6_hrrr"} else 
# {if (hrr < 13) {hrs <- "_7-12_hrrr"} else
# {hrs <- "_13-17_hrrr"}}}

filename <- '/tmp/hrrr'

# filname <- paste("PATH_TO_HRRR_ARL_FILE/hrrr_",format(da, "%Y%m%d"),hrs,sep="")
uv <- read_met_wind(filename,yy=format(da, "%y"),mm=format(da, "%m"),dd=format(da, "%d"),hh=format(da, "%H"),lvl=0)
#writeRaster(uv,filename='/tmp/hrrr/hrrr_wind.nc',overwrite=TRUE)
writeRaster(uv,filename='/share/air-tracker-edf/src/hrrr-uncertainty/hrrr_wind.nc',overwrite=TRUE)
