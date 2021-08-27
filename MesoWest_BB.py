import os
from datetime import datetime
from math import asin, cos, radians, sin, sqrt

import numpy as np
import requests

default_vars = (
    "altimeter,"
    + "pressure,"
    + "sea_level_pressure,"
    + "wind_direction,"
    + "wind_speed,"
    + "wind_gust,"
    + "air_temp,"
    + "relative_humidity,"
    + "dew_point_temperature"
)

hrrr_vars = "wind_direction," + "wind_speed," + "air_temp"

carslaw_vars = (
    "relative_humidity," + "air_temp," + "pressure," + "wind_direction," + "wind_speed"
)

"""TAMMY'S API TOKEN!!! If using a lot, please request your own.
Get your own token here: https://developers.synopticdata.com/"""
MESOWEST_TOKEN = os.getenv("MESOWEST_TOKEN")
if not MESOWEST_TOKEN:
    raise EnvironmentError("MESOWEST_TOKEN environment variable not found.")


def dist(lat1, long1, lat2, long2):
    """Replicating the same formula as mentioned in Wiki"""
    # convert decimal degrees to radians
    lat1, long1, lat2, long2 = map(radians, [lat1, long1, lat2, long2])
    # haversine formula
    dlon = long2 - long1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    # Radius of earth in kilometers is 6371
    km = 6371 * c
    return km


def find_nearest(lat, long):
    distances = df.apply(lambda row: dist(lat, long, row["LAT"], row["LON"]), axis=1)
    return distances  # df.loc[distances.idxmin(), 'STID']


def load_json(URL, verbose=True):
    """Return json data as a dictionary from a URL"""
    if verbose:
        print("\nRetrieving from MesoWest API: %s\n" % URL)

    f = requests.get(URL)
    return f.json()


def get_mesowest_ts(
    stationID, sDATE, eDATE, variables=default_vars, tz="UTC", set_num=0, verbose=True
):
    """
    Get MesoWest Time Series
    Makes a time series query from the MesoWest API for a single station.
    Input:
        stationID  - String of the station ID (a single station)
        sDATE      - datetime object of the start time in UTC
        eDATE      - datetime object of the end time in UTC
        variables  - String of variables you want to request from the MesoWest
                     API, separated by commas. See a list of available variables
                     here: https://synopticlabs.org/api/mesonet/variables/
        tz         - Time stamp option, either 'UTC' or 'LOCAL'
        set_num    - Some stations have multiple sensors and are stored in
                     a different 'set'. By default, grab the first set. You
                     can change this integer if you know your staion has more
                     than one set for a particular variable. Best if used if
                     only requesting a single variable. Knowing what each set
                     is requires knowledge of the station and how the sensor
                     variables are stored in the MesoWest database.
        verbose    - True: Print some diagnostics
                     False: Don't print anything
    Output:
        The time series data for the requested station as a dictionary.
    """

    ## Some basic checks
    assert isinstance(stationID, str), "stationID must be a string"
    assert isinstance(sDATE, datetime) and isinstance(
        eDATE, datetime
    ), "sDATE and eDATE must be a datetime"
    assert tz.upper() in ["UTC", "LOCAL"], "tz must be either 'UTC' or 'LOCAL'"
    assert set_num >= 0 and isinstance(
        set_num, int
    ), "set_num must be a positive integer"

    ## Build the MesoWest API request URL
    URL = (
        "http://api.mesowest.net/v2/stations/timeseries?"
        + "&token="
        + MESOWEST_TOKEN
        + "&stid="
        + stationID
        + "&start="
        + sDATE.strftime("%Y%m%d%H%M")
        + "&end="
        + eDATE.strftime("%Y%m%d%H%M")
        + "&vars="
        + variables
        + "&obtimezone="
        + tz
        + "&output=json"
    )

    ## Open URL, and convert JSON to some python-readable format.
    data = load_json(URL, verbose=verbose)

    if data["SUMMARY"]["RESPONSE_CODE"] == 1:
        # There are no errors in the API Request

        ## Grab the content we are interested in
        return_this = {}

        # Station metadata
        stn = data["STATION"][0]
        return_this["URL"] = URL
        return_this["NAME"] = str(stn["NAME"])
        return_this["STID"] = str(stn["STID"])
        return_this["LAT"] = float(stn["LATITUDE"])
        return_this["LON"] = float(stn["LONGITUDE"])
        return_this["ELEVATION"] = float(stn["ELEVATION"])
        # Note: Elevation is in feet, NOT METERS!

        # Dynamically create keys in the dictionary for each requested variable
        for v in stn["SENSOR_VARIABLES"]:
            if verbose:
                print("v is: %s" % v)
            if v == "date_time":
                # Convert date strings to a datetime object
                dates = data["STATION"][0]["OBSERVATIONS"]["date_time"]
                if tz == "UTC":
                    DATES = [datetime.strptime(i, "%Y-%m-%dT%H:%M:%SZ") for i in dates]
                else:
                    DATES = [
                        datetime.strptime(i[:-5], "%Y-%m-%dT%H:%M:%S") for i in dates
                    ]
                    return_this["UTC-offset"] = [i[-5:] for i in dates]
                return_this["DATETIME"] = DATES
            else:
                # Each variable may have more than one "set", like if a station
                # has more than one sensor. Deafult, set_num=0, will grab the
                # first (either _set_1 or _set_1d).
                key_name = str(v)
                grab_this_set = np.sort(list(stn["SENSOR_VARIABLES"][key_name]))[
                    set_num
                ]
                if verbose:
                    print("    Used %s" % grab_this_set)
                variable_data = stn["OBSERVATIONS"][grab_this_set]
                return_this[key_name] = np.array(variable_data, dtype=np.float)

        return return_this

    else:
        # There were errors in the API request
        if verbose:
            print("  !! Errors: %s" % URL)
            print("  !! Reason: %s\n" % data["SUMMARY"]["RESPONSE_MESSAGE"])
        return "ERROR"


def get_mesowest_radius(
    DATE,
    location,
    radius=150,
    within=30,
    variables=hrrr_vars,
    extra="",
    set_num=0,
    verbose=True,
):
    """
    Get MesoWest stations within a radius
    Get data from all stations within a radius around a station ID or a
    latitude,longitude point from the MesoWest API.
    Input:
        DATE      - datetime object of the time of interest in UTC
        location  - String of a MW station ID or string of a comma-separated
                    lat,lon as the center (i.e. 'WBB' or '40.0,-111.5')
                    HACK: If [location=None] then the radius limit is
                          disregarded and returns all stations. Therefore, it
                          is best used in congunction with the [extra=&...]
                          argument.
        radius    - Distance from center location in *MILES*.
                    Default is 30 miles.
        within    - *MINUTES*, plus or minus, the DATE to get for.
                    Default returns all observations made 30 minutes before and
                    30 minutes after the requested DATE.
        variables - String of variables you want to request from the MesoWest
                    API, separated by commas. See a list of available variables
                    here: https://synopticlabs.org/api/mesonet/variables/
        extra     - Any extra conditions or filters. Refer to the synoptic API
                    documentation for more info. String should be proceeded by
                    a "&". For example, to get stations within a specific
                    set of networks, use [extra='&network=1,2']
        set_num   - Some stations have multiple sensors and are stored in
                    a different 'set'. By default, grab the first set. You
                    can change this integer if you know your staion has more
                    than one set for a particular variable. Best if used if
                    only requesting a single variable. Knowing what each set
                    is requires knowledge of the station and how the sensor
                    variables are stored in the MesoWest database.
        verbose   - True: Print some diagnostics
                    False: Don't print anything
    Output:
        A dictionary of data at each available station.
    """
    ## Some basic checks
    assert isinstance(location, (str, type(None))), "location must be a string or None"
    assert isinstance(DATE, datetime), "DATE must be a datetime"
    assert set_num >= 0 and isinstance(
        set_num, int
    ), "set_num must be a positive integer"

    ## Build the MesoWest API request URL
    if location is None:
        if extra == "":
            if verbose:
                print("  !! Errors:")
                print(
                    "  !! Reason: I don't think you really want to return everything."
                )
                print(
                    "             Refine your request using the 'extra=&...' argument"
                )
            return "ERROR"
        URL = (
            "http://api.mesowest.net/v2/stations/nearesttime?"
            + "&token="
            + MESOWEST_TOKEN
            + "&attime="
            + DATE.strftime("%Y%m%d%H%M")
            + "&within="
            + str(within)
            + "&obtimezone="
            + "UTC"
            + "&vars="
            + variables
            + extra
        )
    else:
        URL = (
            "http://api.mesowest.net/v2/stations/nearesttime?"
            + "&token="
            + MESOWEST_TOKEN
            + "&attime="
            + DATE.strftime("%Y%m%d%H%M")
            + "&within="
            + str(within)
            + "&radius="
            + "%s,%s" % (location, radius)
            + "&obtimezone=UTC"
            + "&vars="
            + variables
            + extra
        )

    ## Open URL, and convert JSON to some python-readable format.
    data = load_json(URL, verbose=verbose)

    if data["SUMMARY"]["RESPONSE_CODE"] == 1:
        # Initiate a dictionary of the data we want to keep
        return_this = {
            "URL": URL,
            "NAME": np.array([]),
            "STID": np.array([]),
            "LAT": np.array([]),
            "LON": np.array([]),
            "ELEVATION": np.array([]),  # Elevation is in feet.
            "DATETIME": DATE,
        }
        #
        # Create a new key for each possible variable
        for v in data["UNITS"]:
            return_this[str(v)] = np.array([])
            # Since some observation times for each variables at the same station
            # *could* be different, I will store the datetimes from each variable
            # with a similar name as the variable.
            return_this[str(v) + "_DATETIME"] = np.array([])
        #
        for stn in data["STATION"]:
            # Store basic metadata for each station in the dictionary.
            return_this["NAME"] = np.append(return_this["NAME"], str(stn["NAME"]))
            return_this["STID"] = np.append(return_this["STID"], str(stn["STID"]))
            return_this["LAT"] = np.append(return_this["LAT"], float(stn["LATITUDE"]))
            return_this["LON"] = np.append(return_this["LON"], float(stn["LONGITUDE"]))
            try:
                return_this["ELEVATION"] = np.append(
                    return_this["ELEVATION"], int(stn["ELEVATION"])
                )
            except:
                return_this["ELEVATION"] = np.append(return_this["ELEVATION"], np.nan)
            #
            # Dynamically store data for all possible variable. If a station does
            # not have a variable, then a np.nan will be it's value.
            for v in data["UNITS"]:
                if v in list(stn["SENSOR_VARIABLES"]) and len(stn["OBSERVATIONS"]) > 0:
                    # If value exists, then append with the data
                    grab_this_set = np.sort(list(stn["SENSOR_VARIABLES"][v]))[set_num]
                    variable_data = float(stn["OBSERVATIONS"][grab_this_set]["value"])
                    date_date = datetime.strptime(
                        stn["OBSERVATIONS"][grab_this_set]["date_time"],
                        "%Y-%m-%dT%H:%M:%SZ",
                    )
                    #
                    return_this[v] = np.append(return_this[v], variable_data)
                    return_this[v + "_DATETIME"] = np.append(
                        return_this[v + "_DATETIME"], date_date
                    )
                else:
                    if verbose:
                        print("%s is not available for %s" % (v, stn["STID"]))
                    # If value doesn't exist, then append with np.nan
                    return_this[v] = np.append(return_this[v], np.nan)
                    return_this[v + "_DATETIME"] = np.append(
                        return_this[v + "_DATETIME"], np.nan
                    )

        return return_this

    else:
        # There were errors in the API request
        if verbose:
            print("  !! Errors: %s" % URL)
            print("  !! Reason: %s\n" % data["SUMMARY"]["RESPONSE_MESSAGE"])
        return "ERROR"
