import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd


# Scrapes data from each TAF line
def scrape_taf(icao):
    try:
        # Selects TAF element by icao
        taf_lines = soup.find("station_id", string=icao).parent.find_all("forecast")

        # Initializes the dict that TAF lines will be appended to later
        taf = {
            "icao": icao,
            "fcst_from": [],
            "fcst_to": [],
            "wnd_dir": [],
            "wnd_speed": [],
            "wnd_gust": [],
            "visibility": [],
            "wx": [],
            "sky_con": [],
        }

        # Loops through each TAF line scraping its data
        # At the end of each loop the line gets appended to the taf dict
        for x in taf_lines:
            fcst_time_from = x.fcst_time_from.string
            fcst_time_to = x.fcst_time_to.string
            wind_dir_degrees = None
            if x.wind_dir_degrees:
                wind_dir_degrees = x.wind_dir_degrees.string
            wind_speed_kt = None
            if x.wind_speed_kt:
                wind_speed_kt = x.wind_speed_kt.string
            wind_gust_kt = None
            if x.wind_gust_kt:
                wind_gust_kt = x.wind_gust_kt.string
            visibility_statute_mi = None
            if x.visibility_statute_mi:
                visibility_statute_mi = x.visibility_statute_mi.string
            wx_string = "NSW"
            if x.wx_string:
                wx_string = x.wx_string.string
            sky_condition = {"cloud_base_ft_agl": [], "sky_cover": []}
            for z in x.select("forecast > sky_condition"):
                try:
                    sky_condition["cloud_base_ft_agl"].append(z["cloud_base_ft_agl"])
                except:
                    sky_condition["cloud_base_ft_agl"].append(None)
                sky_condition["sky_cover"].append(z["sky_cover"])

            taf["fcst_from"].append(fcst_time_from)
            taf["fcst_to"].append(fcst_time_to)
            taf["wnd_dir"].append(wind_dir_degrees)
            taf["wnd_speed"].append(wind_speed_kt)
            taf["wnd_gust"].append(wind_gust_kt)
            taf["visibility"].append(visibility_statute_mi)
            taf["wx"].append(wx_string)
            taf["sky_con"].append(sky_condition)

        return taf
    except:
        # This dict gets returned if no TAF matching the icao is found
        taf = {
            "icao": icao,
            "fcst_from": [],
            "fcst_to": [],
            "wnd_dir": [],
            "wnd_speed": [],
            "wnd_gust": [],
            "visibility": [],
            "wx": [],
            "sky_con": [],
        }

        return taf


# Picks worst conditions for each element
def process_taf(taf):
    # Creates a dataframe from the taf dict
    df = pd.DataFrame(taf)

    # Drops TAF lines outside of the valid times
    df.drop(df[df.fcst_to < valid_from].index, inplace=True)
    df.drop(df[df.fcst_from >= valid_to].index, inplace=True)

    # Fills all None values with 0 in the respective columns
    df["wnd_gust"].fillna(0, inplace=True)
    df["wnd_speed"].fillna(0, inplace=True)

    # Initializes the dict that forecast elements will be appended to
    forecast = {
        "icao": taf["icao"],
        "wnd_dir": None,
        "wnd_speed": None,
        "wnd_gust": None,
        "visibility": None,
        "wx": None,
        "sky_con": None,
    }

    # Determines the worst wind conditions by highest wind speed
    # One caveat is that this assumes the highest wind speed will be a gust
    if df.wnd_gust.any():
        idx = df["wnd_gust"].astype("int8").idxmax()
        winds = df.iloc[idx]
        forecast["wnd_dir"] = winds.wnd_dir
        forecast["wnd_speed"] = winds.wnd_speed
        forecast["wnd_gust"] = winds.wnd_gust
    else:
        idx = df["wnd_speed"].astype("int8").idxmin()
        winds = df.iloc[idx]
        forecast["wnd_dir"] = winds.wnd_dir
        forecast["wnd_speed"] = winds.wnd_speed

    # Calculates the lowest visiblity in statute miles
    # Still working on implementing internal AWC mappings(Ex. P6SM/7SM = 6.21)
    vis = df["visibility"].astype("float16").min()
    if vis == "6.21":
        forecast["visibility"] = "7"
    else:
        forecast["visibility"] = vis

    # Combines present weather from all lines into one string
    # Still working on refining the output string
    wx_df = df.wx.str.split(" +", expand=True)
    present_wx = []
    for x in range(wx_df.shape[1]):
        wx_series = wx_df[x].squeeze().tolist()
        for z in wx_series:
            present_wx.append(z)
    wx_scalar = pd.Series(present_wx)
    wx_scalar.fillna("NSW", inplace=True)
    wx_array = wx_scalar.unique()
    wx_string = ""
    for x in wx_array:
        wx_string += x + "."
    wx_string = wx_string.replace(".", " ")
    wx_string = wx_string[:-1]
    forecast["wx"] = wx_string

    # Determines worst sky condition by highest occlusion
    # Caveat is that of occlusions present multiple times, only lowest one gets used
    # EX: BKN007 BKN020 OVC040. BKN020 gets excluded while rest make it through
    # Reduction logic also does not yet handle vertical visibility
    sky = {"cloud_bases": [], "cloud_cover": [], "occlusion": []}
    for z in df["sky_con"]:
        for v in z["cloud_base_ft_agl"]:
            sky["cloud_bases"].append(v)
        for v in z["sky_cover"]:
            sky["cloud_cover"].append(v)
            if v == "OVC":
                sky["occlusion"].append(8)
            elif v == "BKN":
                sky["occlusion"].append(7)
            elif v == "SCT":
                sky["occlusion"].append(4)
            elif v == "FEW":
                sky["occlusion"].append(2)
            else:
                sky["occlusion"].append(0)

    sky_df = pd.DataFrame(sky)
    max_cover = sky_df["occlusion"].astype("int8").max()
    max_cover_idx = sky_df["occlusion"].astype("int8").idxmax()
    max_cover_string = sky_df["cloud_cover"].iloc[max_cover_idx]
    max_cover_rows = sky_df.loc[sky_df["occlusion"] == max_cover]
    min_base = max_cover_rows["cloud_bases"].astype("int32").min()

    reduced_sky = {"cover": [max_cover_string], "bases": [min_base]}
    sky_reduced_df = pd.DataFrame(reduced_sky)

    lesser_cover_rows = sky_df.loc[sky_df["occlusion"] < max_cover]
    lesser_covers = pd.unique(lesser_cover_rows["occlusion"])
    lesser_covers = np.sort(lesser_covers)
    lesser_covers = lesser_covers[::-1]

    for x in lesser_covers:
        lesser_rows = sky_df.loc[sky_df["occlusion"] == x]
        lesser_base = lesser_rows["cloud_bases"].astype("int32").min()
        if lesser_base < sky_reduced_df["bases"].astype("int32").min():
            cover_idx = lesser_rows["occlusion"].astype("int8").idxmax()
            cover_string = lesser_rows["cloud_cover"].iloc[cover_idx]
            lesser_sky = {"cover": [cover_string], "bases": [lesser_base]}
            lesser_sky_df = pd.DataFrame(lesser_sky)
            sky_reduced_df = pd.concat([sky_reduced_df, lesser_sky_df])

    sky_reduced_df.assign()

    print(sky_reduced_df)

    # sky_string = ""
    # bases = df.sky_con[0]["cloud_base_ft_agl"]
    # cover = df.sky_con[0]["sky_cover"]
    # if "OVC" in cover:
    #     idx = cover.index("OVC")
    #     sky_string += cover[idx] + bases[idx]
    # elif "BKN" in cover:
    #     idx = cover.index("BKN")
    #     sky_string += cover[idx] + bases[idx]
    # elif "SCT" in cover:
    #     idx = cover.index("SCT")
    #     sky_string += cover[idx] + bases[idx]
    # elif "FEW" in cover:
    #     idx = cover.index("FEW")
    #     sky_string += cover[idx] + bases[idx]
    # else:
    #     sky_string = "SKC"
    # forecast["sky_con"] = sky_string

    return forecast


# Development settings for Pandas
pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 1000)
pd.set_option("display.max_colwidth", None)

# TAF data is requested from the Aviation Weather Center API
# API documentation can be found at https://www.aviationweather.gov/dataserver/example?datatype=taf
query_string = "https://www.aviationweather.gov/adds/dataserver_current/httpparam?datasource=tafs&requestType=retrieve&format=xml&mostRecentForEachStation=true&hoursBeforeNow=2&stationString="
stations = ["KSMF"]
valid_from = "2023-05-06T06:00:00Z"
valid_to = "2023-05-07T16:00:00Z"

r = requests.get(query_string + ",".join(stations)).text
soup = BeautifulSoup(r, "xml")

tafs = []
for x in stations:
    tafs.append(scrape_taf(x))

forecasts = []
for x in tafs:
    if x["fcst_from"] == []:
        forecast = {
            "icao": x["icao"],
            "wnd_dir": None,
            "wnd_speed": None,
            "wnd_gust": None,
            "visibility": None,
            "wx": None,
            "sky_con": None,
        }
        forecasts.append(forecast)
    else:
        forecasts.append(process_taf(x))

# print()
