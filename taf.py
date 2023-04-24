import requests
from bs4 import BeautifulSoup
import pandas as pd

# Constructs query for selected stations

query_string = "https://www.aviationweather.gov/adds/dataserver_current/httpparam?datasource=tafs&requestType=retrieve&format=xml&mostRecentForEachStation=true&hoursBeforeNow=2&stationString="
station = "kely"

# Requests TAF to instantiate bs4 then finds all TAF lines
# TAF data is requested from the Aviation Weather Center API
# API documentation can be found at https://www.aviationweather.gov/dataserver/example?datatype=taf

r = requests.get(query_string + station).text
soup = BeautifulSoup(r, "xml")
taf_lines = soup.find_all("forecast")

# Scrapes data from each TAF line

taf = {
    "fcst_from": [],
    "fcst_to": [],
    "wnd_dir": [],
    "wnd_speed": [],
    "wnd_gust": [],
    "visibility": [],
    "wx": [],
    "sky_con": [],
}

for x in taf_lines:
    fcst_time_from = x.fcst_time_from.string
    fcst_time_to = x.fcst_time_to.string
    wind_dir_degrees = x.wind_dir_degrees.string
    wind_speed_kt = x.wind_speed_kt.string
    wind_gust_kt = None
    if x.wind_gust_kt:
        wind_gust_kt = x.wind_gust_kt.string
    visibility_statute_mi = x.visibility_statute_mi.string
    wx_string = "NSW"
    if x.wx_string:
        wx_string = x.wx_string.string
    sky_condition = {"cloud_base_ft_agl": [], "sky_cover": []}
    for z in x.select("forecast > sky_condition"):
        sky_condition["cloud_base_ft_agl"].append(z["cloud_base_ft_agl"])
        sky_condition["sky_cover"].append(z["sky_cover"])

    taf["fcst_from"].append(fcst_time_from)
    taf["fcst_to"].append(fcst_time_to)
    taf["wnd_dir"].append(wind_dir_degrees)
    taf["wnd_speed"].append(wind_speed_kt)
    taf["wnd_gust"].append(wind_gust_kt)
    taf["visibility"].append(visibility_statute_mi)
    taf["wx"].append(wx_string)
    taf["sky_con"].append(sky_condition)

# Converts TAF to dataframe then drops lines outside of valid times
df = pd.DataFrame(taf)

valid_from = "2023-04-24T06:00:00Z"
valid_to = "2023-04-24T17:00:00Z"
df.drop(df[df.fcst_to < valid_from].index, inplace=True)
df.drop(df[df.fcst_from >= valid_to].index, inplace=True)

# Picks worst conditions for each element

forecast = {
    "wnd_dir": None,
    "wnd_speed": None,
    "wnd_gust": None,
    "visibility": None,
    "wx": None,
    "wnd_dir": None,
}

print(df)
