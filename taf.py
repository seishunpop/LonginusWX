import requests
from bs4 import BeautifulSoup
import pandas as pd

# Constructs query for selected stations

query_string = "https://www.aviationweather.gov/adds/dataserver_current/httpparam?datasource=tafs&requestType=retrieve&format=xml&mostRecentForEachStation=true&hoursBeforeNow=2&stationString="
station = "knkt"

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

# Converts TAF to dataframe then drops lines outside of valid times
# Also fills None values as needed to pick worst conditions

df = pd.DataFrame(taf)

valid_from = "2023-04-28T03:00:00Z"
valid_to = "2023-04-29T00:00:00Z"
df.drop(df[df.fcst_to < valid_from].index, inplace=True)
df.drop(df[df.fcst_from >= valid_to].index, inplace=True)
df["wnd_gust"].fillna(0, inplace=True)
df["wnd_speed"].fillna(0, inplace=True)

# Picks worst conditions for each element

forecast = {
    "wnd_dir": None,
    "wnd_speed": None,
    "wnd_gust": None,
    "visibility": None,
    "wx": None,
    "sky_con": None,
}

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

vis = df["visibility"].min()
if vis == "6.21":
    forecast["visibility"] = "7"
else:
    forecast["visibility"] = vis

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
    wx_string+=(x + ".")
wx_string = wx_string.replace(".", " ")
wx_string = wx_string[:-1]
forecast["wx"] = wx_string

sky_string = ""
bases = df.sky_con[0]["cloud_base_ft_agl"]
cover = df.sky_con[0]["sky_cover"]

if "OVC" in cover:
    idx = cover.index("OVC")
    sky_string+=cover[idx] + bases[idx]
elif "BKN" in cover:
    idx = cover.index("BKN")
    sky_string+=cover[idx] + bases[idx]
elif "SCT" in cover:
    idx = cover.index("SCT")
    sky_string+=cover[idx] + bases[idx]
elif "FEW" in cover:
    idx = cover.index("FEW")
    sky_string+=cover[idx] + bases[idx]
else:
    sky_string = "SKC"
forecast["sky_con"] = sky_string

print(forecast)
