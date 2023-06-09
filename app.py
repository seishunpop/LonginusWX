from flask import Flask
from flask import request
from flask import render_template
from flask import send_from_directory
from flask import make_response
import datetime
from datetime import timedelta
from taf_reducer import taf_reducer

app = Flask(__name__)


# This route serves the robots.txt file
@app.route("/robots.txt")
def robots():
    return send_from_directory("static", "robots.txt")


# This route serves an html template for the homepage located in /templates
@app.route("/")
def index():
    return render_template("index.html")


# This route returns a string containing the worst conditions from all TAF lines within the valid period
# Example query string format: ?stations=knkt,ksmf&valid-from=2023-05-14T10:00:00&valid-to=2023-05-15T09:00:00
@app.route("/taf/")
def taf():
    # The AWC TDS(Text Data Server) api requires station identifiers be uppercase
    try:
        stations = request.args.get("stations").upper().split(",")
    except:
        return "Please enter a valid icao<br>Example format: https://longinuswx.com/?stations=knkt,ksmf"

    # The default valid_from is the current zulu datetime in ISO8601 format
    valid_from = datetime.datetime.utcnow().replace(microsecond=0).isoformat()
    if request.args.get("valid-from"):
        valid_from = request.args.get("valid-from")

    # The default valid_to goes 8 hours out
    valid_to = (
        datetime.datetime.utcnow().replace(microsecond=0) + timedelta(hours=8)
    ).isoformat()
    if request.args.get("valid-to"):
        valid_to = request.args.get("valid-to")

    # Calls the taf_reducer function and returns an error message if it fails
    # Missing TAFs will only return the icao
    try:
        res = taf_reducer(stations, valid_from, valid_to)
        res = make_response(res, [("X-Robots-Tag", "noindex")])
        return res
    except:
        res = "Please enter a valid timeframe<br>Example format: https://longinuswx.com/?stations=knkt,ksmf&valid-from=2023-05-14T10:00:00&valid-to=2023-05-15T009:00:00"
        res = make_response(res, [("X-Robots-Tag", "noindex")])
        return res
