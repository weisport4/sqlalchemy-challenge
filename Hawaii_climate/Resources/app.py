# Import the dependencies.
from flask import Flask, jsonify
import pandas as pd
import numpy as np
from sqlHelper import SQLHelper

#################################################
# Flask Setup
# Telling the file it's an app now with SQL
#################################################
app = Flask(__name__)
sql = SQLHelper()

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation_sql<br/>"
        f"/api/v1.0/station_sql<br/>"
        f"/api/v1.0/tobs_activestation/USC00519281<br/>"
        f"/api/v1.0/tobs_start/2016-08-23<br/>"
        f"/api/v1.0/tobs_start_end/2016-08-23/2017-08-23"
    )

# SQL Queries
# last 12 months of precipitation data
@app.route("/api/v1.0/precipitation_sql")
def precipitation_sql():
    data = sql.query_precipitation_sql()
    return(jsonify(data))

# list of stations
@app.route("/api/v1.0/station_sql")
def station_sql():
    data = sql.query_station_sql()
    return(jsonify(data))

# list of temp observation for most active station
@app.route("/api/v1.0/tobs_activestation/<station>")
def tobs_activestation_sql(station):
    data = sql.query_tobs_activestation_sql(station)
    return(jsonify(data))

# start format 2016-08-23
@app.route("/api/v1.0/tobs_start/<start>")
def tobs_start_sql(start):
    data = sql.query_tobs_start_sql(start)
    return(jsonify(data))

# start should be in format 2016-08-23
@app.route("/api/v1.0/tobs_start_end/<start>/<end>")
def tobs_start_end_sql(start, end):
    data = sql.query_tobs_start_end_sql(start, end)
    return(jsonify(data))


# Run the App
if __name__ == '__main__':
    app.run(debug=True)
