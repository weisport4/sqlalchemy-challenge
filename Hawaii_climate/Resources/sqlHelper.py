import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text, func

import pandas as pd
import numpy as np

# The Purpose of this Class is to separate out any Database logic
class SQLHelper():
    #################################################
    # Database Setup
    #################################################

    # define properties (database)
    def __init__(self):
        self.engine = create_engine("sqlite:///hawaii.sqlite")
        self.Base = None

        # automap Base classes
        self.init_base()

    def init_base(self):
        # reflect an existing database into a new model
        self.Base = automap_base()
        # reflect the tables
        self.Base.prepare(autoload_with=self.engine)

    #################################################
    # Database Queries
    #################################################

    # last 12 months of precipitation data
    def query_precipitation_sql(self):

        query = """
                SELECT
                    date,
                    station,
                    prcp
                FROM
                    measurement
                WHERE
                    date >= '2016-08-23'
                ORDER BY
                    date ASC;
            """

        precip_df = pd.read_sql(text(query), con = self.engine)
        data = precip_df.to_dict(orient="records")
        return(data)
    
    # list of stations
    def query_station_sql(self):

        query = """
                SELECT
                    measurement.station
                    name
                    count(*) as num_stations
                FROM
                    measurement
                JOIN 
                    station on measurement.station = station.station
                GROUP BY
                    name
                ORDER BY
                    num_stations desc;
            """

        station_df = pd.read_sql(text(query), con = self.engine)
        data = station_df.to_dict(orient="records")
        return(data)
    
    # list of temp observation for most active station
    def query_activestation_start_sql(self, station):

        query = f"""
                SELECT
                    min(tobs) as lowest_temp,
                    avg(tobs) as average_temp,
                    max(tobs) as highest_temp
                FROM
                    measurement
                WHERE
                    station = '{station}';
            """

        actstations_df = pd.read_sql(text(query), con = self.engine)
        data = actstations_df.to_dict(orient="records")
        return(data)
    
    # For a specified start, calculate TMIN, TAVG, and TMAX for all the dates greater than or equal to the start date.
    def query_tobs_start_sql(self, start):

        query = f"""
                SELECT
                    min(tobs) as min_temp,
                    avg(tobs) as avg_temp,
                    max(tobs) as max_temp
                FROM
                    measurement
                WHERE
                    date >= '{start}';
            """

        tobs_start_df = pd.read_sql(text(query), con = self.engine)
        data = tobs_start_df.to_dict(orient="records")
        return(data)
    
    # For a specified start date and end date, calculate TMIN, TAVG, and TMAX for the dates from the start date to the end date, inclusive.
    def query_tobs_start_end_sql(self, start, end):

        query = f"""
                SELECT
                    min(tobs) as min_temp,
                    avg(tobs) as avg_temp,
                    max(tobs) as max_temp
                FROM
                    measurement
                WHERE
                    date >= '{start}'
                    and date < '{end}';
            """

        tobs_start_end_df = pd.read_sql(text(query), con = self.engine)
        data = tobs_start_end_df.to_dict(orient="records")
        return(data)
