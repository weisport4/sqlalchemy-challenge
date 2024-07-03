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

    # define properties
    def __init__(self):
        self.engine = create_engine("sqlite:///titanic.sqlite")
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

    def query_passengers_orm(self):
        # Save reference to the table
        Passenger = self.Base.classes.passenger

        # Create our session (link) from Python to the DB
        session = Session(self.engine)

        # Query all passengers
        results = session.query(Passenger.name, Passenger.age, Passenger.sex).all()

        # close session
        session.close()

        df = pd.DataFrame(results)
        data = df.to_dict(orient="records")
        return(data)

    # same thing with RAW SQL
    def query_passengers_raw(self):

        # Query all passengers
        query = "SELECT name, age, sex from passenger;"

        df = pd.read_sql(text(query), con = self.engine)
        data = df.to_dict(orient="records")
        return(data)

    def query_dynamic_orm(self, min_age, gender):
        # Save reference to the table
        Passenger = self.Base.classes.passenger

        # Create our session (link) from Python to the DB
        session = Session(self.engine)

        # Query all passengers with filters
        results = session.query(Passenger.name, Passenger.age, Passenger.sex).filter(Passenger.age >= min_age).filter(Passenger.sex == gender).order_by(Passenger.name).all()

        # close session
        session.close()

        df = pd.DataFrame(results)
        data = df.to_dict(orient="records")
        return(data)

    def query_dynamic_raw(self, min_age, gender):
        # Query all passengers
        query = f"""
                SELECT
                    name,
                    age,
                    sex
                FROM
                    passenger
                WHERE
                    age >= {min_age}
                    AND sex = '{gender}'
                ORDER BY
                    name ASC;
                """

        df = pd.read_sql(text(query), con = self.engine)
        data = df.to_dict(orient="records")
        return(data)
