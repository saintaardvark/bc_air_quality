"""Methods for InfluxDB
"""

# TODO: this whole thing should be made into its own standalone module
# for my own use.

import logging
import os

from influxdb import InfluxDBClient
import pandas as pd


def build_influxdb_data(df):
    """
    Build influxdb data out of dataframe and return
    """
    logger = logging.getLogger(__name__)
    logger.info("Building influxdb data...")

    influx_data = []
    # TODO: All this is hardcoded & shouldn't be
    for index, row in df.iterrows():
        ts = index.timestamp()  # seconds since epoch, as float
        # logger.debug(ts)
        measurement = {
            "measurement": "PM10",
            "fields": {
                "pm10": row['ROUNDED_VALUE'],
                "station_name": row['STATION_NAME'],
            },
            "time": index,
        }
        influx_data.append(measurement)

    return influx_data

def build_influxdb_client():
    """
    Build and return InfluxDB client
    """
    # Setup influx client
    logger = logging.getLogger(__name__)

    db = os.getenv("INFLUX_DB", "You forgot to set INFLUX_DB in .secret.sh!")
    host = os.getenv("INFLUX_HOST", "You forgot to set INFLUX_HOST in .secret.sh!")
    port = os.getenv("INFLUX_PORT", "You forgot to set INFLUX_PORT in .secret.sh!")
    influx_user = os.getenv("INFLUX_USER", "You forgot to set INFLUX_USER in .secret.sh!")
    influx_pass = os.getenv("INFLUX_PASS", "You forgot to set INFLUX_PASS in .secret.sh!")

    influx_client = InfluxDBClient(
        host=host,
        port=port,
        username=influx_user,
        password=influx_pass,
        database=db,
        ssl=True,
        verify_ssl=True,
    )
    logger.info("Connected to InfluxDB version {}".format(influx_client.ping()))
    return influx_client


def write_influx_data(influx_data, influx_client):
    """
    Write influx_data to database
    """
    logger = logging.getLogger(__name__)
    logger.info("Writing data to influxdb...")

    influx_client.write_points(influx_data, time_precision="s")
