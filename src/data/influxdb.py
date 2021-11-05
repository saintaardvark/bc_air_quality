"""Methods for InfluxDB
"""

# TODO: this whole thing should be made into its own standalone module
# for my own use.

import logging
import os

from influxdb import InfluxDBClient
import pandas as pd


# OTOH, this could probably be made bigger.  OTOH, this allowed the
# PM25 data to be imported successfully for the first time, so ¯\_(ツ)_/¯
DEFAULT_BATCH_SIZE = 10_000


def build_influxdb_data(df, datatype):
    """
    Build influxdb data out of dataframe and return
    """
    logger = logging.getLogger(__name__)
    logger.info("Building influxdb data...")

    influx_data = []
    # TODO: All this is hardcoded & shouldn't be
    for index, row in df.iterrows():
        # ts = index.timestamp()  # seconds since epoch, as float
        # logger.debug(ts)
        measurement = {
            "measurement": datatype,
            "fields": {
                datatype: row['ROUNDED_VALUE'],
            },
            "tags": {
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
    logger.info("Number of data points: ".format(len(influx_data)))
    influx_client.write_points(influx_data, time_precision="s", batch_size=DEFAULT_BATCH_SIZE)
