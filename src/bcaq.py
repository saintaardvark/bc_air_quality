#!/usr/bin/env python3

import datetime
from ftplib import FTP
import logging
import os
import time

import click
from dotenv import find_dotenv, load_dotenv
import pandas as pd

from  data.csv_to_df import import_csv
from  data.influxdb import build_influxdb_client, build_influxdb_data, write_influx_data
import data.urls as urls

this_file_path = os.path.abspath(os.path.dirname(__file__))
RAW_DATA_DIR = f"{this_file_path}/../data/raw"

@click.group("bcaq")
def bcaq():
    """A wrapper for BC air quality data stuff
    """

@click.command("fetch",
               short_help="Fetch data from BC Env server")
@click.option("--datatype",
              default="PM10",
              help="Type of data to fetch")
def fetch(datatype):
    """Fetch data from BC Env server
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Opening connection to {urls.ENVBC_HOST}")
    ftp = FTP(urls.ENVBC_HOST)
    ftp.login()
    logger.info("Login successful!")
    ftp.cwd(urls.RAW_AQ_DIR)
    logger.info("cd successful!")
    ftp.retrlines('LIST')
    if datatype in urls.CSV_FILES:
        data_file = urls.CSV_FILES[datatype]
        data_file_local_path = f"{RAW_DATA_DIR}/{data_file}"
        with open(data_file_local_path, 'wb') as fp:
            logger.info(f"Downloading {data_file} to {data_file_local_path}")
            ftp.retrbinary(f"RETR {data_file}", fp.write)
    else:
        logger.critical(f"{data_type} not supported")


@click.command("load",
               short_help="Load already-downloaded data into InfluxDB")
@click.argument("csv_file", type=click.File("r"))
@click.option("--type",
              default="PM10",
              help="Type of data to import")
def load(type, csv_file):
    """Import data into InfluxDB
    """
    logger = logging.getLogger(__name__)
    df = import_csv(csv_file, parse_dates=["DATE_PST"])
    influx_data = build_influxdb_data(df)
    influx_client = build_influxdb_client()
    write_influx_data(influx_data, influx_client)
    logger.info("Done!")
    pass


@click.command("explore",
               short_help="Tool to explore already-downloaded data a bit")
@click.argument("csv_file", type=click.File("r"))
def explore(csv_file):
    """A tool to explore the data a bit.
    """
    logger = logging.getLogger(__name__)

    # TODO: Those dates should be at end of day, rather than beginning of day
    df = import_csv(csv_file, parse_dates=["DATE_PST"])
    print(df)
    print(df.STATION_NAME.unique())
    print(df.UNIT.unique())
    print(df.dtypes)
    print(df['STATION_NAME'] == "Burnaby South")
    # influx_data = build_influxdb_data(df)
    # influx_client = build_influxdb_client()
    # write_influx_data(influx_data, influx_client)
    logger.info("Done!")


bcaq.add_command(explore)
bcaq.add_command(fetch)
bcaq.add_command(load)

if __name__ == '__main__':
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=log_fmt)
    bcaq()
