#!/usr/bin/env python3

import datetime
import logging
import os
import time

import click
import pandas as pd

from  data.csv_to_df import import_csv

@click.group("bcaq")
def bcaq():
    """A wrapper for BC air quality data stuff
    """

@click.command("fetch",
               short_help="Fetch data from BC Env server")
@click.option("--type",
                default="PM10",
                help="Type of data to fetch")
def fetch(type):
    """Fetch data from BC Env server
    """
    logger = logging.getLogger(__name__)
    logger.info("Not implemented yet")
    pass

@click.command("load",
               short_help="Load already-downloaded data into InfluxDB")
@click.option("--type",
                default="PM10",
                help="Type of data to import")
def load(type):
    """Import data into InfluxDB
    """
    logger = logging.getLogger(__name__)
    logger.info("Not implemented yet")
    pass



@click.command("explore",
               short_help="Tool to explore already-downloaded data a bit")
@click.argument("csv_file", type=click.File("r"))
def explore(csv_file):
    """A tool to explore the data a bit.
    """
    logger = logging.getLogger(__name__)

    # TODO: Those dates should be at end of day, rather than beginning of day
    df = import_csv(csv_file)
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
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    bcaq()
