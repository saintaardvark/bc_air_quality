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

if __name__ == '__main__':
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    bcaq()
