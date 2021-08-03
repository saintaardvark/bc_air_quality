#!/usr/bin/env python3

import datetime
import logging
import os
import time

import click
import pandas as pd


def import_csv(csv_file):
    """
    Import csv_file and return pandas dataframe.

    Datatypes and index processing are hard-coded.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Reading {csv_file.name}...")

    # dtypes = {"Date": "str", "Km": "float", "kwh/100km": "float"}
    df = pd.read_csv(
        csv_file,
        sep=",",
        # dtype=dtypes,
        parse_dates=["DATE_PST"],
        index_col=0,
    )
    df.index = pd.to_datetime(df.index)

    logger.debug(df)
    logger.debug(df.dtypes)
    logger.debug(df.index.dtype)

    return df




@click.command(short_help="Explore CSV")
@click.argument("csv_file", type=click.File("r"))
def main(csv_file):
    """
    Main entry point
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


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    main()
