#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import sys
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

import click

from data import urls


# TODO: Make this a grupled command:
# foo fetch_data --type PM10
# foo explore_data --file ../data/raw/PM10.csv
# foo import_data --file ../data/raw/PM10.csv
@click.command()
@click.option('--data_type',
              default='PM10',
              show_default=True,
              help='The type of data to fetch.  Supported: PM10')
def main(data_type):
    """ Fetchd data
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')
    logger.info(f'I will fetch from {urls.ENVBC_URL}')
    if data_type == "PM10":
        logger.info('I will fetch PM10 data')
    else:
        logger.fatal(f"Data type {data_type} not supported yet")
        sys.exit(1)

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
