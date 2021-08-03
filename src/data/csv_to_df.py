import pandas as pd
import logging


# TODO: Fix this fugly import path
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
