import pandas as pd
import logging

# TODO: this whole thing should be made into its own standalone module
# for my own use.

# TODO: Fix this fugly import path
def import_csv(csv_file, dtypes=None, parse_dates=False):
    """
    Import csv_file and return pandas dataframe.

    Datatypes and index processing are hard-coded.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Reading {csv_file.name}...")

    if dtypes is None:
        df = pd.read_csv(
            csv_file,
            sep=",",
            parse_dates=parse_dates,
            index_col=0,
        )
    else:
        df = pd.read_csv(
            csv_file,
            sep=",",
            dtype=dtypes,
            parse_dates=parse_dates,
            index_col=0,
        )

    df.index = pd.to_datetime(df.index)

    logger.debug(df)
    logger.debug(df.dtypes)
    logger.debug(df.index.dtype)

    return df
