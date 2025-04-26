"""Local Connector."""

import csv
import logging
import os
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from paper_engine_strategy._types import File

logger = logging.getLogger(__name__)


class LocalConnector(object):
    """Local connector class."""

    def __init__(self, source_dir_name: str) -> None:
        """Local data source_model.

        Args:
            source_dir_name: Source directory name.
        """
        self._source_directory = os.path.join(
            Path(os.path.abspath(os.curdir)).parent.parent, source_dir_name
        )

    def set_source_file(self, file_name: str) -> str:
        """Sets path to the desired target_model directory in 'local_data'.

        Args:
            file_name: source_model directory inside 'local_data'.

        Returns:
            Absolute path to the source_model file.
        """
        return os.path.join(self._source_directory, file_name)

    def read_csv(self, file_name: str) -> File:
        """Returns dataframe with data from file with the provided file name."""
        source_file = self.set_source_file(f"{file_name}.csv")

        with open(source_file) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            file = []
            # REMOVING HEADER
            _ = next(csv_reader)
            for row in csv_reader:
                file.append(tuple(row))

        return file

    def read_parquet(
        self, file_name: str, unflatten: bool = False, transpose: bool = False
    ) -> File:
        """Returns all records from file in the source_model directory."""
        source_file = self.set_source_file(f"{file_name}.parquet")

        logger.debug("Unpacking file...")
        table = pq.read_table(source_file)
        logger.debug("Building dataframe...")
        df: pd.DataFrame = table.to_pandas()
        if transpose:
            df = df.transpose()
        if unflatten:
            logger.debug("Unflattening...")
            records = self.unflatten(df)
        else:
            records = df.to_records()
        del df
        logger.debug("Records generated.")

        return records

    @staticmethod
    def unflatten(df: pd.DataFrame) -> File:
        """Unflatten dataframe."""
        dictionary = df.to_dict()
        records: File = []
        i = 0
        for gvkey in dictionary.keys():
            if i % 1000 == 0:
                logger.debug(f"{i}/{len(dictionary.keys())} gvkeys resolved.")
            for date in dictionary[gvkey].keys():
                records.append((gvkey, date, dictionary[gvkey][date]))
            i += 1
        return records
