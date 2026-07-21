from pathlib import Path
from typing import Iterator

import pandas as pd


class CSVExtractor:
    """
    Streaming CSV extractor.
    """

    def __init__(
        self,
        file_path,
        chunk_size=250000,
        encoding="utf-8",
        parse_dates=None
    ):

        self.file_path = Path(file_path)
        self.chunk_size = chunk_size
        self.encoding = encoding
        self.parse_dates = parse_dates or []


        if not self.file_path.exists():

            raise FileNotFoundError(
                f"File not found: {self.file_path}"
            )


    def extract(self) -> Iterator[pd.DataFrame]:
        """
        Yield dataframe chunks.
        """

        try:

            for chunk in pd.read_csv(
                self.file_path,
                chunksize=self.chunk_size,
                encoding=self.encoding,
                low_memory=False
            ):

                for col in self.parse_dates:

                    if col in chunk.columns:

                        chunk[col] = pd.to_datetime(
                            chunk[col],
                            errors="coerce"
                        )


                yield chunk


        except pd.errors.EmptyDataError:

            print(
                f"Skipping empty file: {self.file_path.name}"
            )

            return
