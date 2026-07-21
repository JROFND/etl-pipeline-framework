import pandas as pd

from transform.base import Transformer


class DataTypeTransformer(Transformer):
    """
    Standard dataframe datatype cleanup.
    """


    def __init__(
        self,
        date_columns=None,
        numeric_columns=None,
        varchar_lengths=None
    ):

        self.date_columns = date_columns or []

        self.numeric_columns = numeric_columns or []

        self.varchar_lengths = varchar_lengths or {}



    def transform(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:


        # Dates

        for col in self.date_columns:

            if col in df.columns:

                df[col] = pd.to_datetime(
                    df[col],
                    errors="coerce"
                )


        # Numeric fields

        for col in self.numeric_columns:

            if col in df.columns:

                df[col] = pd.to_numeric(
                    df[col],
                    errors="coerce"
                )


        # SQL varchar length enforcement

        for col, length in self.varchar_lengths.items():

            if col in df.columns:

                df[col] = (
                    df[col]
                    .astype("string")
                    .str.slice(0, length)
                )


        return df