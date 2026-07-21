from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

import pandas as pd

from etl_logging.logger import get_logger


logger = get_logger("SQLLoader")


class SQLLoader:
    """
    Loads dataframe chunks into SQL Server.
    """


    def __init__(
        self,
        engine: Engine,
        table_name: str,
        schema: str,
        chunksize: int = 5000,
        if_exists: str = "append"
    ):

        self.engine = engine
        self.table_name = table_name
        self.schema = schema
        self.chunksize = chunksize
        self.if_exists = if_exists



    def load(
        self,
        df: pd.DataFrame
    ) -> int:


        rows = len(df)


        if rows == 0:

            logger.warning(
                "Skipping empty dataframe"
            )

            return 0


        try:

            logger.info(
                f"Loading {rows:,} rows "
                f"into {self.schema}.{self.table_name}"
            )


            df.to_sql(
                name=self.table_name,
                con=self.engine,
                schema=self.schema,
                if_exists=self.if_exists,
                index=False,
                chunksize=self.chunksize,
                method="multi"
            )


            logger.info(
                f"Loaded {rows:,} rows successfully"
            )


            return rows



        except SQLAlchemyError as e:


            logger.exception(
                "SQL load failed"
            )


            raise