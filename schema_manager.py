from dataclasses import dataclass
from typing import Dict, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass
class ColumnMetadata:

    name: str
    data_type: str
    max_length: Optional[int]
    precision: Optional[int]
    scale: Optional[int]
    nullable: bool


class SchemaManager:

    def __init__(self, engine: Engine):

        self.engine = engine
        self._cache = {}


    def get_table_schema(
        self,
        table_name: str,
        schema: str = "dbo"
    ):

        key = (schema, table_name)

        if key in self._cache:
            return self._cache[key]

        sql = text("""
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA=:schema
          AND TABLE_NAME=:table
        ORDER BY ORDINAL_POSITION
        """)

        with self.engine.connect() as conn:

            rows = conn.execute(
                sql,
                {
                    "schema": schema,
                    "table": table_name
                }
            )

            metadata = {}

            for row in rows:

                metadata[row.COLUMN_NAME] = ColumnMetadata(
                    name=row.COLUMN_NAME,
                    data_type=row.DATA_TYPE,
                    max_length=row.CHARACTER_MAXIMUM_LENGTH,
                    precision=row.NUMERIC_PRECISION,
                    scale=row.NUMERIC_SCALE,
                    nullable=row.IS_NULLABLE == "YES"
                )

        self._cache[key] = metadata

        return metadata


    def get_date_columns(
        self,
        table_name,
        schema="dbo"
    ):

        metadata = self.get_table_schema(
            table_name,
            schema
        )

        return [

            col

            for col, info in metadata.items()

            if info.data_type.lower() in (
                "date",
                "datetime",
                "datetime2",
                "smalldatetime"
            )
        ]


    def get_numeric_columns(
        self,
        table_name,
        schema="dbo"
    ):

        metadata = self.get_table_schema(
            table_name,
            schema
        )

        return [

            col

            for col, info in metadata.items()

            if info.data_type.lower() in (
                "int",
                "bigint",
                "smallint",
                "tinyint",
                "float",
                "real",
                "decimal",
                "numeric",
                "money",
                "smallmoney"
            )
        ]


    def get_varchar_lengths(
        self,
        table_name,
        schema="dbo"
    ):

        metadata = self.get_table_schema(
            table_name,
            schema
        )

        return {

            col: info.max_length

            for col, info in metadata.items()

            if info.data_type.lower() in (
                "varchar",
                "nvarchar",
                "char",
                "nchar"
            )
        }