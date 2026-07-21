from sqlalchemy import create_engine, URL
from sqlalchemy.engine import Engine


def get_sql_engine(
    server: str,
    database: str,
    driver: str = "ODBC Driver 17 for SQL Server",
    trusted: bool = True,
    encrypt: bool = False,
    trust_server_cert: bool = True,
    fast_executemany: bool = True,
):
    """
    Create SQLAlchemy SQL Server engine.
    """

    params = []

    if trusted:
        params.append("Trusted_Connection=yes")

    if encrypt:
        params.append("Encrypt=yes")

    if trust_server_cert:
        params.append("TrustServerCertificate=yes")


    odbc_connect = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        + ";".join(params)
    )


    url = URL.create(
        "mssql+pyodbc",
        query={
            "odbc_connect": odbc_connect
        }
    )


    engine = create_engine(
        url,
        future=True,
        fast_executemany=fast_executemany,
        pool_pre_ping=True,
        pool_recycle=1800
    )


    return engine

