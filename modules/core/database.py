import polars as pl
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from typing import Optional


class Database:
    """Database connection and query execution handler.

    Supports MSSQL, MySQL, and PostgreSQL databases.
    """

    # Supported database types
    MSSQL = "mssql"
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    POSTGRES = "postgres"

    # Default ports
    DEFAULT_PORTS = {
        MSSQL: 1433,
        MYSQL: 3306,
        POSTGRESQL: 5432,
        POSTGRES: 5432,
    }

    def __init__(
        self,
        database_type: str,
        ip: str,
        username: str,
        pwd: str,
        db: str,
        port: Optional[int] = None,
        trusted: bool = True,
    ) -> None:
        """Initialize database connection.

        Args:
            database_type: Type of database ('mssql', 'mysql', 'postgresql').
            ip: Database IP address or hostname.
            username: Database username.
            pwd: Database password.
            db: Database name.
            port: Database port number. If None, uses default port.
            trusted: Whether to use trusted connection (default: True).
                    Only applicable for MSSQL.

        Raises:
            ValueError: If database_type is not supported.
        """
        database_type = database_type.lower()
        self.database_type = database_type
        self.connection_string = self._build_connection_string(
            database_type=database_type,
            ip=ip,
            username=username,
            pwd=pwd,
            db=db,
            port=port,
            trusted=trusted,
        )

    def _build_connection_string(
        self,
        database_type: str,
        ip: str,
        username: str,
        pwd: str,
        db: str,
        port: Optional[int],
        trusted: bool,
    ) -> str:
        """Build connection string based on database type.

        Args:
            database_type: Type of database.
            ip: Database IP address or hostname.
            username: Database username.
            pwd: Database password.
            db: Database name.
            port: Database port number.
            trusted: Whether to use trusted connection.

        Returns:
            Connection string for the database.

        Raises:
            ValueError: If database_type is not supported.
        """
        # Map database type to connection string builder
        builders = {
            self.MSSQL: self._mssql_connection_string,
            self.MYSQL: self._mysql_connection_string,
            self.POSTGRESQL: self._postgresql_connection_string,
            self.POSTGRES: self._postgresql_connection_string,
        }

        builder = builders.get(database_type)
        if builder is None:
            supported = ", ".join(builders.keys())
            raise ValueError(
                f"Database type '{database_type}' not supported. "
                f"Supported types: {supported}"
            )

        return builder(ip, username, pwd, db, port, trusted)

    def _mssql_connection_string(
        self,
        ip: str,
        username: str,
        pwd: str,
        db: str,
        port: Optional[int],
        trusted: bool,
    ) -> str:
        """Build MSSQL connection string.

        Args:
            ip: Database IP address or hostname.
            username: Database username.
            pwd: Database password.
            db: Database name.
            port: Database port number.
            trusted: Whether to use trusted connection.

        Returns:
            Connection string for MSSQL database.
        """
        driver = "ODBC+Driver+17+for+SQL+Server"
        if trusted:
            connection_string = f"mssql+pyodbc://{ip}/{db}"
            params = f"driver={driver}&trusted_connection=yes"
        else:
            # URL encode credentials for special characters
            encoded_username = quote_plus(username)
            encoded_pwd = quote_plus(pwd)
            connection_string = (
                f"mssql+pyodbc://{encoded_username}:{encoded_pwd}@{ip}/{db}"
            )
            params = f"driver={driver}"

        if port:
            connection_string = connection_string.replace(
                f"@{ip}/", f"@{ip}:{port}/"
            )

        return f"{connection_string}?{params}"

    def _mysql_connection_string(
        self,
        ip: str,
        username: str,
        pwd: str,
        db: str,
        port: Optional[int],
        trusted: bool,
    ) -> str:
        """Build MySQL connection string.

        Args:
            ip: Database IP address or hostname.
            username: Database username.
            pwd: Database password.
            db: Database name.
            port: Database port number.
            trusted: Not used for MySQL (kept for interface consistency).

        Returns:
            Connection string for MySQL database.
        """
        # URL encode credentials for special characters
        encoded_username = quote_plus(username)
        encoded_pwd = quote_plus(pwd)
        port = port or self.DEFAULT_PORTS[self.MYSQL]
        return (
            f"mysql+pymysql://{encoded_username}:{encoded_pwd}@{ip}:{port}/{db}"
        )

    def _postgresql_connection_string(
        self,
        ip: str,
        username: str,
        pwd: str,
        db: str,
        port: Optional[int],
        trusted: bool,
    ) -> str:
        """Build PostgreSQL connection string.

        Args:
            ip: Database IP address or hostname.
            username: Database username.
            pwd: Database password.
            db: Database name.
            port: Database port number.
            trusted: Not used for PostgreSQL (kept for interface consistency).

        Returns:
            Connection string for PostgreSQL database.
        """
        # URL encode credentials for special characters
        encoded_username = quote_plus(username)
        encoded_pwd = quote_plus(pwd)
        port = port or self.DEFAULT_PORTS[self.POSTGRESQL]
        return (
            f"postgresql+psycopg2://{encoded_username}:{encoded_pwd}"
            f"@{ip}:{port}/{db}"
        )

    def execute_query(self, query: str) -> pl.DataFrame:
        """Execute SQL query and return results as Polars DataFrame.

        Args:
            query: SQL query string to execute.

        Returns:
            Polars DataFrame with query results.
        """
        engine = create_engine(self.connection_string)
        try:
            with engine.connect() as connection:
                df = pl.read_sql_query(query, connection)
            return df
        finally:
            engine.dispose()

    def write_dataframe(
        self,
        df: pl.DataFrame,
        table_name: str,
        if_table_exists: str = "append",
    ) -> None:
        """Write Polars DataFrame to database table.

        Args:
            df: Polars DataFrame to write.
            table_name: Target table name.
            if_table_exists: Action if table exists ('append', 'replace',
                          etc.) (default: 'append').
        """
        engine = create_engine(self.connection_string)
        try:
            with engine.connect() as conn:
                df.write_database(
                    table_name=table_name,
                    connection=conn,
                    if_table_exists=if_table_exists,
                )
        finally:
            engine.dispose()