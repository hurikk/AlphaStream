from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pathlib import Path

from src.alphastream.queries.postgres_queries import PostgresQuery
from src.alphastream.database.helpers import start_conn


class PostgresSetup:
    """Used to create a new database.

    Attributes:
        conn: A reference to the connection object established with
            the "postgres" maintenance database.
        db_exists_or_no: Whether the database to be created already
            exists.
    """

    def __init__(self, env_path: Path, new_db_name: str) -> None:
        """Initializes the setup helper and checks if the database exists.

        Args:
            env_path: Path to the environment variables file.
            new_db_name: Name of the database to be created.
        """
        self.conn = start_conn(env_path, database_name="postgres")
        self.db_exists_or_no = PostgresQuery.db_exists_or_no(env_path, new_db_name)

    def create_new_db(self, new_db_name: str) -> None:
        """Creates a new database if it does not already exist.

        Sets the connection to autocommit mode, since `CREATE DATABASE`
        cannot run inside a transaction block. Closes the connection
        after execution.

        Args:
            new_db_name: Name of the database to create.

        Returns:
            None
        """
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = self.conn.cursor()
        if not self.db_exists_or_no:
            cursor.execute(f"CREATE DATABASE {new_db_name}")
        cursor.close()
        self.conn.close()

    def init_db(self, new_db_name: str) -> None:
        """Entry point to create the new database.

        Delegates to `create_new_db`.

        Args:
            new_db_name: Name of the database to create.

        Returns:
            None
        """
        self.create_new_db(new_db_name)
