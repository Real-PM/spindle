import os
import sqlite3
import sys

from loguru import logger

create_table_methods = []


def register_create_table_method(func):
    """
    A decorator function that registers a function to create a table in the database.

    Parameters
    ----------
    func : function
        the function to register
    """
    create_table_methods.append(func)
    return func


class Database:
    """
    A class used to represent a connection to a SQLite database.

    Attributes
    ----------
    db_path : str
        the path to the SQLite database file
    connection : sqlite3.Connection or None
        the connection object to the SQLite database
    """

    def __init__(self, db_path: str):
        """
        Constructs all the necessary attributes for the Database object.

        Parameters
        ----------
        db_path : str
            the path to the SQLite database file
        """
        self.db_path = db_path
        self.connection = None

    def connect(self):
        """
        Establishes a connection to the SQLite database.
        Creates the database file and parent directories if they don't exist.
        """
        if self.connection is not None:
            return
        else:
            try:
                # Ensure parent directory exists
                db_dir = os.path.dirname(self.db_path)
                if db_dir and not os.path.exists(db_dir):
                    os.makedirs(db_dir, exist_ok=True)

                self.connection = sqlite3.connect(self.db_path)
                # Enable foreign key enforcement (off by default in SQLite)
                self.connection.execute("PRAGMA foreign_keys = ON")
                logger.info(f"Connected to SQLite database: {self.db_path}")
            except sqlite3.Error as error:
                logger.error(f"There was an error connecting to SQLite database: {error}")
                sys.exit()

    def ensure_connection(self) -> None:
        """Ensure connection exists.

        SQLite doesn't have NAT timeout issues like MySQL, so this simply
        ensures we have a connection. Kept for API compatibility.
        """
        if self.connection is None:
            self.connect()

    def close(self):
        """
        Closes the connection to the SQLite database.
        """
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Connection closed")

    def drop_table(self, table_name):
        """
        Drops a table from the database if it exists.

        Parameters
        ----------
        table_name : str
            the name of the table to drop
        """
        cursor = self.connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.connection.commit()
        cursor.close()
        logger.info(f"Table {table_name} dropped")

    def create_table(self, query):
        """
        Creates a table in the database using the provided SQL query.

        Parameters
        ----------
        query : str
            the SQL query to create the table
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        cursor.close()
        logger.info("Table created")

    def execute_query(self, query, params=None):
        """
        Executes a SQL query on the database.

        Parameters
        ----------
        query : str
            the SQL query to execute
        params : tuple, optional
            the parameters to use with the SQL query
        """
        self.ensure_connection()
        try:
            cursor = self.connection.cursor()
            logger.debug("Executing query on SQLite database")
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.connection.commit()
            cursor.close()
        except sqlite3.Error as error:
            logger.error(f"Error executing query: {error}")

    def execute_many(self, query: str, params_list: list[tuple]) -> int:
        """
        Executes a SQL query multiple times with different parameters (batch insert).

        Args:
            query: The SQL query to execute (with placeholders)
            params_list: List of parameter tuples

        Returns:
            Number of rows affected
        """
        self.ensure_connection()
        try:
            cursor = self.connection.cursor()
            cursor.executemany(query, params_list)
            self.connection.commit()
            rowcount = cursor.rowcount
            cursor.close()
            return rowcount
        except sqlite3.Error as error:
            logger.error(f"Error executing batch query: {error}")
            self.connection.rollback()
            raise

    def execute_select_query(self, query, params=None):
        """
        Executes a SELECT SQL query on the database and returns the results.

        Parameters
        ----------
        query : str
            the SQL query to execute
        params : tuple, optional
            the parameters to use with the SQL query

        Returns
        -------
        list
            the results of the query
        """
        self.ensure_connection()
        result = []
        try:
            cursor = self.connection.cursor()
            logger.debug("Executing SELECT query on SQLite database")
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            result = cursor.fetchall()
        except sqlite3.Error as error:
            logger.error(f"There was an error executing the query: {error}")
            self.connection.rollback()
        return result

    def create_all_tables(self):
        """
        Creates all tables in the database.
        """
        for method in create_table_methods:
            method(self)

    @register_create_table_method
    def create_artists_table(self, table_name="artists"):
        """
        Creates the artists table in the database.

        Parameters
        ----------
        table_name : str, optional
            the name of the table to create (default is "artists")
        """
        self.execute_query("PRAGMA foreign_keys = OFF")
        self.drop_table(table_name)
        artists_ddl = """CREATE TABLE IF NOT EXISTS artists(
        id INTEGER PRIMARY KEY AUTOINCREMENT
        , artist TEXT NOT NULL
        , last_fm_id TEXT
        , discogs_id TEXT
        , musicbrainz_id TEXT
        , enrichment_attempted_at TEXT
        )"""
        self.create_table(artists_ddl)
        self.execute_query("PRAGMA foreign_keys = ON")

    @register_create_table_method
    def create_track_data_table(self, table_name="track_data"):
        """
        Creates the track_data table in the database.

        Parameters
        ----------
        table_name : str, optional
            the name of the table to create (default is "track_data")
        """
        self.execute_query("PRAGMA foreign_keys = OFF")
        self.drop_table("track_data")
        track_data_ddl = """
        CREATE TABLE IF NOT EXISTS track_data(
        id INTEGER PRIMARY KEY AUTOINCREMENT
        , title TEXT
        , artist TEXT
        , album TEXT
        , added_date TEXT
        , filepath TEXT
        , location TEXT
        , bpm INTEGER
        , genre TEXT
        , artist_id INTEGER
        , plex_id INTEGER
        , musicbrainz_id TEXT
        , acoustid TEXT
        , researched_at TEXT
        , FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE)"""
        self.create_table(track_data_ddl)
        ix_loc = """CREATE INDEX IF NOT EXISTS ix_loc ON track_data (location)"""
        ix_filepath = """CREATE INDEX IF NOT EXISTS ix_filepath ON track_data (filepath)"""
        ix_bpm = """CREATE INDEX IF NOT EXISTS ix_bpm ON track_data (bpm)"""
        ix_mbid = """CREATE INDEX IF NOT EXISTS ix_musicbrainz_id ON track_data (musicbrainz_id)"""
        ix_plex = """CREATE INDEX IF NOT EXISTS ix_plex_id ON track_data (plex_id)"""
        self.execute_query(ix_loc)
        self.execute_query(ix_filepath)
        self.execute_query(ix_bpm)
        self.execute_query(ix_mbid)
        self.execute_query(ix_plex)
        self.execute_query("PRAGMA foreign_keys = ON")

    @register_create_table_method
    def create_history_table(self, table_name="history"):
        """
        Creates the history table in the database.

        Parameters
        ----------
        table_name : str, optional
            the name of the table to create (default is "history")
        """
        self.execute_query("PRAGMA foreign_keys = OFF")
        self.drop_table("history")
        history_ddl = """
        CREATE TABLE IF NOT EXISTS history(
        id INTEGER PRIMARY KEY AUTOINCREMENT
        , tx_date TEXT
        , records INTEGER
        , latest_entry TEXT)"""
        self.create_table(history_ddl)
        self.execute_query("PRAGMA foreign_keys = ON")

    @register_create_table_method
    def create_similar_artists_table(self):
        """
        Creates the similar_artists table in the database.
        """
        self.execute_query("PRAGMA foreign_keys = OFF")
        self.drop_table("similar_artists")
        similar_artists_ddl = """
        CREATE TABLE IF NOT EXISTS similar_artists(
        id INTEGER PRIMARY KEY AUTOINCREMENT
        , artist_id INTEGER
        , similar_artist_id INTEGER
        , FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE
        , FOREIGN KEY (similar_artist_id) REFERENCES artists(id) ON DELETE CASCADE)"""
        self.create_table(similar_artists_ddl)
        self.execute_query("PRAGMA foreign_keys = ON")

    @register_create_table_method
    def create_genres_table(self):
        """
        Creates the genres table in the database.
        """
        self.execute_query("PRAGMA foreign_keys = OFF")
        self.drop_table("genres")
        genres_ddl = """
        CREATE TABLE IF NOT EXISTS genres(
        id INTEGER PRIMARY KEY AUTOINCREMENT
        , genre TEXT NOT NULL
        )
        """
        self.create_table(genres_ddl)
        self.execute_query("PRAGMA foreign_keys = ON")

    @register_create_table_method
    def create_track_genres_table(self):
        """
        Creates the track_genres table in the database.
        """
        self.execute_query("PRAGMA foreign_keys = OFF")
        self.drop_table("track_genres")
        track_genres_ddl = """
        CREATE TABLE IF NOT EXISTS track_genres(
        id INTEGER PRIMARY KEY AUTOINCREMENT
        , track_id INTEGER
        , genre_id INTEGER
        , FOREIGN KEY (track_id) REFERENCES track_data(id) ON DELETE CASCADE
        , FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE
        )
        """
        self.create_table(track_genres_ddl)
        self.execute_query("PRAGMA foreign_keys = ON")

    @register_create_table_method
    def create_artist_genres_table(self):
        """
        Creates the artist_genres table in the database.
        """
        self.execute_query("PRAGMA foreign_keys = OFF")
        self.drop_table("artist_genres")
        artist_genres_ddl = """
        CREATE TABLE IF NOT EXISTS artist_genres(
        id INTEGER PRIMARY KEY AUTOINCREMENT
        , artist_id INTEGER
        , genre_id INTEGER
        , FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE
        , FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE
        )
        """
        self.create_table(artist_genres_ddl)
        self.execute_query("PRAGMA foreign_keys = ON")

    def drop_all_tables(self):
        """
        Drops all tables in the database.
        """
        self.connect()
        self.execute_query("PRAGMA foreign_keys = OFF")
        for method in create_table_methods:
            table_name = method.__name__.replace("create_", "").replace("_table", "")
            self.drop_table(table_name)
        self.execute_query("PRAGMA foreign_keys = ON")
        self.close()
